import getpass
import logging
import os
import re

import numpy as np
import pandas as pd
import psycopg2 as ybconn
from local_modules.safe_password import readpw

from local_modules import lmc_list_upc_2 as lmc
from local_modules.yb_load import yb_load


logger = logging.getLogger(__name__)


class GettingLMCDataFrame:
    def __init__(self, parameters: dict):
        self.parameters = parameters
        self.load_file = None
        self.upc_file = None

    def getting_dataframe(self):
        conn = None

        try:
            logger.info(
                "Starting LMC dataframe process for request ID=%s, LMC List ID=%s",
                self.parameters.get("ID"),
                self.parameters.get("LMC List ID"),
            )

            dbase = "py1usta1"
            yb_user = os.getenv('YELLOWBRICK_USER', getpass.getuser())

            conn = ybconn.connect(
                user=yb_user,
                password=readpw("Yellowbrick"),
                host="orlpybvip01.catmktg.com",
                port="5432",
                database=dbase,
            )
            conn.set_session(autocommit=True)

            logger.info("Connected to Yellowbrick database '%s'", dbase)

            lmc_base_url = "https://listmanager.catalinamarketing.com"
            lmc_list_focus_brands = lmc.LMC_PandasGetAllUPCs(
                lmc_base_url,
                self.parameters["LMC List ID"],
            )

            logger.info(
                "Retrieved %s UPC records from LMC for request ID=%s",
                len(lmc_list_focus_brands),
                self.parameters.get("ID"),
            )

            lmc_list_focus_brands["cmc_cat_nbr"] = round(lmc_list_focus_brands["cmc_cat_nbr"])

            self.load_file = lmc_list_focus_brands[
                ["upc_cd", "cmc_cat_nbr", "group_number", "group_name", "tot_wgt_amt", "tot_wgt_meas"]
            ].copy()

            self.load_file["cmc_cat_nbr"] = self.load_file["cmc_cat_nbr"].fillna(1)
            self.load_file["tot_wgt_amt"] = self.load_file["tot_wgt_amt"].fillna(1)
            self.load_file["tot_wgt_meas"] = self.load_file["tot_wgt_meas"].fillna("CT")
            self.load_file["upc_length"] = lmc_list_focus_brands["upc_cd"].str.len()
            self.load_file = self.load_file[self.load_file["upc_length"] <= 12]
            del self.load_file["upc_length"]

            self.load_file.insert(2, "NULL7", np.nan)
            self.load_file.insert(5, "NULL8", np.nan)
            self.load_file.insert(6, "NULL9", np.nan)
            self.load_file.insert(7, "NULL10", np.nan)

            self.load_file = self.load_file.rename(
                columns={
                    "cmc_cat_nbr": "cat_nbr",
                    "group_number": "brand_nbr",
                    "group_name": "brand_name",
                    "tot_wgt_amt": "eq_vol",
                    "tot_wgt_meas": "eq_meas",
                }
            )

            self.load_file = self.load_file.astype(
                {
                    "upc_cd": "int64",
                    "cat_nbr": "int64",
                    "brand_nbr": "int64",
                    "eq_vol": "float64",
                }
            )

            self.upc_file = self.load_file[["brand_nbr", "brand_name", "upc_cd"]].copy()
            self.upc_file = self.upc_file.rename(
                columns={
                    "brand_name": "brand_desc",
                    "upc_cd": "trade_item_cd",
                }
            )
            self.upc_file = self.upc_file.reset_index(drop=True)

            self.upc_file["brand_desc"] = (
                self.upc_file["brand_desc"]
                .str.replace("\xa0", "", regex=False)
                .str.replace("\u200b", "", regex=False)
                .str.replace(",", "", regex=False)
                .str.replace(".", "", regex=False)
                .str.replace("*", "", regex=False)
                .str.replace("´", "", regex=False)
                .str.replace("(", "", regex=False)
                .str.replace(")", "", regex=False)
                .str.replace("&", "", regex=False)
                .str.replace("/", "", regex=False)
                .str.replace("?", "", regex=False)
                .str.replace(">", "", regex=False)
                .str.replace("<", "", regex=False)
                .str.replace(":", "", regex=False)
                .str.replace("|", "", regex=False)
                .str.replace("+", "", regex=False)
                .str.replace("-", "", regex=False)
            )

            # Sanitize campaign name - remove special characters, keep only alphanumeric and underscore
            campaign_raw = self.parameters["Client Name"]
            campaign_nm = re.sub(r'[^a-zA-Z0-9_]', '', campaign_raw.replace(" ", "_")).lower()
            # Sanitize analyst name - remove special characters, keep only alphanumeric and underscore
            analyst_raw = self.parameters["Requested by"]
            analyst = re.sub(r'[^a-zA-Z0-9_]', '', analyst_raw.replace(" ", "_")).lower()
            table_name = f"vmr_{campaign_nm}_upclmc_{analyst}"
            
            # Email is kept untransformed for reference
            email = self.parameters.get("Email", "")

            logger.info("Loading UPC table '%s' into Yellowbrick", table_name)

            # yb_load handles fallback to psycopg2 internally when ybtools unavailable
            yb_password = readpw("Yellowbrick")
            load_success = yb_load(
                Df=self.upc_file,
                table_name=table_name,
                userid=yb_user,
                passwd=yb_password,
                append=False,
                database=dbase,
            )

            if not load_success:
                raise RuntimeError(f"Failed to load table '{table_name}'")

            check_upload = pd.read_sql(f"select count(*) from {table_name.lower()}", conn)

            logger.info(
                "UPC loading process successfully completed for request ID=%s. Rows uploaded: %s",
                self.parameters.get("ID"),
                check_upload.values[0][0],
            )

            return "UPC Loading process successfully completed!"

        except Exception as e:
            logger.exception(
                "UPC loading process failed for request ID=%s, LMC List ID=%s",
                self.parameters.get("ID"),
                self.parameters.get("LMC List ID"),
            )
            return "UPC LOADING ERROR"

        finally:
            if conn is not None:
                conn.close()
                logger.info("Yellowbrick connection closed for request ID=%s", self.parameters.get("ID"))
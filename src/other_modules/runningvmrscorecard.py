import logging
import re
import traceback
from pathlib import Path

from shareplum import Office365, Site
from shareplum.site import Version


logger = logging.getLogger(__name__)


class RunningVMRScorecard:
    def __init__(self, parameters: dict):
        self.parameters = parameters
        self.final_parameters = None
        self.new_status = None
        self.final_folder_name = None

    def parameters_transformation(self):
        try:
            logger.info("Starting parameter transformation for request ID=%s", self.parameters.get("ID"))

            transformed = self.parameters.copy()

            transformed["Client Name"] = self._sanitize_name(
                transformed["Client Name"],
                replace_dash_with_underscore=True,
            )

            transformed["Program Name"] = self._sanitize_name(
                transformed["Program Name"],
                replace_dash_with_underscore=False,
            )

            transformed["Reward BL"] = self._split_and_trim_bl(transformed["Reward BL"])

            if transformed.get("Announcement BL"):
                transformed["Announcement BL"] = self._split_and_trim_bl(transformed["Announcement BL"])

            transformed["Promoted Groups"] = self._parse_group_values(transformed["Promoted Groups"])
            transformed["Category Groups"] = self._parse_group_values(transformed["Category Groups"])

            transformed["Threshold Metric"] = transformed["Threshold Metric"].lower()
            transformed["Segment Definitions"] = int(str(transformed["Segment Definitions"])[0])
            # Sanitize analyst name - remove special characters, keep only alphanumeric and underscore
            transformed["Requested by"] = re.sub(r'[^a-zA-Z0-9_]', '', transformed["Requested by"].replace(" ", "_")).lower()

            self.final_parameters = transformed

            logger.info("Parameter transformation completed for request ID=%s", transformed.get("ID"))
            return self.final_parameters

        except Exception as e:
            logger.exception("Parameter transformation failed for request ID=%s", self.parameters.get("ID"))
            self.final_parameters = "ERROR"
            return self.final_parameters

    def executing_vmr_scorecard(self):
        try:
            if self.final_parameters == "ERROR" or self.final_parameters is None:
                raise ValueError("Final parameters are not available. Run parameters_transformation first.")

            if self.final_parameters["Status"] != "UNDONE":
                logger.info(
                    "Skipping execution for request ID=%s because status is '%s'",
                    self.final_parameters.get("ID"),
                    self.final_parameters.get("Status"),
                )
                return

            logger.info("Starting scorecard execution for request ID=%s", self.final_parameters.get("ID"))

            threshold_unit = self.final_parameters["Threshold Metric"]
            id_param = self.final_parameters["ID"]
            analyst = self.final_parameters["Requested by"]
            requested_by = self.final_parameters["Requested by"]
            brand_nm = self.final_parameters["Client Name"]
            report_name_for_export = self.final_parameters["Client Name"]
            brand_nbr = self.final_parameters["Promoted Groups"]
            cat_nbr = self.final_parameters["Category Groups"]
            bl_codes = self.final_parameters["Reward BL"]
            lmc_list_id = self.final_parameters["LMC List ID"]
            program_nm = self.final_parameters["Program Name"]
            segment_type = self.final_parameters["Segment Definitions"]
            custom_brand_desc_path = "/analytical_services/Public/moricejuan/Catalina_PG_Categories.xlsx"
            label_level = self.final_parameters["Retailer Category Level"]
            pre_weeks = 52
            campaign_name = brand_nm
            dir_name = brand_nm
            self.final_folder_name = f"{program_nm[:25]}"

            if self.final_parameters.get("Mapping File name"):
                custom_mp_nm = self.final_parameters["Mapping File name"]

            if self.final_parameters.get("Announcement BL"):
                announcement = self.final_parameters["Announcement BL"]

            script_path = Path(__file__).resolve().parent.parent / "reports" / "omni_vmr_scorecard_2024_new_up.py"
            with script_path.open("r", encoding="utf-8") as file:
                script_code = file.read()

            try:
                exec(script_code)
                self.new_status = "COMPLETED"
                logger.info("Scorecard execution completed for request ID=%s", self.final_parameters.get("ID"))
            except Exception:
                self.new_status = "ERROR"
                logger.error(
                    "Scorecard execution failed for request ID=%s\n%s",
                    self.final_parameters.get("ID"),
                    traceback.format_exc(),
                )

        except Exception as e:
            logger.exception("Unexpected execution error for request ID=%s", self.parameters.get("ID"))
            raise RuntimeError(f"Error executing VMR scorecard: {e}") from e

    def updating_line_on_sharepoint(self):
        try:
            if self.final_parameters in (None, "ERROR"):
                raise ValueError("Final parameters are not available.")
            if self.new_status is None:
                raise ValueError("Execution status is not available. Run executing_vmr_scorecard first.")

            logger.info(
                "Updating SharePoint status for request ID=%s with status=%s",
                self.final_parameters.get("ID"),
                self.new_status,
            )

            authcookie = Office365(
                self.parameters["SHAREPOINT_BASE_URL"],
                username=self.parameters["SHAREPOINT_USERNAME"],
                password=self.parameters["SHAREPOINT_PASSWORD"],
            ).GetCookies()

            site = Site(
                self.parameters["SHAREPOINT_SITE_URL"],
                version=Version.v365,
                authcookie=authcookie,
            )

            sp_list = site.List(self.parameters["SHAREPOINT_LIST_TITLE"])

            list_data = [{"ID": self.final_parameters["ID"], "Status": self.new_status}]
            sp_list.UpdateListItems(data=list_data, kind="Update")

            if self.new_status == "COMPLETED":
                list_data = [{
                    "ID": self.final_parameters["ID"],
                    "final_request_name": self.final_folder_name,
                }]
                sp_list.UpdateListItems(data=list_data, kind="Update")

            logger.info(
                "SharePoint status updated successfully for request ID=%s",
                self.final_parameters.get("ID"),
            )

        except Exception as e:
            logger.exception("Failed updating SharePoint for request ID=%s", self.parameters.get("ID"))
            raise RuntimeError(f"Updating status line process had an issue: {e}") from e

    @staticmethod
    def _sanitize_name(value: str, replace_dash_with_underscore: bool) -> str:
        value = value.lower()
        replacements = {
            ",": "",
            ".": "",
            " ": "_",
            "*": "",
            "´": "",
            "(": "",
            ")": "",
            "&": "",
            "/": "",
            "?": "",
            ">": "",
            "<": "",
            ":": "",
            "+": "",
            "'": "",
            "â": "",
            "|": "",
        }

        if replace_dash_with_underscore:
            replacements["-"] = "_"
        else:
            replacements["-"] = ""

        for old, new in replacements.items():
            value = value.replace(old, new)

        return value

    @staticmethod
    def _split_and_trim_bl(value: str) -> list[str]:
        if not isinstance(value, str) or not value.strip():
            return []

        items = value.split(",")
        items = [x.strip() for x in items if x and x.strip()]
        return items

    @staticmethod
    def _parse_group_values(value):
        if isinstance(value, list):
            return value
        if isinstance(value, str) and "," in value:
            return [int(i.strip()) for i in value.split(",") if i.strip()]
        if isinstance(value, str) and value.strip():
            return [int(value.strip())]
        return value
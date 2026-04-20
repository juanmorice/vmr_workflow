import logging
import re
import traceback
from pathlib import Path

from other_modules.excelupdater import ExcelUpdater


logger = logging.getLogger(__name__)


class RunningVMRScorecard:
    def __init__(self, parameters: dict, excel_file_path: str = None):
        self.parameters = parameters
        self.excel_file_path = excel_file_path
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

    def _parse_numeric_input(self, value, allow_zero: bool = True) -> int | None:
        """
        Parse and sanitize numeric input from Excel/Form.
        
        Args:
            value: The input value (can be str, int, float, or None)
            allow_zero: If False, treat 0 as None (not provided)
            
        Returns:
            int or None if invalid/empty
        """
        if value is None:
            return None
        
        # Convert to string and strip whitespace
        str_value = str(value).strip()
        
        # Empty string is treated as not provided
        if str_value == '' or str_value.lower() in ('none', 'null', 'nan'):
            return None
        
        try:
            # Handle floats (e.g., 14.0 -> 14)
            num_value = int(float(str_value))
            
            # Negative values are invalid
            if num_value < 0:
                logger.warning("Ignoring negative numeric input: %s", value)
                return None
            
            # If allow_zero is False, treat 0 as not provided
            if not allow_zero and num_value == 0:
                return None
            
            return num_value
            
        except (ValueError, TypeError):
            logger.warning("Could not parse numeric input: %s", value)
            return None

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
            # analyst is sanitized for DB table name compatibility (remove special chars, lowercase)
            analyst = re.sub(r'[^a-zA-Z0-9_]', '', self.final_parameters["Requested by"].replace(" ", "_")).lower()
            requested_by = self.final_parameters["Requested by"]
            # brand_nm is sanitized for DB table name compatibility (remove special chars, lowercase)
            brand_nm = re.sub(r'[^a-zA-Z0-9_]', '', self.final_parameters["Client Name"].replace(" ", "_")).lower()
            report_name_for_export = self.final_parameters["Client Name"]
            brand_nbr = self.final_parameters["Promoted Groups"]
            cat_nbr = self.final_parameters["Category Groups"]
            BL_CODES = self.final_parameters["Reward BL"]
            lmc_list_id = self.final_parameters["LMC List ID"]
            program_nm = self.final_parameters["Program Name"]
            segment_type = self.final_parameters["Segment Definitions"]
            custom_brand_desc_path = "/analytical_services/Public/moricejuan/Catalina_PG_Categories.xlsx"
            pre_weeks = 52
            campaign_name = report_name_for_export
            dir_name = report_name_for_export
            self.final_folder_name = f"{program_nm[:25]}"

            # Optional parameters - provide defaults for empty values
            # Retailer Category Level: only used for segment_type 4 or 5, defaults to 2
            label_level = self.final_parameters.get("Retailer Category Level") or 2
            
            # Mapping File name: not always needed
            custom_mp_nm = self.final_parameters.get("Mapping File name") or ""
            
            # Announcement BL: not always needed (uses capital A to match scorecard script)
            Announcement = self.final_parameters.get("Announcement BL") or []
            
            # User-provided Redemption Days: overrides calculated value ONLY when provided
            # Logic: If user provides a value different from calculated, use user value
            user_redemption_days = self._parse_numeric_input(
                self.final_parameters.get("Redemption Days"), 
                allow_zero=False
            )
            
            # User-provided Minimum Threshold: used ONLY when calculated value is 0 or null
            # Logic: If DB returns 0/null AND user provides a value, use user value
            user_min_threshold = self._parse_numeric_input(
                self.final_parameters.get("Minimum Threshold"),
                allow_zero=True
            )

            script_path = Path(__file__).resolve().parent / "reports" / "omni_vmr_scorecard_2024_new_up.py"
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

    def updating_line_on_excel(self):
        """
        Update the Excel file based on execution status.
        - COMPLETED: deletes the row from Excel
        - ERROR: updates the Status column to ERROR with red background
        """
        try:
            if self.final_parameters in (None, "ERROR"):
                raise ValueError("Final parameters are not available.")
            if self.new_status is None:
                raise ValueError("Execution status is not available. Run executing_vmr_scorecard first.")

            logger.info(
                "Updating Excel status for request ID=%s with status=%s",
                self.final_parameters.get("ID"),
                self.new_status,
            )

            updater = ExcelUpdater(file_path=self.excel_file_path)
            updater.update_status(
                row_id=self.final_parameters["ID"],
                new_status=self.new_status,
                final_folder_name=self.final_folder_name,
            )

            logger.info(
                "Excel status updated successfully for request ID=%s",
                self.final_parameters.get("ID"),
            )

            return self.new_status

        except Exception as e:
            logger.exception("Failed updating Excel for request ID=%s", self.parameters.get("ID"))
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
        if isinstance(value, (int, float)):
            return [int(value)]
        if isinstance(value, str) and "," in value:
            return [int(i.strip()) for i in value.split(",") if i.strip()]
        if isinstance(value, str) and value.strip():
            return [int(value.strip())]
        return value

import os
import logging
import json
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

logger = logging.getLogger(__name__)


class ExcelUpdater:
    """
    A class to track execution status for Excel requests.
    
    Since the form-linked Excel file cannot be modified, status updates are
    written to a separate tracking file in the outputs directory.
    
    Parameters:
        file_path (str): Path to the original Excel file (for reference only).
        tracking_file (str): Path to status tracking JSON file.
    """

    def __init__(self, file_path: str = None, sheet_name: str = None):
        self.file_path = file_path or os.getenv("EXCEL_FILE_PATH")
        # Status tracking file in outputs directory
        self.tracking_file = os.getenv("STATUS_TRACKING_FILE", "/opt/airflow/outputs/status_tracking.json")
        
        if not self.file_path:
            raise ValueError("Excel file path must be provided or set via EXCEL_FILE_PATH env var")
    
    def _load_tracking(self) -> dict:
        """Load existing tracking data or return empty dict."""
        if os.path.exists(self.tracking_file):
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}
    
    def _save_tracking(self, data: dict) -> None:
        """Save tracking data to file."""
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.tracking_file), exist_ok=True)
        with open(self.tracking_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)

    def update_status(self, row_id: int, new_status: str, final_folder_name: str = None) -> bool:
        """
        Track the status of a request by ID.
        
        Status is saved to a JSON tracking file (not the original Excel).
        
        Parameters:
            row_id (int): The ID of the row/request
            new_status (str): The new status ('COMPLETED' or 'ERROR')
            final_folder_name (str): Optional folder name where outputs were saved
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info("Tracking status for ID=%s: %s", row_id, new_status)
            
            # Load existing tracking data
            tracking = self._load_tracking()
            
            # Update status for this ID
            tracking[str(row_id)] = {
                "status": new_status,
                "updated_at": datetime.now().isoformat(),
                "output_folder": final_folder_name
            }
            
            # Save tracking data
            self._save_tracking(tracking)
            logger.info("Status tracked successfully for ID=%s", row_id)
            
            return True

        except Exception as e:
            logger.exception("Failed to track status for ID=%s", row_id)
            raise RuntimeError(f"Error tracking status: {e}") from e
    
    def get_completed_ids(self) -> set:
        """Get set of IDs that have been completed."""
        tracking = self._load_tracking()
        return {int(k) for k, v in tracking.items() if v.get("status") == "COMPLETED"}
    
    def is_completed(self, row_id: int) -> bool:
        """Check if a specific ID has been completed."""
        return row_id in self.get_completed_ids()

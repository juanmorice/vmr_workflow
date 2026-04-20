import os
import pandas as pd
from typing import List, Optional


class ExcelFileFetcher:
    """
    A class to fetch data from Excel files.
    Drop-in replacement for SharePointListFetcher.
    
    Parameters can be passed directly or via environment variables:
        - EXCEL_FILE_PATH: Path to the Excel file
        - EXCEL_SHEET_NAME: Sheet name or index (default: 0)
        - EXCEL_HEADER_ROW: Header row number (default: 0)
    """

    def __init__(
        self,
        file_path: str = None,
        sheet_name: str | int = None,
        header: int = None
    ):
        self.file_path = file_path or os.getenv("EXCEL_FILE_PATH")
        self.sheet_name = sheet_name if sheet_name is not None else os.getenv("EXCEL_SHEET_NAME", 0)
        self.header = header if header is not None else int(os.getenv("EXCEL_HEADER_ROW", 0))
        
        # Convert sheet_name to int if it's a numeric string
        if isinstance(self.sheet_name, str) and self.sheet_name.isdigit():
            self.sheet_name = int(self.sheet_name)

    def get_list_items(self, fields: Optional[List[str]] = None) -> List[dict]:
        """
        Read data from the Excel file and return as a list of dictionaries.
        
        Parameters:
            fields (List[str] | None): List of column names to retrieve. 
                                       If None, returns all columns.
        
        Returns:
            List[dict]: List of records from the Excel file.
        """
        try:
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=self.header
            )

            if fields:
                missing_fields = [f for f in fields if f not in df.columns]
                if missing_fields:
                    raise ValueError(f"Columns not found in Excel file: {missing_fields}")
                df = df[fields]

            # Convert timestamps to strings for XCom serialization
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')

            # Treat empty/null Status as "UNDONE" (for Microsoft Forms integration)
            if 'Status' in df.columns:
                df['Status'] = df['Status'].fillna('UNDONE').replace('', 'UNDONE')

            # Normalize "Created date" to "Created Date" for consistency
            df.columns = [col if col.lower() != 'created date' else 'Created Date' for col in df.columns]

            # Replace NaN/NaT with empty string for JSON serialization
            df = df.fillna('')
            
            # Filter out empty rows (rows where all fields except ID, Status, and Created Date are empty)
            non_key_cols = [c for c in df.columns if c not in ['ID', 'Status', 'Created Date']]
            if non_key_cols:
                # Keep rows where at least one non-key column has data
                mask = df[non_key_cols].apply(lambda row: any(str(v).strip() != '' for v in row), axis=1)
                df = df[mask]
            
            records = df.to_dict(orient='records')
            
            # Extra safety: replace any remaining float('nan') values
            import math
            import json
            def clean_nan(obj):
                if isinstance(obj, float) and math.isnan(obj):
                    return None
                return obj
            
            cleaned_records = [{k: clean_nan(v) for k, v in record.items()} for record in records]
            
            # Filter out already-completed IDs from tracking file
            tracking_file = os.getenv("STATUS_TRACKING_FILE", "/opt/airflow/outputs/status_tracking.json")
            completed_ids = set()
            if os.path.exists(tracking_file):
                try:
                    with open(tracking_file, 'r') as f:
                        tracking = json.load(f)
                        completed_ids = {int(k) for k, v in tracking.items() if v.get("status") == "COMPLETED"}
                except (json.JSONDecodeError, IOError):
                    pass
            
            # Mark completed IDs as already done
            for record in cleaned_records:
                if record.get('ID') and int(record['ID']) in completed_ids:
                    record['Status'] = 'COMPLETED'  # Will be skipped by DAG
            
            return cleaned_records

        except FileNotFoundError:
            raise RuntimeError(f"Excel file not found: {self.file_path}")
        except Exception as e:
            raise RuntimeError(f"Error reading Excel file: {e}")

    def get_dataframe(self, fields: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Read data from the Excel file and return as a DataFrame.
        
        Parameters:
            fields (List[str] | None): List of column names to retrieve.
                                       If None, returns all columns.
        
        Returns:
            pd.DataFrame: DataFrame with the Excel data.
        """
        try:
            df = pd.read_excel(
                self.file_path,
                sheet_name=self.sheet_name,
                header=self.header
            )

            if fields:
                missing_fields = [f for f in fields if f not in df.columns]
                if missing_fields:
                    raise ValueError(f"Columns not found in Excel file: {missing_fields}")
                df = df[fields]

            return df

        except FileNotFoundError:
            raise RuntimeError(f"Excel file not found: {self.file_path}")
        except Exception as e:
            raise RuntimeError(f"Error reading Excel file: {e}")


# Alias for drop-in replacement
SharePointListFetcher = ExcelFileFetcher

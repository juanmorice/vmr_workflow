#!/usr/bin/env python3
"""
VMR Scorecard Standalone Script
================================
This script runs the VMR scorecard pipeline without Airflow orchestration.
It mirrors the logic in dags/vmr_dag.py but can be executed directly via crontab.

Usage:
    python vmr_standalone.py                    # Process all UNDONE rows
    python vmr_standalone.py --id 24            # Process specific ID only
    python vmr_standalone.py --dry-run          # Preview without executing
    python vmr_standalone.py --id 24 --dry-run  # Preview specific ID

Crontab example (every weekday at 9:00, 12:00, 16:00 EST):
    0 9,12,16 * * 1-5 cd /path/to/project && python src/vmr_standalone.py >> logs/vmr_standalone.log 2>&1
"""

import os
import sys
import glob
import logging
import argparse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from pathlib import Path

# Add src directory to Python path
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
sys.path.insert(0, str(script_dir))
sys.path.insert(0, str(project_root))

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv(project_root / '.env')

# Setup logging
def setup_logging(log_dir: Path = None):
    """Configure logging to both console and file."""
    if log_dir is None:
        log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"vmr_standalone_{datetime.now().strftime('%Y%m%d')}.log"
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    return logging.getLogger('vmr_standalone')


def send_email_smtp(to: str, subject: str, html_content: str, files: list = None):
    """
    Send email using SMTP (replacement for Airflow's send_email).
    Uses SMTP settings from environment variables.
    """
    smtp_host = os.getenv('AIRFLOW__SMTP__SMTP_HOST', 'smtp.office365.com')
    smtp_port = int(os.getenv('AIRFLOW__SMTP__SMTP_PORT', '587'))
    smtp_user = os.getenv('AIRFLOW__SMTP__SMTP_USER')
    smtp_password = os.getenv('AIRFLOW__SMTP__SMTP_PASSWORD')
    smtp_mail_from = os.getenv('AIRFLOW__SMTP__SMTP_MAIL_FROM', smtp_user)
    smtp_starttls = os.getenv('AIRFLOW__SMTP__SMTP_STARTTLS', 'True').lower() == 'true'
    
    if not smtp_user or not smtp_password:
        logging.warning("SMTP credentials not configured - skipping email")
        return False
    
    try:
        msg = MIMEMultipart()
        msg['From'] = smtp_mail_from
        msg['To'] = to
        msg['Subject'] = subject
        
        # Attach HTML body
        msg.attach(MIMEText(html_content, 'html'))
        
        # Attach files if provided
        if files:
            for file_path in files:
                if os.path.exists(file_path):
                    with open(file_path, 'rb') as f:
                        part = MIMEBase('application', 'octet-stream')
                        part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename={os.path.basename(file_path)}'
                        )
                        msg.attach(part)
        
        # Send email
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            if smtp_starttls:
                server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        
        logging.info(f"Email sent successfully to {to}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        return False


def excel_importing(logger) -> list:
    """
    Task 1: Import rows from Excel file.
    Returns list of parameter dictionaries.
    """
    logger.info("=== TASK 1: Excel Importing ===")
    
    from excelfilefetcher import ExcelFileFetcher
    
    excel_path = os.getenv('EXCEL_FILE_PATH', 'src/template_form.xlsx')
    logger.info(f"EXCEL_FILE_PATH: {excel_path}")
    
    fetcher = ExcelFileFetcher()
    logger.info(f"Fetcher file_path: {fetcher.file_path}")
    
    fetched_data = fetcher.get_list_items()
    logger.info(f"Fetched {len(fetched_data)} rows from Excel")
    
    # Log first row sample
    if fetched_data:
        logger.info(f"Sample row keys: {list(fetched_data[0].keys())}")
    
    return fetched_data


def getting_upc_dataframe(parameters: list, logger, filter_id: int = None) -> list:
    """
    Task 2: Load UPC data from LMC for UNDONE rows.
    """
    logger.info("=== TASK 2: Getting UPC DataFrame ===")
    
    from gettinglmcdataframe import GettingLMCDataFrame
    
    for params in parameters:
        try:
            # Skip if not UNDONE
            if params['Status'] != 'UNDONE':
                continue
            
            # Skip if filter_id specified and doesn't match
            if filter_id is not None and params.get('ID') != filter_id:
                continue
            
            logger.info(f"Processing UPC data for row ID={params.get('ID')}")
            upc_creator = GettingLMCDataFrame(params)
            upc_creator.getting_dataframe()
            
        except Exception as e:
            logger.error(f"Row {params.get('ID')} failed during UPC loading: {e}")
            continue
    
    return parameters


def running_vmr_scorecard(parameters: list, logger, filter_id: int = None, dry_run: bool = False) -> list:
    """
    Task 3: Execute VMR scorecard for UNDONE rows.
    """
    logger.info("=== TASK 3: Running VMR Scorecard ===")
    
    from runningvmrscorecard_excel import RunningVMRScorecard
    
    excel_path = os.getenv('EXCEL_FILE_PATH', 'src/template_form.xlsx')
    outputs_dir = str(project_root / 'outputs')
    
    results = []
    
    for params in parameters:
        try:
            # Skip if not UNDONE
            if params['Status'] != 'UNDONE':
                continue
            
            # Skip if filter_id specified and doesn't match
            if filter_id is not None and params.get('ID') != filter_id:
                continue
            
            request_id = params.get('ID')
            logger.info(f"Running scorecard for row ID={request_id}")
            
            if dry_run:
                logger.info(f"[DRY RUN] Would execute scorecard for ID={request_id}")
                results.append({'ID': request_id, 'status': 'DRY_RUN'})
                continue
            
            scorecard_runner = RunningVMRScorecard(params, excel_file_path=excel_path)
            scorecard_runner.parameters_transformation()
            scorecard_runner.executing_vmr_scorecard()
            status = scorecard_runner.updating_line_on_excel()
            results.append({'ID': request_id, 'status': status})
            
            # Send email if completed
            if status == 'COMPLETED':
                # Find output files
                output_files = []
                for pattern in [f'**/*_ID{request_id}_*.xlsx', f'**/*_ID{request_id}_*.pptx']:
                    matches = glob.glob(os.path.join(outputs_dir, pattern), recursive=True)
                    output_files.extend(matches)
                
                if output_files:
                    logger.info(f"Found {len(output_files)} output files for ID={request_id}")
                    send_email_smtp(
                        to=params.get('Email'),
                        subject=f"VMR Scorecard Completed for Request ID={request_id}",
                        html_content=f"<p>Your VMR scorecard request with ID {request_id} has been completed successfully.</p><p>Attached files: {len(output_files)}</p>",
                        files=output_files
                    )
                else:
                    logger.warning(f"No output files found for ID={request_id}")
            
        except Exception as e:
            logger.error(f"Row {params.get('ID')} failed during scorecard execution: {e}")
            results.append({'ID': params.get('ID'), 'status': 'ERROR'})
    
    return results


def main():
    """Main entry point for standalone VMR pipeline."""
    parser = argparse.ArgumentParser(
        description='VMR Scorecard Standalone Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        '--id', type=int, default=None,
        help='Process only the specified request ID'
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Preview what would be processed without executing'
    )
    parser.add_argument(
        '--log-dir', type=str, default=None,
        help='Directory for log files (default: project_root/logs)'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_dir = Path(args.log_dir) if args.log_dir else None
    logger = setup_logging(log_dir)
    
    logger.info("=" * 60)
    logger.info("VMR Scorecard Standalone Pipeline Started")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info(f"Filter ID: {args.id or 'ALL'}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info("=" * 60)
    
    try:
        # Task 1: Excel Importing
        parameters = excel_importing(logger)
        
        if not parameters:
            logger.warning("No rows found in Excel file")
            return
        
        # Count UNDONE rows
        undone_count = sum(1 for p in parameters if p['Status'] == 'UNDONE')
        if args.id:
            undone_count = sum(1 for p in parameters if p['Status'] == 'UNDONE' and p.get('ID') == args.id)
        
        logger.info(f"Found {undone_count} UNDONE row(s) to process")
        
        if undone_count == 0:
            logger.info("No UNDONE rows to process - exiting")
            return
        
        if args.dry_run:
            logger.info("[DRY RUN MODE] - No changes will be made")
            for p in parameters:
                if p['Status'] == 'UNDONE':
                    if args.id is None or p.get('ID') == args.id:
                        logger.info(f"[DRY RUN] Would process ID={p.get('ID')}: {p.get('Program Name')}")
            return
        
        # Task 2: Getting UPC DataFrame
        parameters = getting_upc_dataframe(parameters, logger, filter_id=args.id)
        
        # Task 3: Running VMR Scorecard
        results = running_vmr_scorecard(parameters, logger, filter_id=args.id, dry_run=args.dry_run)
        
        # Summary
        logger.info("=" * 60)
        logger.info("Pipeline Completed - Summary:")
        for result in results:
            logger.info(f"  ID={result['ID']}: {result['status']}")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

from airflow.sdk import dag, task 
from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
import logging
from airflow.utils.email import send_email

logger = logging.getLogger(__name__)

local_tz = pendulum.timezone("US/Eastern")

@dag( 
        dag_id = "vmr_dag",
        start_date = pendulum.datetime(2026, 3, 17, tz=local_tz),
        schedule = CronTriggerTimetable("0 9,12,16 * * 1-5", timezone=local_tz),  # Every weekday at 9:00, 12:00, 16:00
        end_date = pendulum.datetime(2026, 3, 17, tz=local_tz) + pendulum.duration(days=365*2),  # End after 2 years
        is_paused_upon_creation=False,
        catchup=False
)

def vmr_dag():

    @task.python
    def excel_importing(**kwargs):
        import os
        logger.info("=== CHECKPOINT 1: Task started ===")
        
        logger.info("=== CHECKPOINT 2: Importing ExcelFileFetcher ===")
        from excelfilefetcher import ExcelFileFetcher
        
        logger.info("=== CHECKPOINT 3: Creating fetcher instance ===")
        logger.info(f"EXCEL_FILE_PATH env: {os.getenv('EXCEL_FILE_PATH', 'NOT SET')}")
        fetcher = ExcelFileFetcher()
        logger.info(f"Fetcher file_path: {fetcher.file_path}")
        
        logger.info("=== CHECKPOINT 4: Calling get_list_items ===")
        fetched_data = fetcher.get_list_items()
        logger.info(f"=== CHECKPOINT 5: Data fetched, {len(fetched_data)} rows ===")
        
        # Log first row sample (if exists)
        if fetched_data:
            logger.info(f"Sample row keys: {list(fetched_data[0].keys())}")
            logger.info(f"Sample row: {fetched_data[0]}")
        
        logger.info("=== CHECKPOINT 6: Pushing to XCom ===")
        ti = kwargs['ti']
        ti.xcom_push(key='return_result', value=fetched_data)
        logger.info("=== CHECKPOINT 7: Task completed successfully ===")
 
    @task.python
    def getting_upc_dataframe(**kwargs):
        from gettinglmcdataframe import GettingLMCDataFrame  # Lazy import
        ti = kwargs['ti']
        parameters = ti.xcom_pull(key='return_result', task_ids='excel_importing')
        logger.info(f"Pulled {len(parameters) if parameters else 0} rows from excel_importing")
        
        # Process each row from Excel
        for params in parameters:
            try:
                if params['Status'] == 'UNDONE':
                    logger.info(f"Processing row ID={params.get('ID')}")
                    upc_creator = GettingLMCDataFrame(params) 
                    upc_creator.getting_dataframe()
            except Exception as e:
                logger.error(f"Row {params.get('ID')} failed: {e}")
                continue
        
        ti.xcom_push(key='return_result', value=parameters)

    @task.python
    def running_vmr_scorecard(**kwargs):
        import os
        import glob
        from runningvmrscorecard_excel import RunningVMRScorecard  # Lazy import
        ti = kwargs['ti']
        parameters = ti.xcom_pull(key='return_result', task_ids='getting_upc_dataframe')
        logger.info(f"Pulled {len(parameters) if parameters else 0} rows from getting_upc_dataframe")
        
        # Use absolute path for Excel file (relative paths cause permission issues in Docker)
        excel_path = os.getenv('EXCEL_FILE_PATH', 'src/template_form.xlsx')
        if not excel_path.startswith('/'):
            excel_path = f'/opt/airflow/{excel_path}'
        
        results = []
        for params in parameters:
            try:
                if params['Status'] == 'UNDONE':
                    logger.info(f"Running scorecard for row ID={params.get('ID')}")
                    scorecard_runner = RunningVMRScorecard(params, excel_file_path=excel_path)
                    scorecard_runner.parameters_transformation()
                    scorecard_runner.executing_vmr_scorecard()
                    status = scorecard_runner.updating_line_on_excel()
                    results.append({'ID': params.get('ID'), 'status': status})


                    if status == 'COMPLETED':
                        # Find output files for this request ID in the outputs folder
                        request_id = params.get('ID')
                        outputs_dir = '/opt/airflow/outputs'
                        
                        # Search for files with _ID{id}_ pattern (both .xlsx and .pptx)
                        output_files = []
                        for pattern in [f'**/*_ID{request_id}_*.xlsx', f'**/*_ID{request_id}_*.pptx']:
                            matches = glob.glob(os.path.join(outputs_dir, pattern), recursive=True)
                            output_files.extend(matches)
                        
                        if output_files:
                            logger.info(f"Found {len(output_files)} output files for ID={request_id}: {output_files}")
                            send_email(
                                to=params.get('Email'),
                                subject=f"VMR Scorecard Completed for Request ID={request_id}",
                                html_content=f"<p>Your VMR scorecard request with ID {request_id} has been completed successfully.</p><p>Attached files: {len(output_files)}</p>",
                                files=output_files
                            )
                        else:
                            logger.warning(f"No output files found for ID={request_id}")
                            send_email(
                                to=params.get('Email'),
                                subject=f"VMR Scorecard Completed for Request ID={request_id}",
                                html_content=f"<p>Your VMR scorecard request with ID {request_id} has been completed successfully.</p><p>Note: Output files could not be attached.</p>",
                            )



            except Exception as e:
                logger.error(f"Row {params.get('ID')} failed during scorecard execution: {e}")
                results.append({'ID': params.get('ID'), 'status': 'ERROR'})
        
        ti.xcom_push(key='return_result', value=results)

    first = excel_importing()
    second = getting_upc_dataframe()
    third = running_vmr_scorecard()

    first >> second >> third

vmr_dag()
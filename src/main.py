import pandas as pd
from tqdm import tqdm
import os, logging, sys
from src.argument.arg_parser import parse_and_store_args
from src.components.logfactory import get_logger, set_global_log_level

set_global_log_level(logging.INFO)
logger = get_logger(name='main', log_level=logging.INFO, log_to_file=True)

from src.gcs_utils.gcs_connection import GoogleOAuth2Service
from src.gcs_utils.gcs_orchestration import pull_gdrive_data
from src.data_processor.fetch_src_file import FileFetcher
from src.data_processor.data_ingestion import DataOrchestrator
from src.data_processor.data_transformer import DataTransformation
from src.db_operations.delta_lake import DB_DeltaHandler
from src.components.utils import parallel_apply_process, remove_temp_dir
from src.gcs_utils.gdrive_operations import upload_or_update_file_to_gdrive
from src.gcs_utils.gdrive_operations import  delete_file_from_gdrive
from constants import ConstantRetriever, DBConstants
from src.db_operations.sql_procedure import SQL_Procedure
from src.utils.dq_integrity import safe_list
import datetime
from dotenv import load_dotenv

# Load Environment details from .env/ file
load_dotenv()

# Progress Bar data processing
tqdm.pandas(desc="Progress Bar")


def main():

    # arguments recieved from terminal which trigering the script. 
    args = parse_and_store_args()
    SRC_FOLDER_ID = args['src_gdrive']
    BACKUP_FOLDER_ID = args['backup']

    job_run_date = datetime.date.today()
    
    try:
        temp_dir, file_list_nm, file_list_dir = pull_gdrive_data(folder_id=SRC_FOLDER_ID)

        # Flag to check if files are present in GDrive
        is_present_in_gdrive = len(safe_list(file_list_nm))>0

        # Condition if no files are present in GDrive, then pull from local
        if not is_present_in_gdrive:
            temp_dir, local_file_list_nm, local_file_list_dir = FileFetcher.pull_local_file(src_folder=ConstantRetriever.SOURCE_LOCAL_DIR)

            # Append the list of files from both sources
            file_list_nm = safe_list(file_list_nm) + safe_list(local_file_list_nm)
            file_list_dir = safe_list(file_list_dir) + safe_list(local_file_list_dir)
            # temp_dir = safe_list(temp_dir) + safe_list(local_temp_dir)
    except Exception as e:
        logger.error(f'Error occured while pulling data from Sources: {e}')
        logger.error(f'Exiting the Program')
        sys.exit(1)

    idx = 0

    # Data Orchestration Started
    logger.info(f"------------------------------------------------------------------")
    logger.info('Orchestration in Progress.......')
    logger.info(f"------------------------------------------------------------------")

    for file in file_list_dir:

        data_orch_obj = DataOrchestrator(file)
        df = data_orch_obj.get_ingestion_pipeline()

        logger.debug(f'Dataframe Generated for: |`{file_list_nm[idx]}`|')
        logger.debug(f'Columns  {df.columns}')
        
        transformer = DataTransformation()
        logger.info(f'Data Transformation for file - `{file_list_nm[idx]}` in progress.......')

        # Progress bar for categorization wil applying parallel processing
        tqdm.pandas(desc="Categorizeing Data")
        logger.info(f'Processing Transformation on `SUBCATEGORY`......')
        df['Subcategory'] = df['Particulars'].progress_apply(transformer.particular_scrapper)
        if 'Subcategory' in df.columns:
            logger.info(f'Processing Transformation on `CATEGORY`......')
            df['Category'] = df['Subcategory'].progress_apply(transformer.category_mapper)

        # Data Copy for Gsheet
        parsed_df = df.copy()

        # Backup the dataframes to googledrive on every run
        file_upload_nm = file_list_nm[idx].replace('.pdf', '.csv')
        file_upload = f"{job_run_date}_{file_upload_nm}"

        # Backup the Source file after Data Transformation as a Backups
        try:
            upload_or_update_file_to_gdrive(data=df, 
                                            gdrive_folder_id=BACKUP_FOLDER_ID, 
                                            new_file_name=file_upload)
        except Exception as e: 
            logger.error(f'Error while uploading file to GDrive: {e}')


        if 'Bank' in df.columns:
            bank_name = df['Bank'][0]

        # Make Database Directory
        os.makedirs(os.path.dirname(DBConstants.DB_PATH), exist_ok=True)

        # Create DeltaHandler instance
        handler = DB_DeltaHandler(db_name=DBConstants.DB_PATH, metadata_table=DBConstants.LOAD_STATUS_TABLE)

        # Load data with delta logic; this will also check & create the table if needed
        load_delta_status = handler.load_delta(df, expected_columns=DBConstants.TRANSACTION_T_COLS, 
                        bank_name=bank_name, target_table=DBConstants.TRANSACTION_TABLE)
        
        # Functionality to Update Rows on Database
        if load_delta_status == 1:
            SQL_Procedure.trigger_sql_procedure()        
        
        # Remove the source file from gdrive after processing
        if is_present_in_gdrive:
            try:
                delete_file_from_gdrive(file_name=file_list_nm[idx], folder_id=SRC_FOLDER_ID)
            except Exception as e:
                logger.error(f'Error Occured while removing SOURCE file - `{file_list_nm[idx]}` from Gdrive: {e}')
        
        logger.info(f'---------------------------------------------------------------------------------')
        idx = idx +1

    # Removing the temp directory after Operation
    delete_temp_dir = remove_temp_dir(temp_dir)
    if delete_temp_dir:
        logger.warning(f'Deleted Temporary working directory')

if __name__ == "__main__":
    main()
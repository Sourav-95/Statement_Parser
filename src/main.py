import pandas as pd
from tqdm import tqdm
import os, logging, sys
from src.argument.arg_parser import parse_and_store_args
from src.components.logfactory import get_logger, set_global_log_level

set_global_log_level(logging.INFO)
logger = get_logger(name='main', log_level=logging.INFO, log_to_file=True)

from src.gcs_utils.gcs_connection import GoogleOAuth2Service
from src.gcs_utils.gcs_orchestration import pull_gdrive_data
from src.data_processor.data_ingestion import DataOrchestrator
from src.data_processor.data_transformer import DataTransformation
from src.db_operations.delta_lake import DB_DeltaHandler
from src.components.utils import parallel_apply_process, remove_temp_dir
from src.gcs_utils.gdrive_operations import upload_or_update_file_to_gdrive
from src.gcs_utils.gdrive_operations import  delete_file_from_gdrive
from constants import ConstantRetriever, DBConstants
from src.db_operations.sql_procedure import SQL_Procedure
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
    except Exception as e:
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

        ## List of action and wildcard are passed to identify the pattern and replace.
        ## [ ('<column_to_change>'), ('<condition>')...]

        if load_delta_status == 1:
            SQL_Procedure.trigger_sql_procedure()
        
        # SQL Procedure to Transform with Dashboard Logic

        # _, _, gsheet_client = GoogleOAuth2Service.initialize_auth_service_built()

        # handler.load_delta_gsheet(parsed_df=parsed_df, 
        #                           expected_columns=DBConstants.TRANSACTION_T_COLS,
        #                           bank_name=bank_name,
        #                           sheet_id = '1peLXC2y0RcY6iBEd6X8dp_BHDSAx11tP7Ewrpkn_e34',
        #                           gsheet_client=gsheet_client)
        
        
        # Remove the source file from gdrive after processing
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
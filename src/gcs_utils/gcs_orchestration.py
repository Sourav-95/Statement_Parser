import os, sys
import json
import logging
from src.gcs_utils.gcs_connection import GoogleAuthenticator, GoogleOAuth2Service
from src.gcs_utils.gcs_operations import GoogleDriveFolderManager
from src.utils.dq_validation import DataQualityValidation
from src.components.file_unlocker import PDFUnlocker
from src.components.logfactory import get_logger
from constants import ConstantRetriever
from src.components.utils import get_file_nm_list

logger = get_logger(__name__)

def pull_gdrive_data(folder_id):

    ''' This function orchestrate all the modules of `gcs_utils` 
        1. GCS API Connection
        2. Download from Drive
        3. Validates the downloaded file
        
        Returns: 
        1. File Names Only (List of Names)
        2. File Directory TEMP (List of directory)
    '''
    drive_service, mail_service, _ = GoogleOAuth2Service.initialize_auth_service_built()
    
    # Assuming you have an authenticated Google Drive API service object named `service`
    manager = GoogleDriveFolderManager(drive_service)

    # Creation of persistent temporary directory
    script_base = ConstantRetriever.SCRIPT_BASE
    temp_dir = os.path.join(script_base, ConstantRetriever.TEMP_DOWNLOAD_DIR)
    os.makedirs(temp_dir, exist_ok=True)

    temp_file_dir_list = []
    all_metadata = []

    # List all files in the folder
    all_files = manager.list_files(folder_id, page_size=20)
    logger.info(f"Downloading files...........")

    # Download the first file if available
    if all_files:
        for file in all_files:
            destination_path = os.path.join(temp_dir, file['name'])
            file_metadata = manager.file_downloader(file['id'], destination_path)
            temp_file_dir_list.append(destination_path)
            all_metadata.append(file_metadata)
    logger.debug(f'All File Metadata files in Drive:')
    logger.debug(json.dumps(all_metadata, indent=4))
    logger.info(f"Total no of Files Downloaded to `{ConstantRetriever.TEMP_DOWNLOAD_DIR}` Location: {len(temp_file_dir_list)}")

    
    if temp_file_dir_list:
        # Data integrity Check
        dq_validation = DataQualityValidation(file_dir_list=temp_file_dir_list, all_metadata=all_metadata)
        file_to_process = dq_validation.dq_validation()

        # Decryption Stage
        unlocker = PDFUnlocker(file_paths_list=temp_file_dir_list)
        unlocked_pdfs, _ = unlocker.process_all_unlocked_files(passwords_source='./inputs/passwords.yaml',
                                                                     passwords_key='passwords')
        logger.info(f'No of files unlocked : `{len(unlocked_pdfs)}`')
        logger.debug(f'Unlocked files : {get_file_nm_list(unlocked_pdfs)}')
        
        file_to_process = unlocked_pdfs

        file_nm_to_process = get_file_nm_list(file_to_process)
        logger.info(f"List of Files to be processed:{json.dumps(file_nm_to_process)}")
        
        return temp_dir, file_nm_to_process, file_to_process
    else:
        logger.error(f'No files found in GDrive folderID: {folder_id}')
        logger.error(f'Termination the Program')
        sys.exit()
        raise FileNotFoundError
    


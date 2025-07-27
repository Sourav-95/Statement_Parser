import os
import json, shutil
from src.components.file_unlocker import PDFUnlocker
from src.components.logfactory import get_logger
from constants import ConstantRetriever
from src.components.utils import get_file_nm_list

logger = get_logger(__name__)

class FileFetcher:

    @staticmethod
    def local_file(src_folder):
        try:
            if not os.path.exists(src_folder):
                raise FileNotFoundError(f"Source folder {src_folder} does not exist.")
            else:
                file_list = []
                for root, dirs, files in os.walk(src_folder):
                    for file in files:
                        file_list.append(os.path.join(root, file))
                return file_list
        except Exception as e:
            logger.error(f"Error fetching local files: {e}")
    
    @staticmethod
    def pull_local_file(src_folder):

        ''' This function orchestrate all the modules of `gcs_utils` '''

        # Creation of persistent temporary directory
        script_base = ConstantRetriever.SCRIPT_BASE
        temp_dir = os.path.join(script_base, ConstantRetriever.TEMP_DOWNLOAD_DIR)
        os.makedirs(temp_dir, exist_ok=True)

        temp_file_dir_list = []

        # List all files in the folder
        all_files = FileFetcher.local_file(src_folder=src_folder)
        logger.info(f'Total files in the local folder: {len(all_files)}')

        # Download the first file if available
        if all_files:
            for file in all_files:
                file_nm = os.path.basename(file)
                destination_path = os.path.join(temp_dir, file_nm)

                # Move the file from source directory to temp directory
                shutil.move(file, destination_path)
                
                temp_file_dir_list.append(destination_path)

        logger.info(f"Total no of Files Downloaded to `{ConstantRetriever.TEMP_DOWNLOAD_DIR}` Location: {len(temp_file_dir_list)}")

        
        if len(temp_file_dir_list) > 0:

            # Decryption Stage
            unlocker = PDFUnlocker(file_paths_list=temp_file_dir_list)
            unlocked_pdfs, _ = unlocker.process_all_unlocked_files(passwords_source='./inputs/passwords.yaml',
                                                                        passwords_key='passwords')
            logger.debug(f'No of files unlocked : `{len(unlocked_pdfs)}`')
            logger.debug(f'Unlocked files : {get_file_nm_list(unlocked_pdfs)}')
            
            file_to_process = unlocked_pdfs

            file_nm_to_process = get_file_nm_list(file_to_process)
            
            return temp_dir, file_nm_to_process, file_to_process
        else:
            logger.error(f'No files found in Source location: {src_folder}')
from src.utils.dq_integrity import DataIntegrityChecker  # Importing the utility class for data integrity check
from typing import List
from src.components.logfactory import get_logger  # Importing the logger for logging messages
import json

logger = get_logger(__name__)

class DataQualityValidation:
    """
    This class performs data quality validation by comparing MD5 checksums 
    of local files against provided metadata.
    """

    def __init__(self, file_dir_list: List[str], all_metadata: List[dict]):
        """
        Constructor to initialize the file paths and metadata.

        :param file_dir_list: List of file paths to validate
        :param all_metadata: List of metadata dictionaries containing expected checksums
        """
        self.file_dir_list = file_dir_list
        self.all_metadata = all_metadata

    def dq_validation(self):
        """
        Perform data quality validation on the downloaded files.
        Compares MD5 checksums of the files with those in the metadata.
        
        :return: List of file paths that passed the data quality check
        """
        # Initialize the data integrity checker with file paths
        dq_validation = DataIntegrityChecker(self.file_dir_list)
        logger.info(f"------------------------------------------------------------------")
        logger.info(f"Data Integrity Checking.......")
        logger.info(f"------------------------------------------------------------------")
        
        # Generate MD5 checksums for the files
        md5_checksums = dq_validation.md5_for_files()

        # Dictionary to hold filename -> expected checksum from metadata
        required_metadata = {}
        try:
            # Extract expected MD5 checksums from the metadata
            for metadata in self.all_metadata:
                if 'md5Checksum' in metadata:
                    required_metadata[metadata['name']] = metadata['md5Checksum']
                else:
                    required_metadata[metadata['name']] = None  # If checksum not found, store as None
            
            logger.debug(f'Source file from Drive with Checksum: \n{json.dumps(md5_checksums, indent=3)}')
        except Exception as e:
            logger.error(f"Error in extracting metadata: {e}")
            
        # If local checksums exist, proceed with validation
        if md5_checksums:
            try:
                # Get list of file names from local checksums
                keys_list_md5checksum = list(md5_checksums.keys())

                # Match local checksums against expected metadata checksums
                matching_checksum_file = {
                    k: md5_checksums[k]
                    for k in md5_checksums
                    if k in required_metadata and md5_checksums[k] == required_metadata[k]
                }

                # Identify indices of files that failed the checksum validation
                key_removed = [i for i, k in enumerate(keys_list_md5checksum) if k not in matching_checksum_file]
                logger.info(f'Total Files passed with DQ Check: {len(matching_checksum_file)}')
                logger.debug(f'\n{json.dumps(matching_checksum_file, indent=3)}')       

                # Remove files that failed validation from the original list
                for idx in sorted(key_removed, reverse=True):
                    if idx < len(self.file_dir_list):
                        self.file_dir_list.pop(idx)

            except Exception as e:
                logger.error(f"Error in matching checksums: {e}")

        # Return the list of files that passed the data quality check
        return self.file_dir_list
    

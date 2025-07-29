import os
import hashlib
from typing import List
from src.components.logfactory import get_logger

logger = get_logger(__name__)

class DataIntegrityChecker:
    def __init__(self, file_paths:List[str]):
        """
        :param file_paths: List of file paths to process
        """
        self.file_paths = file_paths or []

    def md5_checksum(self, file_path, chunk_size=8192):
        """
        Calculate the MD5 checksum of a file.
        :param file_path: Path to the file
        :param chunk_size: Number of bytes to read at a time
        :return: Hexadecimal MD5 checksum string
        """
        try:
            md5 = hashlib.md5()
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    md5.update(chunk)
            return md5.hexdigest()
        except Exception as e:
            logger.error(f"Could not process {file_path}: {e}")
            return None


    def md5_for_files(self, file_paths=None):
        """
        Calculate MD5 checksums for a list of files.
        :param file_paths: List of file paths
        :return: Dictionary mapping file paths to MD5 checksums
        """
        file_paths = file_paths or self.file_paths
        checksums = {}
        for file_path in file_paths:
            try:
                checksum = self.md5_checksum(file_path)
                file_nm  = os.path.basename(file_path)
                checksums[file_nm] = checksum
            except Exception as e:
                logger.error(f"Could not process {file_path}: {e}")
        return checksums

    def get_file_sizes(self, file_paths=None):
        """
        Get sizes of a list of files.
        :param file_paths: List of file paths
        :return: Dictionary mapping file paths to their sizes in bytes
        """
        file_paths = file_paths or self.file_paths
        file_sizes = {}
        for file_path in file_paths:
            try:
                size = os.path.getsize(file_path)
                file_nm  = os.path.basename(file_path)
                file_sizes[file_nm] = size
            except OSError as e:
                logger.error(f"Could not access {file_path}: {e}")
        return file_sizes
    
def safe_list(val):
    return val if isinstance(val, list) else []

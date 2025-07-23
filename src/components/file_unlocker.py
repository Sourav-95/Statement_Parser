from pypdf import PdfReader, PdfWriter
import os
import shutil
import yaml
import logging
from src.components.logfactory import get_logger

logger = get_logger(__name__)

class PDFUnlocker:
    """
    A class to handle unlocking password-protected PDF files.
    It attempts to decrypt PDFs using a list of passwords.
    """
    def __init__(self, file_paths_list):
        self.file_paths_list = file_paths_list

    def _read_passwords_from_yaml(self, filepath, key='passwords'):
        passwords = []
        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
                if data and key in data and isinstance(data[key], list):
                    passwords = [str(p) for p in data[key]]
                else:
                    logger.warning(f"Warning: '{key}' key not found or not a list in YAML file '{filepath}'.")
        except FileNotFoundError:
            logger.error(f"Error: Password file '{filepath}' not found.")
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file '{filepath}': {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while reading the password file: {e}")
        return passwords

    def unlock_single_pdf(self, pdf_path, passwords_source, passwords_key='passwords'):
        temp_output_pdf_path = pdf_path + ".unlocked_temp"

        try:
            reader = PdfReader(pdf_path)
            if not reader.is_encrypted:
                logger.debug(f"The PDF '{os.path.basename(pdf_path)}' is not encrypted. No action needed.")
                return True

            # Try unlocking with empty password explicitly first
            temp_reader = PdfReader(pdf_path)
            if temp_reader.decrypt(''):
                try:
                    _ = temp_reader.pages[0]  # Force validation
                    reader = temp_reader
                    logger.debug(f"PDF '{os.path.basename(pdf_path)}' unlocked with empty password.")
                    writer = PdfWriter()
                    for page in reader.pages:
                        writer.add_page(page)
                    with open(temp_output_pdf_path, 'wb') as output_file:
                        writer.write(output_file)
                    os.remove(pdf_path)
                    shutil.move(temp_output_pdf_path, pdf_path)
                    return True
                except Exception as e:
                    logger.warning(f"PDF '{os.path.basename(pdf_path)}' decrypted with empty password but failed to read pages: {e}")

            # Get password list from YAML or directly
            if isinstance(passwords_source, str):  # YAML file path
                passwords_to_try = self._read_passwords_from_yaml(passwords_source, passwords_key)
                if not passwords_to_try:
                    logger.error(f"No passwords loaded from '{passwords_source}'. Cannot proceed with '{os.path.basename(pdf_path)}'.")
                    return False
            elif isinstance(passwords_source, list):  # Direct list
                passwords_to_try = passwords_source
            else:
                logger.error("Invalid password source. Must be a file path (str) or a list (list).")
                return False

            found_password = None

            for password in passwords_to_try:
                try:
                    temp_reader = PdfReader(pdf_path)
                    if temp_reader.decrypt(password):
                        try:
                            _ = temp_reader.pages[0]
                            found_password = password
                            reader = temp_reader
                            logger.debug(f"PDF '{os.path.basename(pdf_path)}' unlocked with password.")
                            break
                        except Exception as e:
                            logger.warning(f"Password '{password}' seemed to work but page read failed: {e}")
                except Exception as e:
                    logger.error(f"  Error trying password for '{os.path.basename(pdf_path)}': {e}")
                    continue

            if found_password:
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)

                with open(temp_output_pdf_path, 'wb') as output_file:
                    writer.write(output_file)

                os.remove(pdf_path)
                shutil.move(temp_output_pdf_path, pdf_path)
                logger.debug(f"Successfully unlocked and replaced '{pdf_path}'")
                return True
            else:
                logger.error(f"Failed to decrypt '{pdf_path}'. None of the provided passwords worked.")
                return False

        except FileNotFoundError:
            logger.error(f"Error: The file '{pdf_path}' was not found.")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred while processing '{pdf_path}': {e}")
            if os.path.exists(temp_output_pdf_path):
                os.remove(temp_output_pdf_path)
            return False

    def process_all_unlocked_files(self, passwords_source, passwords_key='passwords'):
        unlocked_files = []
        cannot_unlock_files = []

        logger.info(f"Attempting {len(self.file_paths_list)} files for Decryption Stage.")

        for pdf_path in self.file_paths_list:
            if self.unlock_single_pdf(pdf_path, passwords_source, passwords_key):
                unlocked_files.append(pdf_path)
            else:
                cannot_unlock_files.append(pdf_path)

        logger.debug(f"Completed {len(unlocked_files)} files of Decryption")
        return unlocked_files, cannot_unlock_files

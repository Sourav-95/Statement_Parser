import os
import re
import pandas as pd
import pdfplumber
import contextlib
import io
import logging
from itertools import chain
from typing import List
from src.components.logfactory import get_logger
import warnings
import requests
from constants import DataParserConstants
from datetime import datetime


# Suppress unnecessary logging output from external libraries
logging.getLogger("pdfminer").setLevel(logging.ERROR)
logging.getLogger("pdfplumber").setLevel(logging.ERROR)
warnings.filterwarnings("ignore")

# Initialize logger for the ingestion process
logger = get_logger(__name__)

class DataHandler:
    """
    DataHandler is responsible for extracting text and IFSC code from the input file,
    and interacting with an API to get the bank name based on the IFSC code.
    """

    def __init__(self, file_dir):
        """ Initializes the DataHandler object with the given file path """
        self.file_dir = file_dir

    def get_textdata_from_file(self) -> str:
        """
        Extracts the text data from the first page of a PDF file.

        Returns:
            str: Extracted text from the first page of the PDF.
        
        Raises:
            Exception: If text extraction fails.
        """
        try:
            with contextlib.redirect_stderr(io.StringIO()):
                with pdfplumber.open(self.file_dir) as pdf:
                    page = pdf.pages[0]
                    text_data = page.extract_text()

                    return text_data
        except Exception as e:
            raise Exception(f"Error extracting text from `{self.file_dir}`: {e}")

    def get_account_ifsc(self):
        """
        Extracts the IFSC code from the PDF by searching for patterns in the text.

        Returns:
            str: IFSC code if found, None otherwise.
        """
        pattern_ifsc = r'[A-Z]{4}0[A-Z0-9]{6}'
        statement_text = self.get_textdata_from_file()

        if statement_text:
            ifsc_code = re.findall(pattern_ifsc, statement_text)
            if ifsc_code:
                return ifsc_code[0]
            else:
                print(f"No IFSC code found in {self.file_dir}")
                return None
        else:
            print(f"Failed to extract text from {self.file_dir} OR No Data Found")
            return None
        
    def get_bank_name_from_ifsc(self):
        """
        Uses the extracted IFSC code to query the Razorpay API and get the bank name.

        Returns:
            str: Bank name corresponding to the IFSC code.

        Raises:
            Exception: If no IFSC code is found.
        """
        ifsc_code = self.get_account_ifsc()
        if not ifsc_code:
            raise Exception("No IFSC code found.")
            
        url = f"https://ifsc.razorpay.com/{ifsc_code}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise error for bad status
            response_data_rtrv = response.json()
            return response_data_rtrv.get('BANK')
        except requests.exceptions.HTTPError:
            print("Invalid IFSC code or API error.")
            return None
        except Exception as e:
            print("Error Occurred while getting Bank Name:", e)
            return None
            
class DataProcessor:
    """
    DataProcessor handles the extraction, cleaning, and transformation of data from a PDF into a usable format.
    """

    def __init__(self, file_dir):
        """
        Initializes the DataProcessor with the file directory.

        Args:
            file_dir (str): The path to the input file.
        """
        self.file_dir = file_dir
        self.combined_data = []

    def set_data_to_tabular(self, table_horizon=False):
        """
        Converts the table data from the PDF into a list of lists (tabular format).

        Args:
            table_horizon (bool): If True, processes the table in horizontal format.

        Returns:
            list: A list of rows representing the table data.
        """
        with contextlib.redirect_stderr(io.StringIO()):
            with pdfplumber.open(self.file_dir) as raw_data:
                try:
                    # Extracting the table from pages
                    for i in range(len(raw_data.pages)):
                        page = raw_data.pages[i]
                        if table_horizon:
                            table = page.extract_table(table_settings=DataParserConstants.TABLE_SETTING)
                        else:       
                            table = page.extract_table()
                        if table is not None:
                            self.combined_data = list(chain(self.combined_data, table))
                    logger.debug(f"Data extracted successfully from `{self.file_dir}`")
                except Exception as e:
                    raise Exception(f"Error extracting table from {self.file_dir}: {e}")
        return self.combined_data
    
    @staticmethod
    def header_cleaner(raw_data:List):
        """
        Removes unwanted header rows from the table.

        Args:
            raw_data (list): The raw table data.

        Returns:
            list: The cleaned table data without the header.
        """
        header_condition = DataParserConstants.HEADER_CLEANER_CONDITIONS
        filtered_list = []

        if not raw_data:
            logger.warning(f"No data found..Header cleaning cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"Header cleaning cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                filtered_list = [
                    items for items in raw_data
                    if not any(
                        isinstance(item, str) and any(cond.lower() in item.lower() for cond in header_condition)
                        for item in items
                    )
                ]
                logger.debug(f"Header cleaned successfully")
                return raw_data
            except Exception as e:
                logger.error(f"Error cleaning header: {e}")
                return raw_data

    @staticmethod
    def blank_row_remover(raw_data:List):
        """
        Removes rows that are completely blank from the table.

        Args:
            raw_data (list): The raw table data.

        Returns:
            list: The cleaned table data without blank rows.
        """
        if not raw_data:
            logger.warning(f"No data found..Blank row cleaning cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"Blank row cleaning cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                filtered_list = [row for row in raw_data if not all(elem == '' for elem in row)]
                logger.debug(f"Blank ['','','',.....] rows removed successfully")
                return filtered_list
            except Exception as e:
                logger.error(f"Error removing blank rows: {e}")
                return raw_data     

    @staticmethod
    def next_line_char_remover(raw_data:List):
        """
        Removes newline characters from the table.

        Args:
            raw_data (list): The raw table data.

        Returns:
            list: The cleaned table data without newline characters.
        """
        if not raw_data:
            logger.warning(f"No data found..Next line cleaning cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"Next line cleaning cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                cleaned_list = [
                    [item.replace('\n', ' ') if isinstance(item, str) else item for item in sublist]
                    for sublist in raw_data
                ]
                logger.debug(f"Next line character (/n) removed successfully")
                return cleaned_list
            except Exception as e:
                logger.error(f"Error removing next line: {e}")
                return raw_data
            
    @staticmethod
    def none_row_remover(raw_data:List):
        """
        Removes rows that contain 'None' from the table.

        Args:
            raw_data (list): The raw table data.

        Returns:
            list: The cleaned table data without None rows.
        """
        if not raw_data:
            logger.warning(f"No data found..None row cleaning cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"None row cleaning cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                filtered_list = [sublist for sublist in raw_data if None not in sublist]
                logger.debug(f"None [None,None,None,.....] rows removed successfully")
                return filtered_list
            except Exception as e:
                logger.error(f"Error removing None rows: {e}")
                return raw_data
             
    @staticmethod
    def footer_cleaner(raw_data:List):
        """
        Removes the footer from the table based on specific footer content.

        Args:
            raw_data (list): The raw table data.

        Returns:
            list: The cleaned table data without the footer.
        """
        if not raw_data:
            logger.warning(f"No data found..Footer cleaning cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"Footer cleaning cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                index_to_cut = None
                for i, sublist in enumerate(raw_data):
                    if any('STATEMENTSUMMARY' in str(item) for item in sublist):
                        index_to_cut = i
                        break

                # If found, slice the list to keep only elements before that index
                if index_to_cut is not None:
                    filtered_list = raw_data[:index_to_cut]
                else:
                    filtered_list = raw_data
                logger.debug(f"Footer cleaned successfully")
                return filtered_list
            except Exception as e:
                logger.error(f"Error cleaning footer: {e}")
                return raw_data

    def data_to_dataframe(self, raw_data: List, table_horizon: bool = False) -> pd.DataFrame:
        """
        Converts the cleaned data to a pandas DataFrame.

        Args:
            raw_data (list): The cleaned raw data.
            table_horizon (bool): If True, processes the table in horizontal format.

        Returns:
            DataFrame: The cleaned data in tabular format as a pandas DataFrame.
        """
        if not raw_data:
            logger.warning(f"No data found..Dataframe creation cannot be performed")
            return raw_data
        
        if not isinstance(raw_data, list):
            msg = f"Invalid data format: {type(raw_data)}. Expected a list...!!"
            msg2 = f"Dataframe creation cannot be performed"

            logger.error(msg + msg2)
            return raw_data
        else:
            try:
                header = raw_data[0]
                all_records = raw_data[1:]

                if table_horizon:
                    index = 0
                    while index < (len(all_records)-1):
                        if len(all_records[index][1]) > len(all_records[index+1][1]):
                            new_particular = all_records[index][1] + all_records[index+1][1]
                            all_records[index][1] = new_particular
                            index += 2
                        else:
                            index += 1

                    # Removing Empty String after Cleaning 2nd Column
                    all_records = [
                                    sub for sub in all_records
                                    if not (
                                        isinstance(sub, list) and
                                        any(keyword in " ".join(sub).lower() for keyword in ["from :", "to :", "statement", "of account"])
                                    )
                                ]

                    df = pd.DataFrame(all_records, columns=header)
                else:
                    logger.debug(f'Converting to DataFrame for `{table_horizon}` Table Horizon')
                    df = pd.DataFrame(all_records, columns=header)


                # Final Check to DataFrame
                if not df.empty:
                    # Filter rows where the first column has valid dd/mm/yyyy or dd/mm/yy format 
                    # Changing the first column to string and removing leading/trailing spaces
                    # Filter rows where the first column has valid date format
                    df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip()
                    mask = df.iloc[:, 0].apply(self.is_valid_date)
                    if mask.sum() == 0:
                        logger.warning(f"No valid dates found in column 0. Sample values: {df.iloc[:, 0].unique()[:5]}")
                    df = df[mask].reset_index(drop=True)


                logger.debug(f"Data converted to DataFrame = Records:`{df.shape[0]}` Columns:`{df.shape[1]}")
                return df
            except Exception as e:
                logger.error(f"Error converting to DataFrame: {e}")
                return pd.DataFrame()

    def is_valid_date(self, val):
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d/%m/%y", "%d-%m-%y"):
            try:
                datetime.strptime(val, fmt)
                return True
            except ValueError:
                continue
        return False


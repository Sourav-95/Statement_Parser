import re,os
import pandas as pd
from pathlib import Path
from typing import Union
from src.data_processor.data_extraction import DataHandler, DataProcessor
from src.components.logfactory import get_logger
from constants import DataParserConstants
from src.components.utils import get_file_nm_list

# Initialize logger
logger = get_logger(__name__)


class DataOrchestrator:
    """
    Orchestrates the process of reading, cleaning, and standardizing
    tabular data from bank statement files.

    Attributes:
        src_file_path (Union[str, Path]): Path to the source file or folder.
        df_raw (pd.DataFrame): Raw DataFrame generated from the file.
        data_field_patterns (dict): Patterns to match column roles.
    """

    def __init__(self, src_file_path: Union[str, Path]):
        """
        Initializes the DataOrchestrator with the file path and loads default column patterns.
        """
        self.src_file_path = src_file_path
        self.df_raw = pd.DataFrame()
        self.data_field_patterns = DataParserConstants.COLUMN_PATTERN

    def set_table_horizontal(self) -> bool:
        """
        Determines whether the input data is in horizontal table format,
        based on the bank name (inferred from IFSC code).

        Returns:
            bool: True if table is horizontal, False otherwise.
        """
        data_handle_obj = DataHandler(file_dir=self.src_file_path)
        bank_name = data_handle_obj.get_bank_name_from_ifsc()

        logger.info(f"Bank Name for the File: {bank_name}")

        # Bank-to-table orientation mapping
        bank_table_mapping = {
            'Axis Bank': False,  # Axis Bank uses a non-horizontal table format
            'SBI': True,         # SBI uses horizontal table format
            'HDFC': True,        # HDFC uses horizontal table format
            # Add more banks as needed
        }

        
        # Default to horizontal (True) if the bank isn't in the mapping
        return bank_name, bank_table_mapping.get(bank_name, True)


    def normalize(self, text: str) -> str:
        """
        Normalizes a string for comparison by removing non-alphanumeric characters
        and converting to lowercase.

        Args:
            text (str): Input string.

        Returns:
            str: Normalized string.
        """
        return re.sub(r'[^a-z0-9]', '', text.lower())

    def set_matched_columns(self, columns) -> dict:
        """
        Matches actual DataFrame column names to standard column roles
        defined in constants using keyword-based matching.

        Args:
            columns (Iterable): Column names from the input DataFrame.

        Returns:
            dict: Mapping of original column names to standardized roles.
        """
        identified = {}
        normalized_cols = {self.normalize(col): col for col in columns}

        for role, keywords in self.data_field_patterns.items():
            for norm_col, orig_col in normalized_cols.items():
                for kw in keywords:
                    norm_kw = self.normalize(kw)
                    if norm_kw in norm_col:
                        identified[orig_col] = role
                        break
                if role in identified.values():
                    break  # Stop searching once role is matched
        return identified

    def set_standard_data_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters and renames columns in the input DataFrame based on matched roles.

        Args:
            df (pd.DataFrame): Input DataFrame with raw column names.

        Returns:
            pd.DataFrame: DataFrame with standardized column names.
        """
        try:
            matched_cols = self.set_matched_columns(df.columns)
            logger.debug(f'Matched columns are: {matched_cols}')

            if not matched_cols:
                logger.warning("No matching columns found.")
                return df

            logger.debug(f'Original Columns are: {df.columns}')
            filtered_df = df[list(matched_cols.keys())]  # Keep only matched columns
            filtered_df = filtered_df.rename(columns=matched_cols)  # Rename to standard names
            return filtered_df

        except Exception as e:
            logger.error(f'Standard Column Mapping Error: {e}')
            return df  # Return original in case of failure

    def get_ingestion_pipeline(self) -> pd.DataFrame:
        """
        Main pipeline to process the source file:
        1. Converts raw file content to tabular data
        2. Cleans the data (headers, blanks, footers, etc.)
        3. Converts cleaned list of lists into a DataFrame
        4. Standardizes column names based on matching patterns

        Returns:
            pd.DataFrame: Final cleaned and standardized DataFrame.
        """
        file_nm = os.path.basename(self.src_file_path)
        logger.info(f'Data Extraction for file : |`{file_nm}`|.......')
        logger.debug(f'Temp File Dir: {self.src_file_path}')
        data_processor_obj = DataProcessor(file_dir=self.src_file_path)

        # Step 1: Determine table orientation (horizontal or not)
        bank, horizontal = self.set_table_horizontal()
        logger.debug(f'Table Horizon:{horizontal}')
        logger.debug(f'Data Cleaning for file: |`{file_nm}`|..........')

        try:
            
            # Step 2: Extract raw tabular data
            all_data = data_processor_obj.set_data_to_tabular(table_horizon=horizontal)
            logger.info(f'Total Records of extracted Data: {len(all_data)}')

            # Step 3: Apply multiple cleaning steps
            cleaned_data = data_processor_obj.header_cleaner(all_data)
            logger.debug(f'Total Records after `Header` removed: {len(cleaned_data)}')

            cleaned_data = data_processor_obj.blank_row_remover(cleaned_data)
            logger.debug(f'Total record after `Blank Row` removed: {len(cleaned_data)}')

            cleaned_data = data_processor_obj.next_line_char_remover(cleaned_data)
            logger.debug(f'Records after `Next Line character` removed: {len(cleaned_data)}')

            cleaned_data = data_processor_obj.none_row_remover(cleaned_data)
            logger.debug(f'Total records after `None` removed: {len(cleaned_data)}')

            cleaned_data = data_processor_obj.footer_cleaner(cleaned_data)
            logger.debug(f'Total records after `Footer` removed: {len(cleaned_data)}')

            # Step 4: Convert cleaned data into a DataFrame
            self.df_raw = data_processor_obj.data_to_dataframe(
                cleaned_data, table_horizon=horizontal
            )
            logger.info(f'Total Records after cleaning: `{len(self.df_raw)}`')

            # Step 5: Standardize columns if DataFrame is not empty
            if not self.df_raw.empty:
                try:
                    self.df_raw = self.set_standard_data_fields(df=self.df_raw)
                    self.df_raw['Bank']=bank
                    logger.debug(f'Data Field Standardized for file : |`{file_nm}`|')
                except Exception as e:
                    logger.error(f"Error while standardizing columns: {e}")
                    raise Exception

            return self.df_raw

        except Exception as e:
            logger.warning(f"Unexpected error in `get_ingestion_pipeline()` for file {self.src_file_path}: {str(e)}")

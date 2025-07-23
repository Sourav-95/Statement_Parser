import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import inspect, text
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from src.components.logfactory import get_logger
import logging
from constants import DBConstants

logger = get_logger(__name__)

class DB_DeltaHandler:
    def __init__(self, db_name: str, metadata_table: str):
        self.engine: Engine = create_engine(f"sqlite:///{db_name}")
        self.metadata_table = metadata_table
        self._ensure_metadata_table_exists()

    def _ensure_metadata_table_exists(self):
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.metadata_table} (
            bank_name TEXT PRIMARY KEY,
            last_loaded_date DATE
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def _table_exists(self, table_name: str) -> bool:
        try:
            inspector = inspect(self.engine)
            logger.debug(f'Inspected Table:`{table_name}` ')
            return inspector.has_table(table_name)
        except Exception as e:
            logger.error(str(e))
    
    def _validate_table_schema(self, table_name: str, expected_columns: set):
        inspector = inspect(self.engine)
        actual_columns = {col["name"] for col in inspector.get_columns(table_name)}

        missing = expected_columns - actual_columns
        extra = actual_columns - expected_columns

        if missing:
            logger.error(f"Missing columns: {missing}")
            raise ValueError(f"Table '{table_name}' is missing columns: {missing}")

        if extra:
            logger.warning(f"Extra columns in table '{table_name}': {extra}")
            raise ValueError(f"Table '{table_name}' has unexpected columns: {extra}")

        if missing:
            raise ValueError(
                f"Table '{table_name}' exists but is missing columns: {missing}"
            )

    def _ensure_target_table_exists(self, target_table: str, expected_columns: set):
        try:
            if self._table_exists(target_table):
                self._validate_table_schema(target_table, expected_columns)
                logger.debug(f"Table '{target_table}' exists and schema is valid.")
            else:
                logger.debug(f"Table '{target_table}' not found. Creating it...")
                # Create table script - adjust columns as needed
                create_sql = DBConstants.CREATE_SQL_TEMPLATE.format(table = target_table)
                logger.debug(f'Checking CREATE SQL : {create_sql}')

                with self.engine.begin() as conn:
                    conn.execute(text(create_sql))

                logger.debug(f"Table '{target_table}' created.")

        except Exception as e:
            logger.error(str(e))

    def get_last_loaded_date(self, bank_name: str) -> datetime:
        query = f"""
        SELECT last_loaded_date
        FROM {self.metadata_table}
        WHERE bank_name = :bank_name;
        """
        try:
            result = pd.read_sql(query, self.engine, params={"bank_name": bank_name})
            logger.debug(f'Last loaded date fetched from METADATA_T')
            return result["last_loaded_date"].iloc[0] if not result.empty else None
        except Exception as e:
            logger.error(str(e))
        

    def update_last_loaded_date(self, bank_name: str, last_load_date: datetime):
        try:
            if isinstance(last_load_date, pd.Timestamp):
                last_load_date = last_load_date.date()  # or strftime("%Y-%m-%d")

            query = text("""
                INSERT INTO {metadata_table} (bank_name, last_loaded_date)
                VALUES (:bank_name, :last_date)
                ON CONFLICT(bank_name) DO UPDATE SET last_loaded_date = excluded.last_loaded_date;
            """.format(metadata_table=self.metadata_table))

            with self.engine.begin() as conn:
                conn.execute(query, {"bank_name": bank_name, "last_date": last_load_date})
            logger.debug(f'`{self.metadata_table}` updated with last load date : `{last_load_date}`')
        except Exception as e:
            logger.error(str(e))

    def __reset_datatype__(self, df: pd.DataFrame):
        '''Convert columns to appropriate datatypes with cleaning'''

        # Resetting the Date column to datetime format
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], dayfirst=True, errors="coerce").dt.date
        else:
            logger.error("Column `Date` not found in DataFrame. Skipping datetime conversion.")

        # List of numeric columns to convert
        numeric_cols = ["Debit", "Credit", "Balance"]

        for col in numeric_cols:
            if col in df.columns:
                # Clean unwanted characters like commas, spaces, and non-breaking spaces
                df[col] = df[col].astype(str).str.replace(r"[^\d\.-]", "", regex=True)

                # Convert cleaned strings to numeric
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                logger.error(f"Column `{col}` not found in DataFrame. Skipping type reset for this column.")

    def load_delta(self, parsed_df: pd.DataFrame, expected_columns:set, bank_name: str, target_table: str):
        if "Date" not in parsed_df.columns:
            raise ValueError("DataFrame must contain `Date` column.")

        # Ensure DataFrame recieved has only records from `expected_column`
        parsed_df = parsed_df[list(expected_columns)]
        
        # Resetting the DataFrame datatypes
        self.__reset_datatype__(parsed_df)
        # parsed_df["Date"] = pd.to_datetime(parsed_df["Date"], dayfirst=True, errors="coerce")

        # Step 1: Ensure target table exists
        self._ensure_target_table_exists(target_table, expected_columns)

        # Step 2: Filter based on metadata
        logger.debug(f'Getting Last Load Date for `{bank_name}`')
        last_date = self.get_last_loaded_date(bank_name)
        logger.info(f'Data Type of Last Date: {last_date}')

        if last_date is None or pd.isna(last_date):
            logger.warning("Last date is None or NaT — skipping delta load.")
        else:
            last_date = pd.Timestamp(last_date)
            if isinstance(last_date, pd.Timestamp):
                last_date = last_date.date()

            parsed_df = parsed_df[parsed_df["Date"] >= last_date]

            # Delete existing records for last date
            logger.info(f'Deleting the existing records for last date')
            with self.engine.begin() as conn:
                delete_stmt = text(f"DELETE FROM {target_table} WHERE Date = :last_date AND Bank = :bank_name")
                conn.execute(delete_stmt, {"last_date": last_date, "bank_name": bank_name})

        if parsed_df.empty:
            print(f"No new records to insert for {bank_name}.")
            return 1

        # Step 3: Insert into SQL
        try:
            parsed_df.to_sql(target_table, self.engine, if_exists="append", index=False)
            new_last_date = parsed_df["Date"].max()

            # Updating the Last Date of Inserted data to `MetaData Table`
            self.update_last_loaded_date(bank_name, new_last_date)
            logger.info(f"Inserted {len(parsed_df)} new records for `{bank_name}` up to {new_last_date}.")
            return 1
        except IntegrityError as e:
            logger.error(f"IntegrityError: {e.orig}")
            logger.error("Some records may already exist or violate schema constraints.")
            return 0

    def load_delta_gsheet(self, parsed_df: pd.DataFrame, expected_columns: set, bank_name: str, sheet_id: str, gsheet_client: gspread.Client):
        if "Date" not in parsed_df.columns:
            raise ValueError("DataFrame must contain `Date` column.")

        # Keep only expected columns
        parsed_df = parsed_df[list(expected_columns)]

        # Clean and normalize datatypes
        self.__reset_datatype__(parsed_df)

        # Step 1: Filter using last load date for that bank
        logger.debug(f"Getting Last Load Date for `{bank_name}`")
        last_date = self.get_last_loaded_date(bank_name)

        if last_date:
            parsed_df = parsed_df[parsed_df["Date"] > last_date]  # avoid duplicates

        if parsed_df.empty:
            logger.info(f"No new records to insert for `{bank_name}`.")
            return

        try:
            # Step 2: Connect to Google Sheet
            sheet = gsheet_client.open_by_key(sheet_id)
            worksheet = sheet.get_worksheet(0)  # always using first sheet

            # Read existing data (if any)
            existing_data = worksheet.get_all_values()
            has_header = bool(existing_data)

            # Step 3: Convert date columns to ISO string
            parsed_df = parsed_df.applymap(
                lambda x: x.isoformat() if isinstance(x, (datetime.date, pd.Timestamp)) else x
            )

            # Step 4: Insert or append
            if not has_header:
                logger.info(f"Sheet is empty. Writing with headers for `{bank_name}`.")
                set_with_dataframe(worksheet, parsed_df, include_column_header=True, resize=False)
            else:
                existing_rows = len(existing_data)
                start_row = existing_rows + 1  # next empty row
                logger.info(f"Appending data starting from row {start_row} for `{bank_name}`.")
                set_with_dataframe(worksheet, parsed_df, row=start_row, include_column_header=False, resize=False)

            # Step 5: Update metadata
            new_last_date = parsed_df["Date"].max()
            self.update_last_loaded_date(bank_name, new_last_date)
            logger.info(f"Inserted {len(parsed_df)} new records to Google Sheet for `{bank_name}` up to {new_last_date}.")

        except Exception as e:
            logger.error(f"Failed to insert data into Google Sheet: {str(e)}")

import os
from pathlib import Path


class ConstantRetriever:
    
    SCRIPT_BASE = os.path.dirname(os.path.abspath(__file__))
    TEMP_DOWNLOAD_DIR = 'temp'

class GoogleAuthConstants:

    GDRIVE_SCOPES = 'https://www.googleapis.com/auth/drive'
    GDRIVE_SERVICE_NAME = 'drive'
    GDRIVE_VERSION = 'v3'

    GMAIL_SCOPES = 'https://www.googleapis.com/auth/gmail.readonly'
    GMAIL_SERVICE_NAME = 'gmail'
    GMAIL_VERSION = 'v1'

    GSHEET_SCOPES = 'https://www.googleapis.com/auth/spreadsheets'

    GDRIVE_DEL_SCOPE = 'https://www.googleapis.com/auth/drive'

class DataParserConstants:

    # Constants for DataParser
    TABLE_SETTING = {"vertical_strategy": "lines",
                                  "horizontal_strategy": "text",
                                  "intersection_tolerance": 15,
                                  "join_tolerance": 20,
                                  }
    
    HEADER_CLEANER_CONDITIONS = ['OPENING BALANCE', 'transaction total', 'closing balance', 'From']

    # Stopwords
    REMOVED_ENGLISH_WORDS = {
        'sour', 'mum', 'sourav', 'mir', 'septexp', 'pm', 'pa', 'pos', 'remitter'
    }

    DISCARD_STOPWORD = ['other', 'others']

    # Add the list of column patterns dictionary if came accross new account statement
    COLUMN_PATTERN = {
        'Date':        ['date', 'trans date'],
        'Particulars':   ['narration', 'particulars'],
        'Debit':       ['debit', 'withdrawal amt', 'dr'],
        'Credit':      ['credit', 'deposit amt', 'cr'],
        'Balance':     ['balance', 'closing balance']
    }

    BANKING_KEYWORD_URL = Path("./inputs/banking_keywords.txt")
    # '/Users/souravm/Documents/Projects/statement_parser/src/components/banking_keywords.txt'

# Set Database Inputs as Constant
class DBConstants:
    
    # DB_PATH = Path("database/bank_transactions.db")
    DB_PATH = Path("G:/DATABASE/bank_transactions.db")

    TRANSACTION_TABLE = "TRANSACTION_T"
    LOAD_STATUS_TABLE = "LOAD_STATS_METADATA"

    CREATE_SQL_TEMPLATE = """
        CREATE TABLE {table} (
            Date DATE NOT NULL,
            Particulars TEXT NOT NULL,
            Debit NUMERIC,
            Credit NUMERIC,
            Balance NUMERIC,
            Bank TEXT,
            Subcategory TEXT,
            Category TEXT 
        );
        """
    
    TRANSACTION_T_COLS = {"Date", "Particulars", "Credit", "Debit", "Balance", "Bank", "Subcategory", "Category"}
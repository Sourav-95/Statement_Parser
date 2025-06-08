from sqlalchemy import create_engine, text
from src.components.logfactory import get_logger
import os

logger = get_logger(__name__)

logger.info(f'Creating DB Engine')
logger.info(f'{os.getcwd()}')
database_dir = os.path.join(os.getcwd(), 'database')

engine = create_engine('sqlite:///database_dir, transaction.db')
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS DEBIT_T (
            date DATETIME,
            particulats TEXT,
            debit INTEGER,
            balance INTEGER,
            category TEXT
        )
    """))

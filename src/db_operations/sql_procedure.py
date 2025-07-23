import sqlite3
from constants import DBConstants
from src.components.logfactory import get_logger
logger = get_logger(__name__)

class SQL_Procedure:

    @classmethod
    def update_rows(cls, db_path, table_name, set_column, condition_column, updates: list):
        """
        Reusable update method.

        Parameters:
            db_path (str): Path to the SQLite database file.
            table_name (str): Name of the table to update.
            set_column (str): Column to update (e.g., 'Category').
            condition_column (str): Column to match condition (e.g., 'Particulars').
            updates (list of tuples): List of (new_value, condition_value) pairs.
        """

        if not isinstance(updates, list) or not updates:
            return 0  # Invalid input

        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Dynamically build the query with safe table/column names
                query = f"""
                UPDATE {table_name}
                SET {set_column} = ?
                WHERE {condition_column} LIKE ?
                """

                cursor.executemany(query, updates)
                conn.commit()
                return 1  # Success

        except Exception as e:
            print(f"[ERROR] Failed to update records: {e}")
            return -1  # Failure
        
    @classmethod
    def trigger_sql_procedure():
        try:
            updates = [
                        ('OFFICE FOOD', '%HUNGERBOX%'),
                        ('OFFICE FOOD', '%Daalchini%'), 
                        ('FURLENCO', '%Furlenco%'),
                        ('SWIGGY', '%swiggy%'),
                        ('GROCERY', '%grocery%'),
                        ('NEWSPAPER', '%newspa%'),
                        ('YULU', '%yulu%'),
                        ('BIKE', '%MOTOR%')
                    ]
            
            updates2 = [
                        ('Bills', '%Furlenco%'),
                        ('Restaurant', '%swiggy%'),
                        ('Grocery', '%grocery%'),
                        ('Bills', '%newspa%'),
                        ('Transport', '%yulu%')
                    ]
            
            investment = [
                ('Investment', 'RD'),
            ]

            # Update Row 1
            SQL_Procedure.update_rows(
                db_path=DBConstants.DB_PATH,
                table_name='TRANSACTION_T',
                set_column='Subcategory',
                condition_column='Particulars',
                updates=updates
            )

            # Update 2 
            SQL_Procedure.update_rows(
                db_path=DBConstants.DB_PATH,
                table_name='TRANSACTION_T',
                set_column='Category',
                condition_column='Particulars',
                updates=updates2
            )

            # Investment Update RD 
            SQL_Procedure.update_rows(
                db_path=DBConstants.DB_PATH,
                table_name='TRANSACTION_T',
                set_column='Category',
                condition_column='Subcategory',
                updates=investment
            )

            logger.info(f'Updated Rows `SQL Stored Procedure`')

        except Exception as e:
            logger.error(f'Error Occured as : {e}')   

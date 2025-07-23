import logging
from src.components.logfactory import get_logger, set_global_log_level
logger = get_logger(__name__)


from src.db_operations.sql_procedure import SQL_Procedure
from constants import DBConstants


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
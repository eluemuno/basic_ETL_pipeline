import sqlite3
import pandas as pds


def create_db_table(db_name, create_tb_statement):
    try:
        db_cxn = sqlite3.connect(db_name)
        cursor = db_cxn.cursor()
        cursor.execute(create_tb_statement)
    except Exception as e:
        return 'Error:', e
    else:
        return 'Table created'


def run_sql_query(dbname, query):
    try:
        conn = sqlite3.connect(dbname)
        cursor = conn.cursor()
        result = pds.read_sql(query, conn)
        df = pds.DataFrame(result)
    except Exception as e:
        return 'Error:', e
    else:
        return df

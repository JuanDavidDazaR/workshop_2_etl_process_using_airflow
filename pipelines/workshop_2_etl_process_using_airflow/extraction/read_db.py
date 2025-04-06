"""Module to read the Grammys from database.
This module connects to the database and retrieves all records
from the 'grammyAwards' table."""

import os
import sys
import pandas as pd
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)
from db.db_conection import connect_db


def extract_grammy_database():
    """
    Extracts Grammy Awards data from the database.

    Returns:
        pd.DataFrame: A DataFrame containing the Grammy Awards data.
        None: If an error occurs during extraction.
    """

    engine = connect_db()

    try:
        df_grammy = pd.read_sql_query('SELECT * FROM "grammyAwards"', engine)
        return df_grammy
    except pd.io.sql.DatabaseError as e:
        print(f"Database error: {e}")
        return None
    except ConnectionError as e:
        print(f"Connection error: {e}")
        return None

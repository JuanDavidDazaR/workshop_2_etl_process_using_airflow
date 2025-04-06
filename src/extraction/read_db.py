"""Extracts Grammy Awards data from the database."""

import os
import sys
import logging
import pandas as pd

from src.db.db_conection import connect_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)


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
        logger.error("Database error: %s", e)
    except ConnectionError as e:
        logger.error("Connection error: %s", e)
    return None

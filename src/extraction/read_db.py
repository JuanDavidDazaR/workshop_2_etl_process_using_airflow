"""Extracts Grammy Awards data from the database and saves it to a temporary CSV file."""

import os
import sys
import logging
import tempfile
import pandas as pd

from src.db.db_conection import connect_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

def extract_grammy_database():
    engine = connect_db()
    try:
        df_grammy = pd.read_sql_query('SELECT * FROM "grammyAwards"', engine)
        logger.info("Datos de Grammy extraídos exitosamente de la base de datos.")
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            df_grammy.to_csv(tmp_file.name, index=False)
            tmp_file_path = tmp_file.name
        logger.info(f"DataFrame guardado en archivo temporal: {tmp_file_path}")
        return tmp_file_path
    except pd.io.sql.DatabaseError as e:
        logger.error("Database error: %s", e)
        return None
    except ConnectionError as e:
        logger.error("Connection error: %s", e)
        return None

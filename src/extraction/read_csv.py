"""Module to read the Spotify dataset from a CSV file."""

import os
import logging
import tempfile
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def read_csv_spotify():
    """
    Reads the Spotify dataset from a CSV file
    and saves it to a temporary CSV file.

    Returns:
        str: Path to the temporary file where the DataFrame is saved as CSV.
    Raises:
        FileNotFoundError: If the CSV file cannot be found.
        pd.errors.EmptyDataError: If the CSV file is empty.
        pd.errors.ParserError: If the CSV file cannot be parsed.
    """
    try:
        BASE_DIR = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../..")
        )
        CSV_PATH = os.path.join(
            BASE_DIR, "data", "0_raw", "spotify_dataset.csv"
        )
        logger.info(f"Intentando leer el archivo desde: {CSV_PATH}")
        df_spotify = pd.read_csv(CSV_PATH)  # Leer como DataFrame en memoria
        logger.info("Archivo CSV leído exitosamente.")

        # Guardar el DataFrame en un archivo temporal CSV
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            df_spotify.to_csv(tmp_file.name, index=False)  # Guardar como CSV
            tmp_file_path = tmp_file.name
        logger.info(f"DataFrame guardado en archivo temporal: {tmp_file_path}")
        return tmp_file_path

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}. Please check the file path: {CSV_PATH}")
        raise
    except pd.errors.EmptyDataError:
        logger.error("The file is empty.")
        raise
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Check its format.")
        raise
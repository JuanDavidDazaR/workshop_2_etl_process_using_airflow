""" Module to read the Spotify dataset from a CSV file. """
import logging
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
    Reads the Spotify dataset from a CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the dataset.
        None: If the file cannot be read due to an error.
    """
    try:
        df_spotify = pd.read_csv("../../../data/0_raw/spotify_dataset.csv")
        return df_spotify
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
    except pd.errors.EmptyDataError:
        logger.error("The file is empty.")
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Check its format.")
    return None

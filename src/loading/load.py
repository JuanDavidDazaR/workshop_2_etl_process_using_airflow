"""Module to load the merged Spotify-Grammy dataset into a SQL database."""

import logging
import pandas as pd
import os
from sqlalchemy.exc import SQLAlchemyError
from src.db.db_conection import connect_db  # Reutilizamos la conexión existente

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def load_to_db(ti):
    """ 
    Load the merged Spotify-Grammy dataset into a SQL database
    and save a copy for EDA.
    
    Args:
        ti: Task instance to pull the file path from XCom.
    Returns:
        str: Path to the saved file for EDA
    """
    merged_file_path = ti.xcom_pull(task_ids='merge_spotify_grammy')
    if not merged_file_path:
        raise ValueError("No file path received from merge_spotify_grammy task")
    
    # Read the combined CSV file
    logger.info(f"Reading combined data from: {merged_file_path}")
    merged_df = pd.read_csv(merged_file_path)
    
    # Save a copy for EDA in the project directory
    eda_file_path = os.path.expanduser("~/workshop_2_etl_process_using_airflow/data/2_final/spotify_grammy_merged.csv")
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(eda_file_path), exist_ok=True)
    
    # Save the file for EDA
    merged_df.to_csv(eda_file_path, index=False)
    logger.info(f"Data saved for EDA at: {eda_file_path}")
    
    # Connect to the database
    engine = connect_db()
    if engine is None:
        raise ConnectionError("Could not connect to the database")
    
    try:
        # Save the DataFrame to the database
        table_name = "spotify_grammy_merged"
        logger.info(f"Saving data to table '{table_name}'...")
        merged_df.to_sql(
            table_name, 
            engine, 
            if_exists='replace',  # Replace the table if it already exists
            index=False
        )
        logger.info(f"Data successfully saved to table '{table_name}' with {len(merged_df)} rows")
    except SQLAlchemyError as e:
        logger.error(f"Error saving data to the database: {e}")
        raise
    finally:
        # Delete the temporary file only if it's different from our EDA file
        if merged_file_path != eda_file_path:
            try:
                os.remove(merged_file_path)
                logger.info(f"Temporary file deleted: {merged_file_path}")
            except Exception as e:
                logger.warning(f"Could not delete temporary file {merged_file_path}: {e}")
    
    return eda_file_path
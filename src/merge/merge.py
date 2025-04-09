# src/merge.py
"""Module to merge Spotify and Grammy Awards datasets."""

import logging
import pandas as pd
import os
import tempfile

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def merge_spotify_grammy(ti):
    """
    Merge transformed Spotify and Grammy datasets.

    Args:
        ti: Task instance to pull file paths from XCom.

    Returns:
        str: Path to the temporary file where the merged DataFrame is saved.
    """
    spotify_file_path = ti.xcom_pull(task_ids='transform_spotify')
    grammy_file_path = ti.xcom_pull(task_ids='transform_grammy')

    if not spotify_file_path or not grammy_file_path:
        raise ValueError("Missing file paths from transform tasks")

    # Leer los DataFrames transformados
    logger.info(f"Leyendo Spotify desde: {spotify_file_path}")
    spotify_df = pd.read_parquet(spotify_file_path)
    logger.info(f"Leyendo Grammy desde: {grammy_file_path}")
    grammy_df = pd.read_parquet(grammy_file_path)

    # Limpiar Spotify para el merge
    spotify_df['track_name'] = spotify_df['track_name'].str.lower().str.strip()
    spotify_df['artists'] = spotify_df['artists'].str.lower().str.strip()

    # Realizar el merge
    merged_df = pd.merge(
        spotify_df,
        grammy_df,
        how='left',
        left_on=['artists'],
        right_on=['artist']
    )

    # Eliminar columnas redundantes
    merged_df = merged_df.drop(columns=['artist'], errors='ignore')

    # Rellenar valores NaN en 'winner'
    merged_df['winner'] = merged_df['winner'].fillna(False)

    # Guardar el resultado en un archivo temporal
    with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as tmp_file:
        merged_df.to_parquet(tmp_file.name, index=False)
        merged_file_path = tmp_file.name
    logger.info(f"DataFrame combinado guardado en: {merged_file_path} con {len(merged_df)} filas")

    # Limpiar archivos temporales
    for file_path in [spotify_file_path, grammy_file_path]:
        try:
            os.remove(file_path)
            logger.info(f"Archivo temporal eliminado: {file_path}")
        except Exception as e:
            logger.warning(f"No se pudo eliminar el archivo temporal {file_path}: {e}")

    return merged_file_path
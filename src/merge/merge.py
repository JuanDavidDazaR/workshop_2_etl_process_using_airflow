# src/merge/merge.py
"""Module to merge Spotify, Grammy Awards, and MusicBrainz datasets."""

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


def merge_spotify_grammy_musicbrainz(ti):
    """
    Merge transformed Spotify, Grammy, and MusicBrainz datasets.

    Args:
        ti: Task instance to pull file paths from XCom.

    Returns:
        str: Path to the temporary file where the merged DataFrame is saved (CSV).
    """
    # Obtener las rutas de los archivos desde XCom
    spotify_file_path = ti.xcom_pull(task_ids='transform_spotify')
    grammy_file_path = ti.xcom_pull(task_ids='transform_grammy')
    musicbrainz_file_path = ti.xcom_pull(task_ids='transform_api')  # Cambiado a 'transform_api'

    # Verificar que todos los archivos estén presentes
    if not all([spotify_file_path, grammy_file_path, musicbrainz_file_path]):
        missing = [f for f, p in [("Spotify", spotify_file_path), ("Grammy", grammy_file_path), ("MusicBrainz", musicbrainz_file_path)] if not p]
        raise ValueError(f"Faltan rutas de archivo para: {', '.join(missing)}")

    # Leer los DataFrames transformados (como CSV)
    logger.info(f"Leyendo Spotify desde: {spotify_file_path}")
    spotify_df = pd.read_csv(spotify_file_path)
    logger.info(f"Leyendo Grammy desde: {grammy_file_path}")
    grammy_df = pd.read_csv(grammy_file_path)
    logger.info(f"Leyendo MusicBrainz desde: {musicbrainz_file_path}")
    musicbrainz_df = pd.read_csv(musicbrainz_file_path)

    # Limpiar columnas de artistas para el merge
    spotify_df['artists'] = spotify_df['artists'].str.lower().str.strip()
    grammy_df['artist'] = grammy_df['artist'].str.lower().str.strip()
    musicbrainz_df['name'] = musicbrainz_df['name'].str.lower().str.strip()

    # Primer merge: Spotify con Grammy
    merged_df = pd.merge(
        spotify_df,
        grammy_df,
        how='left',
        left_on=['artists'],
        right_on=['artist']
    )
    # Eliminar columna redundante 'artist' de Grammy
    merged_df = merged_df.drop(columns=['artist'], errors='ignore')

    # Segundo merge: Resultado anterior con MusicBrainz
    final_merged_df = pd.merge(
        merged_df,
        musicbrainz_df,
        how='left',
        left_on=['artists'],
        right_on=['name']
    )
    # Eliminar columna redundante 'name' de MusicBrainz
    final_merged_df = final_merged_df.drop(columns=['name'], errors='ignore')

    # Rellenar valores NaN en 'winner' (de Grammy)
    final_merged_df['winner'] = final_merged_df['winner'].fillna(False)

    # Guardar el resultado en un archivo temporal (como CSV)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        final_merged_df.to_csv(tmp_file.name, index=False)
        merged_file_path = tmp_file.name
    logger.info(f"DataFrame combinado guardado en: {merged_file_path} con {len(final_merged_df)} filas")

    # Limpiar archivos temporales
    for file_path in [spotify_file_path, grammy_file_path, musicbrainz_file_path]:
        try:
            os.remove(file_path)
            logger.info(f"Archivo temporal eliminado: {file_path}")
        except Exception as e:
            logger.warning(f"No se pudo eliminar el archivo temporal {file_path}: {e}")

    return merged_file_path
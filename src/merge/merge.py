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
    Performs two merges with Grammy: first by 'artist', then by 'nominee'.
    Drops columns with 85% or more null values.

    Args:
        ti: Task instance to pull file paths from XCom.

    Returns:
        str: Path to the temporary file where the merged DataFrame is saved (CSV).
    """
    # Obtener las rutas de los archivos desde XCom
    spotify_file_path = ti.xcom_pull(task_ids='transform_spotify')
    grammy_file_path = ti.xcom_pull(task_ids='transform_grammy')
    musicbrainz_file_path = ti.xcom_pull(task_ids='transform_api')

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

    # Limpiar columnas para el merge
    spotify_df['artists'] = spotify_df['artists'].str.lower().str.strip()
    grammy_df['artist'] = grammy_df['artist'].str.lower().str.strip()
    grammy_df['nominee'] = grammy_df['nominee'].str.lower().str.strip()
    musicbrainz_df['name'] = musicbrainz_df['name'].str.lower().str.strip()

    # Primer merge: Spotify con Grammy por 'artists' y 'artist'
    merge_by_artist = pd.merge(
        spotify_df,
        grammy_df,
        how='left',
        left_on=['artists'],
        right_on=['artist'],
        suffixes=('', '_artist')
    )
    logger.info(f"Primer merge (por artist): {len(merge_by_artist)} filas")

    # Eliminar columna redundante 'artist' de Grammy
    merge_by_artist = merge_by_artist.drop(columns=['artist'], errors='ignore')

    # Segundo merge: Resultado anterior con Grammy por 'artists' y 'nominee'
    merge_by_nominee = pd.merge(
        merge_by_artist,
        grammy_df,
        how='left',
        left_on=['artists'],
        right_on=['nominee'],
        suffixes=('', '_nominee')
    )
    logger.info(f"Segundo merge (por nominee): {len(merge_by_nominee)} filas")

    # Eliminar columna redundante 'nominee' de Grammy
    merge_by_nominee = merge_by_nominee.drop(columns=['nominee'], errors='ignore')

    # Combinar columnas duplicadas, asegurando que 'nominated' se preserve
    for col in grammy_df.columns:
        if col in ['artist', 'nominee']:
            continue
        col_nominee = f"{col}_nominee"
        if col_nominee in merge_by_nominee.columns:
            if col not in merge_by_nominee.columns:
                merge_by_nominee[col] = merge_by_nominee[col_nominee]  # Crear si no existe
            else:
                merge_by_nominee[col] = merge_by_nominee[col].fillna(merge_by_nominee[col_nominee])
            merge_by_nominee = merge_by_nominee.drop(columns=[col_nominee])

    # Tercer merge: Resultado con MusicBrainz por 'artists' y 'name'
    final_merged_df = pd.merge(
        merge_by_nominee,
        musicbrainz_df,
        how='left',
        left_on=['artists'],
        right_on=['name']
    )
    logger.info(f"Tercer merge (con MusicBrainz): {len(final_merged_df)} filas")

    # Eliminar columna redundante 'name' de MusicBrainz
    final_merged_df = final_merged_df.drop(columns=['name'], errors='ignore')

    # Rellenar valores NaN en 'winner' y 'nominated', creando 'nominated' si no existe
    if 'winner' not in final_merged_df.columns:
        final_merged_df['winner'] = False
    else:
        final_merged_df['winner'] = final_merged_df['winner'].fillna(False)

    # Eliminar columnas con 85% o más de valores nulos, excluyendo 'country' y 'type'
    threshold = 0.85
    total_rows = len(final_merged_df)
    null_counts = final_merged_df.isnull().sum()
    columns_to_drop = [col for col in final_merged_df.columns 
                       if null_counts[col] / total_rows >= threshold 
                       and col not in ['country', 'type']]
    
    if columns_to_drop:
        logger.info(f"Eliminando columnas con 85% o más de valores nulos: {columns_to_drop}")
        final_merged_df = final_merged_df.drop(columns=columns_to_drop)

    # Asegurar que 'country' y 'type' estén presentes, rellenando NaN si es necesario
    if 'country' not in final_merged_df.columns:
        logger.warning("'country' no está en el DataFrame final. Creándola con 'N/A'.")
        final_merged_df['country'] = 'N/A'
    else:
        final_merged_df['country'] = final_merged_df['country'].fillna('N/A')

    if 'type' not in final_merged_df.columns:
        logger.warning("'type' no está en el DataFrame final. Creándola con 'N/A'.")
        final_merged_df['type'] = 'N/A'
    else:
        final_merged_df['type'] = final_merged_df['type'].fillna('N/A')

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
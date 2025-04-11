"""
Transform Spotify dataset.
This module contains a function to transform the Spotify dataset.
The function performs the following transformations:
1. Drops the 'Unnamed: 0' column if it exists.
2. Removes rows with missing values.
3. Removes duplicate rows.
4. Converts all text columns to lowercase.
5. Groups the dataset by 'track_id', combines 'track_genre', and preserves other columns.
6. Calculates the mean popularity for each 'track_id', categorizes it into levels, and drops the mean.
7. Merges the transformed data back, ensuring no nulls remain.
8. Saves the transformed dataset to a temporary CSV file and returns the file path.
"""

import logging
import pandas as pd
import tempfile
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(message)s - %(levelname)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def transform_spotify_data(ti):
    """
    Transform the Spotify dataset by reading it from a temporary CSV file,
    applying transformations, and saving the result to a new temporary CSV file.

    Args:
        ti: Task instance to pull the file path from XCom.

    Returns:
        str: Path to the temporary CSV file where the transformed DataFrame is saved.
    """
    tmp_file_path = ti.xcom_pull(task_ids='read_csv')
    if not tmp_file_path:
        raise ValueError("No file path received from read_csv task")

    logger.info(f"Leyendo DataFrame desde archivo temporal: {tmp_file_path}")
    df_spotify = pd.read_csv(tmp_file_path)
    logger.info("DataFrame leído exitosamente para transformación.")

    # 1. Eliminar la columna 'Unnamed: 0' si existe
    if "Unnamed: 0" in df_spotify.columns:
        df_spotify = df_spotify.drop(columns=["Unnamed: 0"])

    # 2. Eliminar filas con valores nulos o faltantes
    df_spotify = df_spotify.dropna()

    # 3. Eliminar duplicados
    df_spotify = df_spotify.drop_duplicates()

    # 4. Convertir todas las columnas de texto a minúsculas
    text_columns = df_spotify.select_dtypes(include=['object']).columns
    for col in text_columns:
        df_spotify[col] = df_spotify[col].str.lower()

    # 5. Agrupar por 'track_id' y combinar 'track_genre'
    agg_dict = {
        "track_genre": lambda x: ", ".join(set(x.dropna())),  # Combinar géneros únicos, ignorando nulos
    }
    # Preservar otras columnas con 'first'
    for col in df_spotify.columns:
        if col not in ["track_id", "track_genre"]:
            agg_dict[col] = "first"

    df_spotify_grouped = (
        df_spotify.groupby("track_id")
        .agg(agg_dict)
        .reset_index()
    )

    # 6. Calcular la popularidad promedio por 'track_id', categorizarla y eliminar el promedio
    df_popularity = (
        df_spotify.groupby("track_id")["popularity"]
        .mean()
        .reset_index()
        .rename(columns={"popularity": "popularity_mean"})  # Renombrar para claridad
    )
    bins = [0, 20, 40, 60, 80, 100]
    labels = ["very low", "low", "medium", "high", "very high"]  # También en minúsculas
    df_popularity["popularity_category"] = pd.cut(
        df_popularity["popularity_mean"],
        bins=bins,
        labels=labels,
        include_lowest=True
    )
    # Eliminar la columna 'popularity_mean' después de categorizar
    df_popularity = df_popularity.drop(columns=["popularity_mean"])

    # 7. Combinar las transformaciones con el DataFrame agrupado
    df_spotify_transformed = df_spotify_grouped.merge(
        df_popularity[["track_id", "popularity_category"]],
        on="track_id",
        how="left"
    )

    # 8. Eliminar filas con valores nulos después de la transformación
    df_spotify_transformed = df_spotify_transformed.dropna()

    # 9. Guardar el resultado en un archivo temporal CSV
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        df_spotify_transformed.to_csv(tmp_file.name, index=False)
        transformed_tmp_file_path = tmp_file.name
        num_rows = len(df_spotify_transformed)
        logger.info(f"DataFrame transformado guardado en: {transformed_tmp_file_path} con {num_rows} filas")

    # Eliminar el archivo temporal original
    try:
        os.remove(tmp_file_path)
        logger.info(f"Archivo temporal original eliminado: {tmp_file_path}")
    except Exception as e:
        logger.warning(f"No se pudo eliminar el archivo temporal {tmp_file_path}: {e}")

    return transformed_tmp_file_path
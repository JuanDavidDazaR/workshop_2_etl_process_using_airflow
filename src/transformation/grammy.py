"""Transformation module for Grammy Awards data.
This module contains functions to transform the Grammy Awards data
by cleaning, converting text to lowercase, selecting relevant columns, 
and saving to a temporary CSV file.
"""

import pandas as pd
import tempfile
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def transform_grammy_data(ti):
    """
    Transforms the Grammy Awards data by:
    - Dropping rows with any null values.
    - Converting all text columns to lowercase.
    - Keeping only selected relevant columns.
    - Saving the transformed data to a temporary CSV file.

    Args:
        ti: Task instance to pull the file path from XCom (e.g., from a 'read_grammy' task).

    Returns:
        str: Path to the temporary file where the transformed DataFrame is saved in CSV format.
    """
    # Obtener la ruta del archivo crudo desde una tarea anterior (e.g., 'read_grammy')
    grammy_file_path = ti.xcom_pull(task_ids='read_grammy')
    if not grammy_file_path:
        raise ValueError("No file path received from read_grammy task")

    # Leer el archivo crudo como CSV
    logger.info(f"Leyendo Grammy desde: {grammy_file_path}")
    df_grammy = pd.read_csv(grammy_file_path)  # Leer como CSV

    # Transformaciones en memoria
    logger.info("Transformando datos de Grammy...")

    # 1. Eliminar filas con valores nulos
    df_grammy = df_grammy.dropna()

    # 2. Convertir todas las columnas de texto a minúsculas
    text_columns = df_grammy.select_dtypes(include=['object']).columns
    for col in text_columns:
        df_grammy[col] = df_grammy[col].str.lower()

    # 3. Seleccionar columnas relevantes
    selected_columns = ["year", "title", "category", "nominee", "artist", "winner"]
    df_grammy = df_grammy[selected_columns]

    # Guardar el DataFrame transformado en un archivo temporal CSV
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        df_grammy.to_csv(tmp_file.name, index=False)  # Guardar como CSV
        transformed_tmp_file_path = tmp_file.name
        logger.info(f"DataFrame transformado guardado en: {transformed_tmp_file_path} con {len(df_grammy)} filas")

    # Eliminar el archivo temporal original
    try:
        os.remove(grammy_file_path)
        logger.info(f"Archivo temporal original eliminado: {grammy_file_path}")
    except Exception as e:
        logger.warning(f"No se pudo eliminar el archivo temporal {grammy_file_path}: {e}")

    return transformed_tmp_file_path
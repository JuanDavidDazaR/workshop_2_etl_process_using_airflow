import pandas as pd
import os
from datetime import datetime

def transform_musicbrainz_data(input_file="/tmp/musicbrainz_temp_random.csv", output_temp_dir="/tmp"):
    """
    Transforma los datos extraídos de MusicBrainz: elimina nulos, duplicados y convierte fechas.
    
    Args:
        input_file (str): Ruta del archivo CSV temporal de entrada.
        output_temp_dir (str): Directorio donde se guardará el archivo CSV temporal transformado.
    
    Returns:
        str: Ruta del archivo CSV transformado.
    """
    # Verificar si el archivo de entrada existe
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"El archivo {input_file} no existe.")

    # Leer el archivo CSV
    df = pd.read_csv(input_file)

    # 1. Eliminar filas con datos nulos en columnas clave
    # Consideramos "artist_id" y "name" como columnas esenciales
    df = df.dropna(subset=["artist_id", "name"])

    # 2. Eliminar duplicados basados en "artist_id" (clave única)
    df = df.drop_duplicates(subset=["artist_id"], keep="first")

    # 3. Convertir columnas de fechas a tipo datetime
    # Manejar "N/A" y formatos variados
    def parse_date(date_str):
        if pd.isna(date_str) or date_str == "N/A":
            return pd.NaT  # Not a Time (valor nulo para fechas)
        try:
            # Intentar parsear como fecha completa o solo año
            return pd.to_datetime(date_str, errors="coerce")
        except Exception:
            return pd.NaT

    # Aplicar la conversión a las columnas de fecha
    df["begin_date"] = df["begin_date"].apply(parse_date)
    df["end_date"] = df["end_date"].apply(parse_date)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # 4. Opcional: Reemplazar "N/A" en otras columnas por valores vacíos o específicos
    df = df.replace("N/A", "")

    output_temp_dir = "/tmp"
    os.makedirs(output_temp_dir, exist_ok=True)
    output_file = f"{output_temp_dir}/musicbrainz_transformed_temp.csv"
    df.to_csv(output_file, index=False)
    print(f"Datos transformados guardados en: {output_file}")

    return output_file 
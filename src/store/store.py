# src/store.py
"""Module to store data in Google Drive."""

import logging
import pandas as pd
import os
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv
from pathlib import Path

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)

# Cargar variables de entorno desde .env en el directorio raíz
env_path = Path(__file__).parent.parent.resolve() / ".env"
load_dotenv(dotenv_path=env_path)

# Obtener variables de entorno
config_dir = Path(os.getenv("CONFIG_DIR")).resolve()
client_secrets_file = os.getenv("CLIENT_SECRETS_FILE")
settings_file = os.getenv("SETTINGS_FILE")
credentials_file = os.getenv("CREDENTIALS_FILE")
folder_id = os.getenv("FOLDER_ID")

# Verificar que las variables estén definidas
if not all([client_secrets_file, settings_file, credentials_file, folder_id]):
    raise ValueError("Faltan variables de entorno en .env. Verifica CONFIG_DIR, CLIENT_SECRETS_FILE, SETTINGS_FILE, CREDENTIALS_FILE y FOLDER_ID.")
logger.info(f"Using folder_id: {folder_id}")
logger.info(f"Using settings_file: {settings_file}")

# Función para autenticar Google Drive
def auth_drive():
    try:
        logger.info("Starting Google Drive authentication process.")
        gauth = GoogleAuth()
        
        # Cargar settings.yaml explícitamente
        gauth.settings_file = settings_file
        gauth.LoadClientConfigFile(client_secrets_file)
        
        # Forzar el puerto 8081 antes de cualquier autenticación
        gauth.local_webserver_port = 8081
        logger.info(f"Local webserver port configurado: {gauth.local_webserver_port}")
        
        if os.path.exists(credentials_file):
            gauth.LoadCredentialsFile(credentials_file)
            if gauth.access_token_expired:
                logger.info("Access token expired, refreshing token.")
                gauth.Refresh()
            else:
                logger.info("Using saved credentials.")
        else:
            logger.info("Saved credentials not found, performing web authentication.")
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(credentials_file)
            logger.info("Local webserver authentication completed and credentials saved successfully.")

        drive = GoogleDrive(gauth)
        logger.info("Google Drive authentication completed successfully.")
        return drive
    except Exception as e:
        logger.error(f"Authentication error: {e}", exc_info=True)
        raise


# Función para subir el DataFrame a Google Drive
def store_to_drive(ti):
    """
    Store the merged dataset in Google Drive.

    Args:
        ti: Task instance to pull file path from XCom.
    """
    merged_file_load_path = ti.xcom_pull(task_ids='load_to_db')
    if not merged_file_load_path:
        raise ValueError("No file path received from load_to_db task")

    # Verificar que el archivo exista
    if not os.path.exists(merged_file_load_path):
        raise FileNotFoundError(f"Merged file not found: {merged_file_load_path}")

    # Leer el DataFrame combinado (como CSV)
    logger.info(f"Leyendo DataFrame combinado desde: {merged_file_load_path}")
    df = pd.read_csv(merged_file_load_path)

    # Subir a Google Drive
    drive = auth_drive()
    title = "spotify_grammy_merged.csv"
    logger.info(f"Storing {title} on Google Drive.")
    
    csv_file = df.to_csv(index=False)
    
    file = drive.CreateFile({
        "title": title,
        "parents": [{"kind": "drive#fileLink", "id": folder_id}],
        "mimeType": "text/csv"
    })
    
    file.SetContentString(csv_file)
    file.Upload()
    logger.info(f"File {title} uploaded successfully to Google Drive.")

    # Limpiar el archivo temporal
    try:
        os.remove(merged_file_load_path)
        logger.info(f"Archivo temporal eliminado: {merged_file_load_path}")
    except Exception as e:
        logger.warning(f"No se pudo eliminar el archivo temporal {merged_file_load_path}: {e}")
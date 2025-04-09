from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from dotenv import load_dotenv
from pathlib import Path
import os
import pandas as pd
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%d/%m/%Y %I:%M:%S %p")

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
logging.info(f"Using folder_id: {folder_id}")
logging.info(f"Using settings_file: {settings_file}")

# Función para autenticar Google Drive
def auth_drive():
    try:
        logging.info("Starting Google Drive authentication process.")
        gauth = GoogleAuth()
        
        # Cargar settings.yaml explícitamente
        gauth.settings_file = settings_file
        gauth.LoadClientConfigFile(client_secrets_file)
        
        # Forzar el puerto 8081 antes de cualquier autenticación
        gauth.local_webserver_port = 8081
        logging.info(f"Local webserver port configurado: {gauth.local_webserver_port}")
        
        if os.path.exists(credentials_file):
            gauth.LoadCredentialsFile(credentials_file)
            if gauth.access_token_expired:
                logging.info("Access token expired, refreshing token.")
                gauth.Refresh()
            else:
                logging.info("Using saved credentials.")
        else:
            logging.info("Saved credentials not found, performing web authentication.")
            gauth.LocalWebserverAuth()
            gauth.SaveCredentialsFile(credentials_file)
            logging.info("Local webserver authentication completed and credentials saved successfully.")

        drive = GoogleDrive(gauth)
        logging.info("Google Drive authentication completed successfully.")
        return drive
    except Exception as e:
        logging.error(f"Authentication error: {e}", exc_info=True)
        raise

# Función para subir el DataFrame a Google Drive
def storing_merged_data(title, df):
    drive = auth_drive()
    logging.info(f"Storing {title} on Google Drive.")
    
    csv_file = df.to_csv(index=False)
    
    file = drive.CreateFile({
        "title": title,
        "parents": [{"kind": "drive#fileLink", "id": folder_id}],
        "mimeType": "text/csv"
    })
    
    file.SetContentString(csv_file)
    file.Upload()
    logging.info(f"File {title} uploaded successfully.")

# Prueba manual
if __name__ == "__main__":
    df = pd.DataFrame({"song": ["Song A", "Song B"], "artist": ["Artist 1", "Artist 2"]})
    storing_merged_data("test_upload.csv", df)
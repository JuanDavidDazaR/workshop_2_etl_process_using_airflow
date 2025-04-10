import requests
import pandas as pd
import os
import time
import random
from datetime import datetime


def extract_musicbrainz_artists(num_artists=100, output_dir="data/1_interm", temp_dir="/tmp"):
    """
    Extrae datos de un número específico de artistas "aleatorios" desde MusicBrainz API y los guarda en CSV.
    Continúa con el siguiente paso incluso si hay errores.
    
    Args:
        num_artists (int): Número de artistas a extraer (máximo 100 por limitaciones de la API).
        output_dir (str): Directorio donde se guardará el archivo CSV final (data/1_interm).
        temp_dir (str): Directorio temporal para el archivo CSV intermedio.
    """
    # Asegurarse de que num_artists no exceda el límite por página de MusicBrainz (100)
    limit_per_page = min(num_artists, 100)
    
    # Generar un offset aleatorio para simular aleatoriedad
    max_offset = 10000  # Estimación conservadora, MusicBrainz tiene millones de artistas
    offset = random.randint(0, max_offset)

    # URL y parámetros de búsqueda
    url = "https://musicbrainz.org/ws/2/artist"
    params = {
        "query": "artist",  # Consulta genérica para obtener cualquier artista
        "limit": limit_per_page,
        "offset": offset,  # Desplazamiento aleatorio
        "fmt": "json"
    }
    headers = {
        "User-Agent": "MiETLApp/1.0 (tucorreo@ejemplo.com)"  # Reemplaza con tu correo
    }

    # Hacer la solicitud de búsqueda con manejo de errores
    artists = []
    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            search_data = response.json()
            artists = search_data.get("artists", [])
        else:
            print(f"Error en la solicitud de búsqueda: {response.status_code}. Continuando sin datos iniciales.")
    except requests.RequestException as e:
        print(f"Excepción en la solicitud de búsqueda: {e}. Continuando sin datos iniciales.")

    if not artists:
        print("No se encontraron artistas en esta búsqueda. Continuando con un DataFrame vacío.")

    # Lista para almacenar datos de todos los artistas
    all_artists_data = []

    # Iterar sobre los artistas encontrados (hasta num_artists)
    for i, artist in enumerate(artists[:num_artists]):
        # Consultar detalles completos del artista usando su MBID
        artist_url = f"https://musicbrainz.org/ws/2/artist/{artist['id']}"
        params_detail = {
            "inc": "aliases+genres+release-groups+tags",
            "fmt": "json"
        }
        try:
            response_detail = requests.get(artist_url, params=params_detail, headers=headers)
            if response_detail.status_code != 200:
                print(f"Error al obtener detalles de {artist['name']}: {response_detail.status_code}. Pasando al siguiente.")
                continue

            # Obtener datos detallados
            data = response_detail.json()

            # Estructurar los datos en un diccionario
            artist_data = {
                "artist_id": data.get("id", ""),
                "name": data.get("name", ""),
                "sort_name": data.get("sort-name", ""),
                "type": data.get("type", ""),
                "country": data.get("country", "N/A"),
                "begin_area": data.get("begin-area", {}).get("name", "N/A") if data.get("begin-area") else "N/A",
                "begin_date": data.get("life-span", {}).get("begin", "N/A"),
                "end_date": data.get("life-span", {}).get("end", "N/A"),
                "genres": ", ".join([g["name"] for g in data.get("genres", [])]),
                "tags": ", ".join([t["name"] for t in data.get("tags", [])]),
                "release_groups": ", ".join([rg["title"] for rg in data.get("release-groups", [])]),
                "timestamp": datetime.now().isoformat()
            }
            all_artists_data.append(artist_data)

            print(f"Procesado artista {i+1}/{min(num_artists, len(artists))}: {artist['name']}")
        except requests.RequestException as e:
            print(f"Excepción al obtener detalles de {artist['name']}: {e}. Pasando al siguiente.")
            continue

        # Respetar el límite de 1 solicitud por segundo
        time.sleep(1)

    # Convertir a DataFrame (incluso si está vacío)
    df = pd.DataFrame(all_artists_data)

    # Crear directorios si no existen
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    # Guardar en archivo temporal
    temp_file = f"{temp_dir}/musicbrainz_temp_random.csv"
    df.to_csv(temp_file, index=False)
    print(f"Datos guardados temporalmente en: {temp_file}")

    # Guardar en carpeta data/1_interm
    output_file = f"{output_dir}/musicbrainz_artists_random.csv"
    df.to_csv(output_file, index=False)
    print(f"Datos guardados en: {output_file}")

    return temp_file

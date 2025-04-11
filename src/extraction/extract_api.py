# src/extract/extract_musicbrainz.py
import requests
import pandas as pd
import os
import time
import random
from datetime import datetime


def extract_musicbrainz_artists(num_artists=2000, output_dir="data/1_interm", temp_dir="/tmp"):
    limit_per_page = 100
    num_pages = (num_artists + limit_per_page - 1) // limit_per_page
    initial_offset = random.randint(0, 10000)
    url = "https://musicbrainz.org/ws/2/artist"
    headers = {"User-Agent": "MiETLApp/1.0 (tucorreo@ejemplo.com)"}

    all_artists = []
    for page in range(num_pages):
        offset = initial_offset + (page * limit_per_page)
        params = {"query": "artist", "limit": limit_per_page, "offset": offset, "fmt": "json"}
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                artists = response.json().get("artists", [])
                all_artists.extend(artists)
                print(f"Página {page + 1}/{num_pages}: {len(artists)} artistas obtenidos.")
            else:
                print(f"Error en página {page + 1}: {response.status_code}.")
        except requests.RequestException as e:
            print(f"Excepción en página {page + 1}: {e}.")
        time.sleep(1)
        if len(all_artists) >= num_artists:
            break

    all_artists = all_artists[:num_artists]
    print(f"Total de artistas recolectados: {len(all_artists)}")

    all_artists_data = []
    for i, artist in enumerate(all_artists):
        artist_url = f"https://musicbrainz.org/ws/2/artist/{artist['id']}"
        params_detail = {"inc": "aliases+genres+release-groups+tags", "fmt": "json"}
        try:
            response_detail = requests.get(artist_url, params=params_detail, headers=headers)
            if response_detail.status_code == 200:
                data = response_detail.json()
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
                print(f"Procesado artista {i + 1}/{len(all_artists)}: {artist['name']}")
        except requests.RequestException as e:
            print(f"Excepción al obtener detalles de {artist['name']}: {e}.")
        time.sleep(1)

    df = pd.DataFrame(all_artists_data)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)

    temp_file = f"{temp_dir}/musicbrainz_temp_random.csv"
    df.to_csv(temp_file, index=False)
    print(f"Datos guardados temporalmente en: {temp_file}")

    output_file = f"{output_dir}/musicbrainz_artists_random.csv"
    df.to_csv(output_file, index=False)
    print(f"Datos guardados en: {output_file}")

    return temp_file


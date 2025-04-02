""" Module to read the Spotify dataset from a CSV file. """
import pandas as pd


def read_csv_spotify():
    """
    Reads the Spotify dataset from a CSV file.

    Returns:
        pd.DataFrame: A DataFrame containing the dataset.
        None: If the file cannot be read due to an error.
    """
    try:
        df_spotify = pd.read_csv("../data/0_raw/spotify_dataset.csv")
        return df_spotify
    except FileNotFoundError:
        print("Error: File not found. Please check the file path.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: Error parsing the file. Check its format.")
    return None

import pandas as pd


def transform_spotify_dataset(df_spotify):
    
    if 'Unnamed: 0' in df_spotify.columns:
        df_spotify = df_spotify.drop(columns=['Unnamed: 0'])

    df_spotify = df_spotify.dropna()

    df_spotify = df_spotify.drop_duplicates()

    def join_track_genre(df_spotify_filter):
        except_track_genre = [col for col in df_spotify_filter.columns if col != "track_genre"]
        df_spotify_filter_grouped = df_spotify_filter.groupby(except_track_genre)["track_genre"].apply(
            lambda x: ", ".join(set(x))  
        ).reset_index()
        return df_spotify_filter_grouped

    df_spotify_filter = join_track_genre(df_spotify)

    df_spotify_grouped = df_spotify_filter.groupby("track_id")["popularity"].mean().reset_index()

    bins = [0, 20, 40, 60, 80, 100]
    labels = ["Muy Baja", "Baja", "Media", "Alta", "Muy Alta"]
    df_spotify_grouped["popularity"] = pd.cut(df_spotify_grouped["popularity"], bins=bins, labels=labels, include_lowest=True)

    df_spotify_filter = df_spotify_filter.drop(columns=["popularity"])
    df_spotify_filter = df_spotify_filter.merge(df_spotify_grouped[["track_id", "popularity"]], on="track_id", how="left")

    df_spotify_filter = join_track_genre(df_spotify_filter)
   

    return df_spotify_filter

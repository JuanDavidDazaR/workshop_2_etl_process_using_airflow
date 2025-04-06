"""
Transform Spotify dataset.

This module contains a function to transform the Spotify dataset.
The function performs the following transformations:
1. Drops the 'Unnamed: 0' column if it exists.
2. Removes rows with missing values.
3. Removes duplicate rows.
4. Groups the dataset by all
columns except'track_genre'and combines the genres.
5. Calculates the mean popularity for each 'track_id'.
6. Categorizes popularity
into bins: 'Very Low', 'Low', 'Medium', 'High', 'Very High'.
7. Merges the popularity categories back into the dataset.
8. Returns the transformed dataset.
"""

import pandas as pd


def transform_spotify_dataset(df_spotify):
    """
    Transform the Spotify dataset with cleaning, grouping, and
    popularity categorization.

    Args:
        df_spotify (pd.DataFrame): Raw Spotify data.

    Returns:
        pd.DataFrame: Transformed Spotify dataset.
    """
    if "Unnamed: 0" in df_spotify.columns:
        df_spotify = df_spotify.drop(columns=["Unnamed: 0"])

    df_spotify = df_spotify.dropna()
    df_spotify = df_spotify.drop_duplicates()

    def join_track_genre(df_spotify_filter):
        """
        Group by all columns except 'track_genre' and concatenate genre values.

        Args:
            df_spotify_filter (pd.DataFrame): Filtered DataFrame.

        Returns:
            pd.DataFrame: DataFrame with combined 'track_genre' per group.
        """
        except_track_genre = [
            col for col in df_spotify_filter.columns if col != "track_genre"
        ]
        df_spotify_filter_grouped = (
            df_spotify_filter.groupby(except_track_genre)["track_genre"]
            .apply(lambda x: ", ".join(set(x)))
            .reset_index()
        )
        return df_spotify_filter_grouped

    df_spotify_filter = join_track_genre(df_spotify)

    df_spotify_grouped = (
        df_spotify_filter
        .groupby("track_id")["popularity"].mean().reset_index()
    )

    bins = [0, 20, 40, 60, 80, 100]
    labels = ["Very Low", "Low", "Medium", "High", "Very High"]
    df_spotify_grouped["popularity"] = pd.cut(
        df_spotify_grouped["popularity"],
        bins=bins, labels=labels, include_lowest=True
    )

    df_spotify_filter = df_spotify_filter.drop(columns=["popularity"])
    df_spotify_filter = df_spotify_filter.merge(
        df_spotify_grouped[["track_id", "popularity"]],
        on="track_id", how="left"
    )

    df_spotify_filter = join_track_genre(df_spotify_filter)

    return df_spotify_filter

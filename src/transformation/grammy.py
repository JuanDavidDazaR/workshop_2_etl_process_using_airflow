"""Transformation module for Grammy Awards data.
This module contains functions to transform the Grammy Awards data
by cleaning and selecting relevant columns.
"""

import pandas as pd


def transform_grammy_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the Grammy Awards data by:
    - Dropping rows with any null values.
    - Keeping only selected relevant columns.

    Args:
        df (pd.DataFrame): Raw Grammy Awards data.

    Returns:
        pd.DataFrame: Cleaned and transformed DataFrame.
    """

    # Drop all rows with null values
    df = df.dropna()

    # Keep only required columns
    selected_columns = \
        ["year", "title", "category", "nominee", "artist", "workers", "winner"]
    df = df[selected_columns]

    return df

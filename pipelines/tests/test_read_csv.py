import unittest
from unittest.mock import patch, mock_open
import pandas as pd

from workshop_2_etl_process_using_airflow.extraction.read_csv import (
    read_csv_spotify,
)


class TestReadCSVSpotify(unittest.TestCase):

    @patch(
        "builtins.open",
        new_callable=mock_open,
        read_data="track_id,track_name,artists\n1,Song A,Artist A",
    )
    @patch("pandas.read_csv")
    def test_read_csv_success(self, mock_read_csv, mock_file):
        expected_df = pd.DataFrame(
            {"track_id": [1], "track_name": ["Song A"], "artists": ["Artist A"]
             }
        )
        mock_read_csv.return_value = expected_df
        result = read_csv_spotify()
        pd.testing.assert_frame_equal(result, expected_df)

    @patch("pandas.read_csv", side_effect=FileNotFoundError)
    def test_file_not_found(self, mock_read_csv):
        result = read_csv_spotify()
        self.assertIsNone(result)

    @patch("pandas.read_csv", side_effect=pd.errors.EmptyDataError)
    def test_empty_file(self, mock_read_csv):
        result = read_csv_spotify()
        self.assertIsNone(result)

    @patch("pandas.read_csv", side_effect=pd.errors.ParserError)
    def test_parser_error(self, mock_read_csv):
        result = read_csv_spotify()
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()

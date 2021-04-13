import unittest

import pandas as pd


class TestTask1(unittest.TestCase):

    def setUp(self):
        self.bike_df = pd.read_csv("./output/bike_task1.csv")
        self.weather_df = pd.read_csv("./output/weather_task1.csv")

    def tearDown(self):
        self.bike_df = None
        self.weather_df = None

    def test_bike_columns_ok(self):
        cols = list(self.bike_df.columns)

        self.assertIn("dd_mm_yy", cols)
        self.assertIn("city_id", cols)
        self.assertIn("bikes_available_citywide", cols)
        self.assertIn("count_1", cols)
        self.assertIn("count_2", cols)

    def test_bike_rows_ok(self):
        self.assertGreater(self.bike_df.shape[0], 1000000)

    def test_bike_inactive_stations_removed_ok(self):
        self.assertEqual(1, self.bike_df['status'].nunique())

    def test_weather_columns_ok(self):
        cols = list(self.bike_df.columns)

        self.assertIn("city_id", cols)

    def test_weather_rows_ok(self):
        self.assertGreater(self.weather_df.shape[0], 40000)


if __name__ == '__main__':
    unittest.main()
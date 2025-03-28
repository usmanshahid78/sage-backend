import unittest
from unittest.mock import patch, MagicMock
import os
import json
import numpy as np
from io import BytesIO
from PIL import Image
import cv2
import psycopg2
from scrappers.googleEarth import (
    get_coordinates,
    get_elevation,
    detect_trees,
    detect_buildings,
    insert_data_into_db,
    get_json_data,
)

class TestGetCoordinates(unittest.TestCase):
    @patch("requests.get")
    def test_get_coordinates_success(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [{"geometry": {"lat": 44.12345, "lng": -121.12345}}]
        }
        mock_get.return_value = mock_response

        # Call the function
        lat, lon = get_coordinates("3715 NW COYNER AVE, REDMOND, OR 97756")

        # Assertions
        self.assertEqual(lat, 44.12345)
        self.assertEqual(lon, -121.12345)
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_get_coordinates_failure(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        # Call the function
        lat, lon = get_coordinates("Invalid Address")

        # Assertions
        self.assertIsNone(lat)
        self.assertIsNone(lon)

class TestGetElevation(unittest.TestCase):
    @patch("requests.get")
    def test_get_elevation_success(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "results": [{"elevation": 1000}]  # Elevation in meters
        }
        mock_get.return_value = mock_response

        # Call the function
        elevation = get_elevation(44.12345, -121.12345)

        # Assertions
        self.assertAlmostEqual(elevation, 1000 * 3.281, places=2)  # Convert meters to feet
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_get_elevation_failure(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.json.return_value = {"results": []}
        mock_get.return_value = mock_response

        # Call the function
        elevation = get_elevation(44.12345, -121.12345)

        # Assertions
        self.assertIsNone(elevation)

class TestDetectTrees(unittest.TestCase):
    @patch("requests.get")
    def test_detect_trees_success(self, mock_get):
        # Mock the API response with a sample image
        img = Image.new("RGB", (600, 400), color=(50, 205, 50))  # Green image
        img_byte_arr = BytesIO()
        img.save(img_byte_arr, format="PNG")
        img_byte_arr.seek(0)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = img_byte_arr.getvalue()
        mock_get.return_value = mock_response

        # Call the function
        has_trees = detect_trees(44.12345, -121.12345)

        # Assertions
        self.assertTrue(has_trees)
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_detect_trees_failure(self, mock_get):
        # Mock the API response with an error
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Call the function
        has_trees = detect_trees(44.12345, -121.12345)

        # Assertions
        self.assertIsNone(has_trees)

class TestDetectBuildings(unittest.TestCase):
    # @patch("requests.get")
    # def test_detect_buildings_success(self, mock_get):
    #     # Mock the API response with a sample image
    #     img = Image.new("RGB", (600, 400), color=(128, 128, 128))  # Gray image
    #     img_byte_arr = BytesIO()
    #     img.save(img_byte_arr, format="PNG")
    #     img_byte_arr.seek(0)
    #
    #     mock_response = MagicMock()
    #     mock_response.status_code = 200
    #     mock_response.content = img_byte_arr.getvalue()
    #     mock_get.return_value = mock_response
    #
    #     # Call the function
    #     has_buildings = detect_buildings(44.12345, -121.12345)
    #
    #     # Assertions
    #     self.assertTrue(has_buildings)
    #     mock_get.assert_called_once()

    @patch("requests.get")
    def test_detect_buildings_failure(self, mock_get):
        # Mock the API response with an error
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Call the function
        has_buildings = detect_buildings(44.12345, -121.12345)

        # Assertions
        self.assertIsNone(has_buildings)

class TestInsertDataIntoDb(unittest.TestCase):
    @patch("psycopg2.connect")
    def test_insert_data_into_db_success(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Call the function
        insert_data_into_db(128433, 44.12345, -121.12345, 5.0, True, False)

        # Assertions
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

class TestGetJsonData(unittest.TestCase):
    def test_get_json_data(self):
        # Test data
        data = {
            "property_id": 128433,
            "gps_coord": "44.12345,-121.12345",
            "slope": 5.0,
            "has_trees": True,
            "has_buildings": False,
            "power_visible": False,
        }

        # Call the function
        json_data = get_json_data(data)

        # Assertions
        self.assertIsInstance(json_data, str)
        self.assertEqual(json.loads(json_data), data)

if __name__ == "__main__":
    unittest.main()
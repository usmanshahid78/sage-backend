import unittest
from unittest.mock import patch, MagicMock
import os
import json
import requests
from bs4 import BeautifulSoup
import psycopg2
from scrappers.planningData import (
    fetch_with_requests,
    fetch_with_selenium,
    parse_zoning_table,
    insert_zoning_data,
    get_json_data,
    analyze_zoning_code,
)

class TestFetchWithRequests(unittest.TestCase):
    @patch("requests.get")
    def test_fetch_with_requests_success(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Zoning Designation</body></html>"
        mock_get.return_value = mock_response

        # Call the function
        result = fetch_with_requests("https://example.com")

        # Assertions
        self.assertEqual(result, "<html><body>Zoning Designation</body></html>")
        mock_get.assert_called_once()

    @patch("requests.get")
    def test_fetch_with_requests_failure(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Call the function
        result = fetch_with_requests("https://example.com")

        # Assertions
        self.assertIsNone(result)

class TestFetchWithSelenium(unittest.TestCase):
    @patch("selenium.webdriver.Chrome")
    def test_fetch_with_selenium_success(self, mock_chrome):
        # Mock the WebDriver and its methods
        mock_driver = MagicMock()
        mock_driver.page_source = "<html><body>Zoning Designation</body></html>"
        mock_chrome.return_value = mock_driver

        # Call the function
        result = fetch_with_selenium("https://example.com")

        # Assertions
        self.assertEqual(result, "<html><body>Zoning Designation</body></html>")
        mock_chrome.assert_called_once()
        mock_driver.get.assert_called_once_with("https://example.com")
        mock_driver.quit.assert_called_once()

    @patch("selenium.webdriver.Chrome")
    def test_fetch_with_selenium_failure(self, mock_chrome):
        # Mock the WebDriver to raise an exception
        mock_chrome.side_effect = Exception("WebDriver error")

        # Call the function
        result = fetch_with_selenium("https://example.com")

        # Assertions
        self.assertIsNone(result)

class TestParseZoningTable(unittest.TestCase):
    def test_parse_zoning_table_success(self):
        # Sample HTML content
        html_content = """
        <table>
            <tr><th>Jurisdiction</th><th>Zone</th><th>Overlay</th></tr>
            <tr><td>Deschutes</td><td>MUA10</td><td>None</td></tr>
        </table>
        """

        # Call the function
        result = parse_zoning_table(html_content)

        # Assertions
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["jurisdiction"], "Deschutes")
        self.assertEqual(result[0]["zone"], "MUA10")
        self.assertEqual(result[0]["overlay"], "None")

    def test_parse_zoning_table_failure(self):
        # Call the function with invalid HTML
        result = parse_zoning_table("<html><body>No table here</body></html>")

        # Assertions
        self.assertEqual(result, [])

class TestInsertZoningData(unittest.TestCase):
    @patch("psycopg2.connect")
    def test_insert_zoning_data_success(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Sample zoning data
        zoning_data = [
            {
                "property_id": 128433,
                "jurisdiction": "Deschutes",
                "zone": "MUA10",
                "overlay": "None",
                "max_lot_coverage": "50%",
                "max_building_height": "35 ft",
                "front_setback": "20 ft",
                "side_setback": "10 ft",
                "rear_setback": "25 ft",
            }
        ]

        # Call the function
        insert_zoning_data(zoning_data)

        # Assertions
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

class TestGetJsonData(unittest.TestCase):
    def test_get_json_data(self):
        # Sample data
        data = [
            {
                "property_id": 128433,
                "jurisdiction": "Deschutes",
                "zone": "MUA10",
                "overlay": "None",
            }
        ]

        # Call the function
        result = get_json_data(data)

        # Assertions
        self.assertIsInstance(result, str)
        self.assertEqual(json.loads(result), data)

class TestAnalyzeZoningCode(unittest.TestCase):
    @patch("psycopg2.connect")
    def test_analyze_zoning_code(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Call the function
        result = analyze_zoning_code("MUA10")

        # Assertions
        self.assertIn("Zoning Requirements for MUA10", result)
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

if __name__ == "__main__":
    unittest.main()
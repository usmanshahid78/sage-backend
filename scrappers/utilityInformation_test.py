import unittest
from unittest.mock import patch, MagicMock
import requests
from bs4 import BeautifulSoup
import psycopg2
from scrappers.utilityInformation import (
    check_for_septic,
    check_for_well,
    save_to_db,
    check_water_systems,
)

class TestCheckForSeptic(unittest.TestCase):
    @patch("requests.get")
    def test_check_for_septic_success(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = """
        <html>
            <body>
                <table class="infoTable">
                    <tr><td>Permit Type</td><td>Septic</td></tr>
                </table>
            </body>
        </html>
        """
        mock_get.return_value = mock_response

        # Call the function
        result = check_for_septic("131527")

        # Assertions
        self.assertEqual(result, "Septic")
        mock_get.assert_called_once_with("https://dial.deschutes.org/Real/Permits/131527")

    @patch("requests.get")
    def test_check_for_septic_no_table(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>No table here</body></html>"
        mock_get.return_value = mock_response

        # Call the function
        result = check_for_septic("131527")

        # Assertions
        self.assertEqual(result, "No permit table found in the response.")

    @patch("requests.get")
    def test_check_for_septic_failure(self, mock_get):
        # Mock the API response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        # Call the function
        result = check_for_septic("131527")

        # Assertions
        self.assertIsNone(result)

class TestCheckForWell(unittest.TestCase):
    # @patch("selenium.webdriver.Chrome")
    # def test_check_for_well_success(self, mock_chrome):
    #     # Mock the WebDriver and its methods
    #     mock_driver = MagicMock()
    #     mock_driver.find_elements.return_value = [MagicMock(is_selected=MagicMock(return_value=True))]
    #     mock_chrome.return_value = mock_driver
    #
    #     # Call the function
    #     result = check_for_well()
    #
    #     # Assertions
    #     self.assertEqual(result, "Well")
    #     mock_chrome.assert_called_once()
    #     mock_driver.get.assert_called_once_with("https://apps.wrd.state.or.us/apps/gw/well_log/")
    #     mock_driver.quit.assert_called_once()

    # @patch("selenium.webdriver.Chrome")
    # def test_check_for_well_failure(self, mock_chrome):
    #     # Mock the WebDriver to raise an exception
    #     mock_chrome.side_effect = Exception("WebDriver error")
    #
    #     # Call the function
    #     result = check_for_well()
    #
    #     # Assertions
    #     self.assertIn("An error occurred", result)
    pass

class TestSaveToDb(unittest.TestCase):
    @patch("psycopg2.connect")
    def test_save_to_db_success(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Call the function
        save_to_db("131527", "Septic", "Well")

        # Assertions
        mock_connect.assert_called_once_with(
            host="sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com",
            database="sagedatabase",
            user="postgres",
            password="12345678",
            port="5432",
        )
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("psycopg2.connect")
    def test_save_to_db_failure(self, mock_connect):
        # Mock the database connection to raise an exception
        mock_connect.side_effect = Exception("Database error")

        # Call the function
        save_to_db("131527", "Septic", "Well")

        # Assertions
        mock_connect.assert_called_once()

class TestCheckWaterSystems(unittest.TestCase):
    # @patch("water_systems.check_for_septic")
    # @patch("water_systems.check_for_well")
    # @patch("water_systems.save_to_db")
    # def test_check_water_systems_success(self, mock_save_to_db, mock_check_for_well, mock_check_for_septic):
    #     # Mock the functions
    #     mock_check_for_septic.return_value = "Septic"
    #     mock_check_for_well.return_value = "Well"
    #
    #     # Call the function
    #     result = check_water_systems("131527")
    #
    #     # Assertions
    #     self.assertEqual(result, ("Septic", "Well"))
    #     mock_check_for_septic.assert_called_once_with("131527")
    #     mock_check_for_well.assert_called_once()
    #     mock_save_to_db.assert_called_once_with("131527", "Septic", "Well")
    pass

if __name__ == "__main__":
    unittest.main()
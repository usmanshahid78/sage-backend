import unittest
from flask import Flask, json
from main import app  # Replace 'your_flask_app' with the name of your Flask app file

class FlaskAppTestCase(unittest.TestCase):

    def setUp(self):
        """Set up the test client and other test variables."""
        self.app = app
        self.client = self.app.test_client()
        self.app.config['TESTING'] = True

    def test_fetch_property_data_valid_request(self):
        """Test the /fetch-property-data endpoint with valid data."""
        # Mock data for the request
        mock_data = {
            "property_id": "12345",
            "address": "123 Main St, Springfield, USA"
        }

        # Make a POST request to the endpoint
        response = self.client.post(
            '/fetch-property-data',
            data=json.dumps(mock_data),
            content_type='application/json'
        )

        # Check the response status code
        self.assertEqual(response.status_code, 200)

        # Check if the response contains the expected keys
        response_data = json.loads(response.data)
        self.assertIn("property_id", response_data)
        self.assertIn("gps_coord", response_data)
        self.assertIn("slope", response_data)
        self.assertIn("has_trees", response_data)
        self.assertIn("has_buildings", response_data)

    def test_generate_pdf_valid_request(self):
        """Test the /generate-pdf endpoint with valid data."""
        # Mock data for the request
        mock_data = {
            "property_id": "12345",
            "owner_name": "John Doe",
            "address": "123 Main St, Springfield, USA"
        }

        # Make a POST request to the endpoint
        response = self.client.post(
            '/generate-pdf',
            data=json.dumps(mock_data),
            content_type='application/json'
        )

        # Check the response status code
        self.assertEqual(response.status_code, 200)

        # Check if the response is a PDF file
        self.assertEqual(response.headers['Content-Type'], 'application/pdf')
        self.assertIn('attachment; filename=report.pdf', response.headers['Content-Disposition'])

    def test_generate_pdf_invalid_request(self):
        """Test the /generate-pdf endpoint with minimal data."""
        # Mock data with minimal fields
        mock_data = {
            "property_id": "12345"
        }

        # Make a POST request to the endpoint
        response = self.client.post(
            '/generate-pdf',
            data=json.dumps(mock_data),
            content_type='application/json'
        )

        # Check the response status code - endpoint accepts any valid JSON
        self.assertEqual(response.status_code, 200)

        # Check if the response is a PDF file
        self.assertEqual(response.headers['Content-Type'], 'application/pdf')
        self.assertIn('attachment; filename=report.pdf', response.headers['Content-Disposition'])

if __name__ == '__main__':
    unittest.main()
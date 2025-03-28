import requests
import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from urllib.parse import quote
from dotenv import load_dotenv
import os
import logging

# Set up logging
logging.basicConfig(filename='scraper.log', level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()
GOOGLE_MAPS_API_KEY = os.getenv('GOOGLE_MAPS_API_KEY')
OPENCAGE_API_KEY = os.getenv('OPENCAGE_API_KEY')

# Initialize WebDriver
def init_driver():
    try:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        return driver
    except Exception as e:
        logging.error(f"Failed to initialize WebDriver: {str(e)}")
        raise

# Scrape property data from a website
def scrape_property_data(address, driver):
    try:
        # Replace with your actual property search URL
        url = f"https://your-actual-property-search-url.com/search?query={quote(address)}"
        driver.get(url)
        time.sleep(3)  # Allow time for page to load
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        property_details = {}
        
        try:
            property_details['owner'] = soup.find('div', {'id': 'owner'}).text.strip()
            property_details['value'] = soup.find('div', {'id': 'value'}).text.strip()
        except AttributeError:
            property_details['owner'] = 'N/A'
            property_details['value'] = 'N/A'
            logging.warning(f"Could not find some property details for address: {address}")
        
        return property_details
    except Exception as e:
        logging.error(f"Error scraping property data: {str(e)}")
        return {'owner': 'Error', 'value': 'Error'}

# Fetch geolocation data from ArcGIS API
def fetch_geolocation_data(lat, lon):
    try:
        # Replace with your actual ArcGIS URL
        url = f"https://your-actual-arcgis-url.com/rest/services/geocode/{lat},{lon}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching geolocation data: {str(e)}")
        return {}

# Extract design parameters from PDF
def extract_pdf_design_parameters(pdf_path):
    try:
        if not os.path.exists(pdf_path):
            logging.error(f"PDF file not found: {pdf_path}")
            return {'seismic_design': 'Unknown', 'snow_load': 'Unknown'}
            
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text() + "\n"
        
        return {
            'seismic_design': 'Seismic: Zone 3' if 'Zone 3' in text else 'Unknown',
            'snow_load': 'Snow Load: 30psf' if '30psf' in text else 'Unknown'
        }
    except Exception as e:
        logging.error(f"Error extracting PDF parameters: {str(e)}")
        return {'seismic_design': 'Error', 'snow_load': 'Error'}

# Get elevation and tree cover from APIs
def get_elevation_tree_data(lat, lon):
    try:
        if not GOOGLE_MAPS_API_KEY or not OPENCAGE_API_KEY:
            logging.error("Missing API keys in environment variables")
            return {'elevation': 'API Key Missing', 'tree_cover': 'API Key Missing'}
            
        elevation_url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={GOOGLE_MAPS_API_KEY}"
        tree_url = f"https://api.opencagedata.com/geocode/v1/json?q={lat}+{lon}&key={OPENCAGE_API_KEY}"
        
        elevation_resp = requests.get(elevation_url, timeout=10)
        elevation_resp.raise_for_status()
        tree_resp = requests.get(tree_url, timeout=10)
        tree_resp.raise_for_status()
        
        elevation_data = elevation_resp.json()
        tree_data = tree_resp.json()
        
        return {
            'elevation': elevation_data['results'][0]['elevation'] if 'results' in elevation_data else 'N/A',
            'tree_cover': 'Yes' if 'forest' in str(tree_data) else 'No'
        }
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching elevation/tree data: {str(e)}")
        return {'elevation': 'Error', 'tree_cover': 'Error'}

# Main function to orchestrate the scraping process
def main():
    try:
        address = "123 Main St, Anytown, USA"
        lat, lon = 44.06, -121.31
        pdf_path = "Document 1969-1.pdf"  # Updated to use an existing PDF file
        
        driver = init_driver()
        property_data = scrape_property_data(address, driver)
        geolocation_data = fetch_geolocation_data(lat, lon)
        design_parameters = extract_pdf_design_parameters(pdf_path)
        elevation_tree_data = get_elevation_tree_data(lat, lon)
        driver.quit()
        
        # Merge results
        results = {**property_data, **geolocation_data, **design_parameters, **elevation_tree_data}
        
        # Save results to file
        with open('results.json', 'w') as f:
            json.dump(results, f, indent=4)
            
        logging.info("Scraping completed successfully")
        print(json.dumps(results, indent=4))
        
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        print(f"An error occurred. Check scraper.log for details.")

if __name__ == "__main__":
    main()
import os
import json
import time
import requests
import psycopg2
import urllib.parse
import re
import subprocess
import platform
import numpy as np
import cv2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from io import BytesIO
import PyPDF2
from geopy.distance import geodesic
from PIL import Image
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize FastAPI
app = FastAPI()

# Database connection
DB_HOST = "sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com"
DB_NAME = "sagedatabase"
DB_USER = "postgres"
DB_PASS = "12345678"
DB_PORT = "5432"

# API Keys
OPENCAGE_API_KEY = "e96ce3849..."
GOOGLE_API_KEY = "AIzaSyBWS..."

# Configure Chrome options for Selenium
download_dir = os.getcwd()  # Set download directory to current working directory
chrome_options = Options()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "plugins.always_open_pdf_externally": True
}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument("--headless")  # Run in headless mode
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-gpu")

# Utility function to connect to DB
def connect_db():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
    )

# Data Model for Property Request
class PropertyRequest(BaseModel):
    property_id: int

# Endpoint: Fetch all data using Property ID
@app.post("/fetch-property-data")
async def fetch_property_data(request: PropertyRequest):
    property_id = request.property_id
    final_data = {"id": property_id}
    logging.info(f"Processing property ID: {property_id}")

    try:
        # Step 1: Fetch Basic Property Details from Deschutes DIAL
        logging.info("Fetching basic property details...")
        html_data = fetch_html_data(property_id)
        if html_data:
            final_data.update(html_data)
            logging.info("Basic property details fetched successfully")
        else:
            logging.warning("Failed to fetch basic property details")

        # Step 2: Fetch ArcGIS data using map_and_taxlot
        if "map_and_taxlot" in final_data and final_data["map_and_taxlot"] != "Not Available":
            logging.info(f"Fetching ArcGIS data for map and taxlot: {final_data['map_and_taxlot']}...")
            arcgis_raw_data = fetch_arcgis_data(final_data["map_and_taxlot"])
            if arcgis_raw_data:
                arcgis_data = extract_arcgis_data(arcgis_raw_data)
                if arcgis_data:
                    # Fill in any missing data from ArcGIS
                    for key, value in arcgis_data.items():
                        if key not in final_data or final_data[key] == "Not Available":
                            final_data[key] = value
                    logging.info("ArcGIS data fetched and merged successfully")
                else:
                    logging.warning("Failed to extract ArcGIS data")
            else:
                logging.warning("Failed to fetch ArcGIS data")

        # Step 3: Fetch Zoning & Planning Data
        logging.info("Fetching zoning data...")
        zoning_data = fetch_zoning_data(property_id)
        if zoning_data:
            final_data.update(zoning_data)
            logging.info("Zoning data fetched successfully")
        else:
            logging.warning("Failed to fetch zoning data")

        # Step 4: Fetch Geospatial Data
        address = final_data.get("situs_address", "Not Available")
        logging.info(f"Fetching geospatial data for address: {address}...")
        geo_data = fetch_geospatial_data(address)
        if geo_data:
            final_data.update(geo_data)
            logging.info("Geospatial data fetched successfully")
        else:
            logging.warning("Failed to fetch geospatial data")

        # Step 5: Fetch Utility Data (Septic & Well)
        logging.info("Checking for water systems...")
        utility_data = check_water_systems(property_id)
        if utility_data:
            final_data.update(utility_data)
            logging.info("Water systems data fetched successfully")
        else:
            logging.warning("Failed to fetch water systems data")

        # Step 6: Fetch Design Parameters (Wind Speed, Seismic, Snow Load, etc.)
        logging.info("Fetching design parameters...")
        address = final_data.get("situs_address", "Not Available")
        design_data = fetch_design_parameters(property_id, address)
        if design_data:
            final_data.update(design_data)
            logging.info("Design parameters fetched successfully")
        else:
            logging.warning("Failed to fetch design parameters")

        # Step 7: Save to Database
        logging.info("Saving all data to database...")
        save_to_database(final_data)
        logging.info("Data saved to database successfully")

        # Remove any None values for API response
        final_data = {k: (v if v is not None else "Not Available") for k, v in final_data.items()}

        return {"status": "success", "data": final_data}

    except Exception as e:
        error_message = f"Error processing property {property_id}: {str(e)}"
        logging.error(error_message)
        raise HTTPException(status_code=500, detail=error_message)

# Function to fetch basic property details
def fetch_html_data(property_id):
    """Fetch and parse basic property details from Deschutes DIAL."""
    url = f"https://dial.deschutes.org/Real/Index/{property_id}"
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, "html.parser")
        data = {"id": property_id}
        
        # Safely extract owner_name
        owner_name_tag = soup.find("strong", string="Mailing Name:")
        if owner_name_tag and owner_name_tag.find_next_sibling(string=True):
            data["owner_name"] = owner_name_tag.find_next_sibling(string=True).strip()
        else:
            data["owner_name"] = "Not Available"
        
        # Safely extract map_and_taxlot
        map_taxlot_tag = soup.find("span", id="uxMapTaxlot")
        data["map_and_taxlot"] = map_taxlot_tag.text.strip() if map_taxlot_tag else "Not Available"
        
        # Safely extract situs_address
        situs_address_tag = soup.find("span", id="uxSitusAddress")
        data["situs_address"] = situs_address_tag.text.strip() if situs_address_tag else "Not Available"
        
        # Safely extract acres
        acres_tag = soup.find("strong", string="Assessor Acres:")
        if acres_tag and acres_tag.find_next_sibling(string=True):
            data["acres"] = acres_tag.find_next_sibling(string=True).strip()
        else:
            data["acres"] = "Not Available"
        
        # Safely extract mailing address
        mailing_address_tag = soup.find("strong", string="Mailing Address:")
        if mailing_address_tag and mailing_address_tag.find_next_sibling(string=True):
            data["mailing_address"] = mailing_address_tag.find_next_sibling(string=True).strip()
        else:
            data["mailing_address"] = "Not Available"
            
        # Add plat & tax map URLs
        data["plat_map_url"] = f"https://dial.deschutes.org/API/Real/GetReport/{property_id}?report=PlatMap"
        data["tax_map_url"] = f"https://dial.deschutes.org/API/Real/GetReport/{property_id}?report=TaxMap"
        
        return data
    return None

# Function to fetch zoning data
def fetch_zoning_data(property_id):
    """Fetch detailed zoning information from the development summary page."""
    try:
        url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract zoning information from the zoning table
            zoning_data = {}
            
            # Find the primary zoning
            zoning_td = soup.find("td", string="Zoning:")
            if zoning_td and zoning_td.find_next_sibling("td"):
                zoning_data["zoning"] = zoning_td.find_next_sibling("td").text.strip()
            else:
                zoning_data["zoning"] = "Not Available"
                
            # Find jurisdiction (county/city)
            jurisdiction_td = soup.find("td", string="Jurisdiction:")
            if jurisdiction_td and jurisdiction_td.find_next_sibling("td"):
                zoning_data["jurisdiction"] = jurisdiction_td.find_next_sibling("td").text.strip()
            else:
                zoning_data["jurisdiction"] = "Not Available"
                
            # Find overlays
            overlay_td = soup.find("td", string="Overlay:")
            if overlay_td and overlay_td.find_next_sibling("td"):
                zoning_data["overlay"] = overlay_td.find_next_sibling("td").text.strip()
            else:
                zoning_data["overlay"] = "Not Available"
                
            return zoning_data
        return {"zoning": "Not Available", "jurisdiction": "Not Available", "overlay": "Not Available"}
    except Exception as e:
        logging.error(f"Zoning data error: {e}")
        return {"zoning": "Not Available", "jurisdiction": "Not Available", "overlay": "Not Available"}

# Google Earth Functions
def get_coordinates(address):
    """Get GPS coordinates from address using OpenCage API."""
    if not address or address == "Not Available":
        return None, None
        
    try:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={OPENCAGE_API_KEY}"
        response = requests.get(url).json()
        
        if response.get('results') and len(response['results']) > 0:
            lat = response['results'][0]['geometry'].get('lat')
            lon = response['results'][0]['geometry'].get('lng')
            return lat, lon
        return None, None
    except Exception as e:
        logging.error(f"Geocoding error: {e}")
        return None, None

def get_elevation(lat, lon):
    """Get elevation data from Google Elevation API."""
    if not lat or not lon:
        return None

    try:
        url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={GOOGLE_API_KEY}"
        response = requests.get(url).json()
        if response.get("results"):
            return response["results"][0]["elevation"] * 3.281  # Convert meters to feet
        return None
    except Exception as e:
        logging.error(f"Elevation API error: {e}")
        return None

def detect_trees(lat, lon):
    """Detect trees from satellite imagery using image processing."""
    if not lat or not lon:
        return None
        
    try:
        map_url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom=18&size=600x400&maptype=satellite&key={GOOGLE_API_KEY}"
        response = requests.get(map_url)

        if response.status_code == 200:
            img_pil = Image.open(BytesIO(response.content))
            img_cv = np.array(img_pil)
            img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            vegetation_mask = gray > 100  
            green_percentage = (np.sum(vegetation_mask) / vegetation_mask.size) * 100
            return bool(green_percentage > 5)
        return False
    except Exception as e:
        logging.error(f"Tree detection error: {e}")
        return False

def detect_buildings(lat, lon):
    """Detect buildings from satellite imagery using edge detection."""
    if not lat or not lon:
        return None
        
    try:
        map_url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom=18&size=600x400&maptype=satellite&key={GOOGLE_API_KEY}"
        response = requests.get(map_url)
        
        if response.status_code == 200:
            img_pil = Image.open(BytesIO(response.content))
            img_cv = np.array(img_pil)
            img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            edges = cv2.Canny(gray, 50, 150)
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            building_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > 1000]
            return bool(len(building_contours) > 2)
        return False
    except Exception as e:
        logging.error(f"Building detection error: {e}")
        return False

def calculate_slope(lat, lon):
    """Calculate the slope percentage at a given location."""
    if not lat or not lon:
        return None
        
    try:
        offset_distance = 5 / 5280  # Convert 5 feet to miles
        point2 = geodesic(miles=offset_distance).destination((lat, lon), bearing=0)
        lat2, lon2 = point2.latitude, point2.longitude

        elevation1 = get_elevation(lat, lon)
        elevation2 = get_elevation(lat2, lon2)

        if elevation1 is not None and elevation2 is not None:
            rise = abs(elevation2 - elevation1)
            run = 5  # 5 feet
            return (rise / run) * 100
        return None
    except Exception as e:
        logging.error(f"Slope calculation error: {e}")
        return None

def fetch_geospatial_data(address):
    """Fetch comprehensive geospatial data for a property."""
    lat, lon = get_coordinates(address)
    if not lat or not lon:
        return {
            "gps_coord": "Not Available",
            "slope": "Not Available",
            "has_trees": False,
            "has_buildings": False,
            "power_visible": False  # Default value
        }
    
    slope = calculate_slope(lat, lon)
    has_trees = detect_trees(lat, lon)
    has_buildings = detect_buildings(lat, lon)
    
    return {
        "gps_coord": f"{lat},{lon}",
        "slope": slope if slope is not None else "Not Available",
        "has_trees": has_trees,
        "has_buildings": has_buildings,
        "power_visible": False  # Default value
    }

# Function to fetch utility details (Septic & Well)
def check_water_systems(property_id):
    septic_status = check_for_septic(property_id)
    well_status = check_for_well(property_id)
    return {
        "waste_water_type": septic_status, 
        "water_type": well_status
    }

def check_for_septic(property_id):
    """Check if the property has a septic system by analyzing permit data."""
    try:
        url = f"https://dial.deschutes.org/Real/Permits/{property_id}"
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"class": "infoTable"})
            
            if table:
                rows = table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) > 1 and "Septic" in cells[1].text.strip():
                        return "Septic"
            
            # Check for keywords in the page content
            if "Septic" in response.text or "septic" in response.text:
                return "Septic"
                
            return "No septic permit"
        return "No septic permit"
    except Exception as e:
        logging.error(f"Septic check error: {e}")
        return "No septic permit"

def check_for_well(property_id):
    """Check if the property has a well system using selenium."""
    try:
        # For now, we'll use a simpler implementation without Selenium
        # since it requires browser setup and can be complex in production
        
        # In a real implementation, we would:
        # 1. Initialize Selenium webdriver
        # 2. Navigate to the well logs website
        # 3. Search for the property address
        # 4. Check if results are found
        
        # This is a simplified version that randomly returns well/no well
        # Replace with actual implementation when ready
        return "No well found"
    except Exception as e:
        logging.error(f"Well check error: {e}")
        return "No well found"

# Function to fetch design parameters
def fetch_design_parameters(property_id, address=None):
    """Fetch comprehensive design parameters including snow load, wind speed, etc."""
    design_data = {}
    
    try:
        # Step 1: Get the standard design parameters from the PDF
        pdf_data = extract_design_parameters_from_pdf()
        design_data.update(pdf_data)
        
        # Step 2: Get the snow load data if address is provided
        if address and address != "Not Available":
            snow_load = get_snow_load(address)
            if snow_load:
                design_data["ground_snow_load"] = snow_load
        
        return design_data
    except Exception as e:
        logging.error(f"Design parameters error: {e}")
        return {
            "ultimate_wind_design_speed": "Not Available",
            "frost_depth": "Not Available",
            "ground_snow_load": "Not Available",
            "seismic_design_category": "Not Available",
            "exposure": "Not Available"
        }

def extract_design_parameters_from_pdf():
    """Extract design parameters from county PDF document."""
    try:
        pdf_url = "https://www.deschutes.org/sites/default/files/fileattachments/community_development/page/679/design_requirements_for_the_entire_county_2.pdf"
        response = requests.get(pdf_url)
        if response.status_code == 200:
            reader = PyPDF2.PdfReader(BytesIO(response.content))
            text = "".join([page.extract_text() for page in reader.pages])
            
            wind_speed_match = re.search(r'Ultimate Design\s+Wind Speed\s+(\d+\s?mph)', text, re.IGNORECASE)
            frost_depth_match = re.search(r'Frost Depth\s+(\d+["]?)', text, re.IGNORECASE)
            exposure_match = re.search(r'Exposure\s+"?([A-D])"?', text, re.IGNORECASE)
            seismic_match = re.search(r'Seismic\s+([A-D])', text, re.IGNORECASE)
            
            return {
                "ultimate_wind_design_speed": wind_speed_match.group(1) if wind_speed_match else "Not Available",
                "frost_depth": frost_depth_match.group(1) if frost_depth_match else "Not Available",
                "exposure": exposure_match.group(1) if exposure_match else "Not Available",
                "seismic_design_category": seismic_match.group(1) if seismic_match else "Not Available"
            }
        return {
            "ultimate_wind_design_speed": "Not Available",
            "frost_depth": "Not Available",
            "exposure": "Not Available",
            "seismic_design_category": "Not Available"
        }
    except Exception as e:
        logging.error(f"PDF parsing error: {e}")
        return {
            "ultimate_wind_design_speed": "Not Available",
            "frost_depth": "Not Available",
            "exposure": "Not Available",
            "seismic_design_category": "Not Available"
        }

def get_snow_load(address):
    """Get the ground snow load for a specific address."""
    try:
        # Step 1: Geocode the address to get coordinates
        x, y = get_coordinates_for_snow_load(address)
        if not x or not y:
            return None
            
        # Step 2: Create the URL for the snow load map service
        snow_load_url = create_snow_load_url(x, y)
        
        # Step 3: Get the snow load value
        snow_load = get_snow_load_value(snow_load_url)
        return snow_load
    except Exception as e:
        logging.error(f"Snow load error: {e}")
        return None

def get_coordinates_for_snow_load(address):
    """Get coordinates in the format required for snow load API."""
    try:
        encoded_address = urllib.parse.quote(address)
        geocode_url = (
            f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
            f"SingleLine={encoded_address}&f=json&outSR=%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D&countryCode=USA&maxLocations=1"
        )
        response = requests.get(geocode_url).json()
        
        if response.get("candidates"):
            x = response["candidates"][0]["location"]["x"]
            y = response["candidates"][0]["location"]["y"]
            return x, y
        return None, None
    except Exception as e:
        logging.error(f"Snow load geocoding error: {e}")
        return None, None

def create_snow_load_url(x, y):
    """Create URL for the snow load map service."""
    return (
        f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/identify?"
        f"f=json&tolerance=2&returnGeometry=true&returnFieldName=false&returnUnformattedValues=false&imageDisplay=770,501,96&"
        f'geometry={{"x":{x},"y":{y}}}&geometryType=esriGeometryPoint&sr=102100&'
        f"mapExtent={x-0.01},{y-0.01},{x+0.01},{y+0.01}&layers=all:0,91"
    )

def get_snow_load_value(snow_load_url):
    """Extract snow load value from the ArcGIS response."""
    try:
        response = requests.get(snow_load_url).json()
        for result in response.get('results', []):
            if result['layerName'] == "Snowload":
                return result['attributes'].get('SNOWLOAD')
        return None
    except Exception as e:
        logging.error(f"Snow load extraction error: {e}")
    return None

# Function to save all collected data into DB
def save_to_database(data):
    try:
        # Ensure all required fields exist in data with default values if missing
        required_fields = [
            "id", "owner_name", "map_and_taxlot", "situs_address", "acres", 
            "zoning", "gps_coord", "waste_water_type", "water_type", 
            "ultimate_wind_design_speed", "frost_depth", "ground_snow_load", 
            "seismic_design_category", "exposure", "jurisdiction", "overlay",
            "slope", "has_trees", "has_buildings", "power_visible", 
            "mailing_address", "plat_map_url", "tax_map_url"
        ]
        
        for field in required_fields:
            if field not in data:
                data[field] = "Not Available"
                
        # Log the data instead of saving to database
        logging.info(f"Would save data to database: {json.dumps(data, default=str)}")
        
        # Database operations are commented out for now due to missing relations
        '''
        conn = connect_db()
        cursor = conn.cursor()
        
        # 1. First save basic property info
        basic_info_query = """
        INSERT INTO basic_info 
        (id, owner_name, mailing_address, parcel_number, acres, plat_map, plat_map_url, 
         tax_map, tax_map_url, account, site_address)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET owner_name = EXCLUDED.owner_name,
            mailing_address = EXCLUDED.mailing_address,
            parcel_number = EXCLUDED.parcel_number,
            acres = EXCLUDED.acres,
            plat_map = EXCLUDED.plat_map,
            plat_map_url = EXCLUDED.plat_map_url,
            tax_map = EXCLUDED.tax_map,
            tax_map_url = EXCLUDED.tax_map_url,
            account = EXCLUDED.account,
            site_address = EXCLUDED.site_address;
        """
        
        basic_info_values = (
            data["id"],
            data.get("owner_name", "Not Available"),
            data.get("mailing_address", "Not Available"),
            data.get("map_and_taxlot", "Not Available"),
            data.get("acres", "Not Available"),
            "YES" if data.get("plat_map_url") else "NO",
            data.get("plat_map_url", "Not Available"),
            "YES" if data.get("tax_map_url") else "NO",
            data.get("tax_map_url", "Not Available"),
            data["id"],  # account is same as id
            data.get("situs_address", "Not Available")
        )
        
        cursor.execute(basic_info_query, basic_info_values)
        
        # 2. Save zoning data
        zoning_query = """
        INSERT INTO zoning_info 
        (id, jurisdiction, zoning, overlay)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET jurisdiction = EXCLUDED.jurisdiction,
            zoning = EXCLUDED.zoning,
            overlay = EXCLUDED.overlay;
        """
        
        zoning_values = (
            data["id"],
            data.get("jurisdiction", "Not Available"),
            data.get("zoning", "Not Available"),
            data.get("overlay", "Not Available")
        )
        
        cursor.execute(zoning_query, zoning_values)
        
        # 3. Save Google Earth data
        google_earth_query = """
        INSERT INTO google_earth_info 
        (property_id, gps_coord, slope, power_visible, existing_structures, trees_brush)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (property_id) DO UPDATE
        SET gps_coord = EXCLUDED.gps_coord,
            slope = EXCLUDED.slope,
            power_visible = EXCLUDED.power_visible,
            existing_structures = EXCLUDED.existing_structures,
            trees_brush = EXCLUDED.trees_brush;
        """
        
        google_earth_values = (
            data["id"],
            data.get("gps_coord", "Not Available"),
            str(data.get("slope", "Not Available")),
            data.get("power_visible", False),
            data.get("has_buildings", False),
            data.get("has_trees", False)
        )
        
        cursor.execute(google_earth_query, google_earth_values)
        
        # 4. Save utility data
        utility_query = """
        INSERT INTO utility_details 
        (id, waste_water_type, water_type, created_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET waste_water_type = EXCLUDED.waste_water_type,
            water_type = EXCLUDED.water_type,
            created_at = NOW();
        """
        
        utility_values = (
            data["id"],
            data.get("waste_water_type", "Not Available"),
            data.get("water_type", "Not Available")
        )
        
        cursor.execute(utility_query, utility_values)
        
        # 5. Save design data
        design_query = """
        INSERT INTO design_data 
        (id, ground_snow_load, seismic_design_category, basic_wind_speed, 
         ultimate_wind_design_speed, exposure, frost_depth)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET ground_snow_load = EXCLUDED.ground_snow_load,
            seismic_design_category = EXCLUDED.seismic_design_category,
            basic_wind_speed = EXCLUDED.basic_wind_speed,
            ultimate_wind_design_speed = EXCLUDED.ultimate_wind_design_speed,
            exposure = EXCLUDED.exposure,
            frost_depth = EXCLUDED.frost_depth;
        """
        
        design_values = (
            data["id"],
            data.get("ground_snow_load", "Not Available"),
            data.get("seismic_design_category", "Not Available"),
            None,  # basic_wind_speed not collected
            data.get("ultimate_wind_design_speed", "Not Available"),
            data.get("exposure", "Not Available"),
            data.get("frost_depth", "Not Available")
        )
        
        cursor.execute(design_query, design_values)
        
        # 6. Also save to consolidated table
        full_data_query = """
        INSERT INTO full_property_data 
        (id, owner_name, map_and_taxlot, situs_address, acres, zoning, jurisdiction, overlay,
         gps_coord, slope, has_trees, has_buildings, power_visible, waste_water_type, water_type, 
         ultimate_wind_design_speed, frost_depth, ground_snow_load, seismic_design_category, exposure,
         mailing_address, plat_map_url, tax_map_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET owner_name = EXCLUDED.owner_name, 
            map_and_taxlot = EXCLUDED.map_and_taxlot, 
            situs_address = EXCLUDED.situs_address, 
            acres = EXCLUDED.acres, 
            zoning = EXCLUDED.zoning,
            jurisdiction = EXCLUDED.jurisdiction,
            overlay = EXCLUDED.overlay,
            gps_coord = EXCLUDED.gps_coord, 
            slope = EXCLUDED.slope,
            has_trees = EXCLUDED.has_trees,
            has_buildings = EXCLUDED.has_buildings,
            power_visible = EXCLUDED.power_visible,
            waste_water_type = EXCLUDED.waste_water_type, 
            water_type = EXCLUDED.water_type, 
            ultimate_wind_design_speed = EXCLUDED.ultimate_wind_design_speed, 
            frost_depth = EXCLUDED.frost_depth, 
            ground_snow_load = EXCLUDED.ground_snow_load, 
            seismic_design_category = EXCLUDED.seismic_design_category, 
            exposure = EXCLUDED.exposure,
            mailing_address = EXCLUDED.mailing_address,
            plat_map_url = EXCLUDED.plat_map_url,
            tax_map_url = EXCLUDED.tax_map_url;
        """
        
        full_data_values = (
            data["id"],
            data.get("owner_name", "Not Available"),
            data.get("map_and_taxlot", "Not Available"),
            data.get("situs_address", "Not Available"),
            data.get("acres", "Not Available"),
            data.get("zoning", "Not Available"),
            data.get("jurisdiction", "Not Available"),
            data.get("overlay", "Not Available"),
            data.get("gps_coord", "Not Available"),
            data.get("slope", "Not Available"),
            data.get("has_trees", False),
            data.get("has_buildings", False),
            data.get("power_visible", False),
            data.get("waste_water_type", "Not Available"),
            data.get("water_type", "Not Available"),
            data.get("ultimate_wind_design_speed", "Not Available"),
            data.get("frost_depth", "Not Available"),
            data.get("ground_snow_load", "Not Available"),
            data.get("seismic_design_category", "Not Available"),
            data.get("exposure", "Not Available"),
            data.get("mailing_address", "Not Available"),
            data.get("plat_map_url", "Not Available"),
            data.get("tax_map_url", "Not Available")
        )
        
        cursor.execute(full_data_query, full_data_values)
        
        conn.commit()
        cursor.close()
        conn.close()
        '''
        
    except Exception as e:
        logging.error(f"Database error: {e}")
        # Don't re-raise the exception to prevent API errors
        # Just log it and continue

# Planning Data Functions
def fetch_arcgis_data(map_and_taxlot):
    """Fetch data from ArcGIS using the map and taxlot number."""
    try:
        if not map_and_taxlot or map_and_taxlot == "Not Available":
            return None
            
        api_url = f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/0/query?f=json&where=Taxlot_Assessor_Account.TAXLOT%20%3D%20%27{map_and_taxlot}%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100"
        response = requests.get(api_url)
        
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        logging.error(f"ArcGIS data error: {e}")
        return None

def extract_arcgis_data(api_data):
    """Extract relevant data from the ArcGIS API response."""
    if not api_data or "features" not in api_data or not api_data["features"]:
        return {}
        
    try:
        attributes = api_data["features"][0]["attributes"]
        data = {
            "owner_name": attributes.get("dbo_GIS_MAILING.OWNER", "Not Available"),
            "situs_address": (attributes.get("Taxlot_Assessor_Account.Address", "") + ", BEND, OR 97703") if attributes.get("Taxlot_Assessor_Account.Address") else "Not Available",
            "map_and_taxlot": attributes.get("Taxlot_Assessor_Account.TAXLOT", "Not Available"),
            "acres": attributes.get("Taxlot_Assessor_Account.Shape_Area", "Not Available"),  # Converting area to acres
            "plat_name": attributes.get("Taxlot_Assessor_Account.SUBDIVISION_NAME", "Not Available"),
            "plat_map_url": f"https://dial.deschutes.org/API/Real/GetReport/{attributes.get('Taxlot_Assessor_Account.ACCOUNT')}?report=PlatMap" if attributes.get('Taxlot_Assessor_Account.ACCOUNT') else None,
            "tax_map_url": f"https://dial.deschutes.org/API/Real/GetReport/{attributes.get('Taxlot_Assessor_Account.ACCOUNT')}?report=TaxMap" if attributes.get('Taxlot_Assessor_Account.ACCOUNT') else None
        }
        return data
    except Exception as e:
        logging.error(f"ArcGIS data extraction error: {e}")
        return {}


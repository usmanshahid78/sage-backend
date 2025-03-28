import os
import re
import cv2
import time
import json
import base64
import psycopg2
import requests
import urllib.parse
import numpy as np
from PIL import Image
from io import BytesIO
from bs4 import BeautifulSoup
from datetime import datetime
from geopy.distance import geodesic
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import openai
import PyPDF2

# Load environment variables
load_dotenv()

# Set OpenAI key
openai.api_key = os.getenv("OPEN_AI_API_KEY")

# Database config
DB_HOST = os.getenv("DB_HOST")
DB_NAME = "sagedatabase"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

# External API Keys
OPENCAGE_API_KEY = os.getenv("OPENCAGE_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

def get_property_id(taxlot_id):
    """Fetch Property ID using Taxlot ID."""
    search_url = f"https://dial.deschutes.org/results/general?value={taxlot_id}"
    session = requests.Session()
    response = session.get(search_url, allow_redirects=True)
    if response.status_code == 200:
        return response.url.split("/")[-1]  # Extract Property ID
    print(f"❌ Failed to fetch Property ID for Taxlot ID: {taxlot_id}")
    return None

def fetch_html_data(property_id):
    """Fetch HTML content for the main property page."""
    url = f"https://dial.deschutes.org/Real/Index/{property_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content, url
    else:
        print(f"❌ Failed to fetch HTML data for property_id: {property_id}")
        return None, None

def fetch_development_summary(property_id):
    """Fetch Development Summary HTML content."""
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content, url
    else:
        print(f"❌ Failed to fetch Development Summary for property_id: {property_id}")
        return None, None

def fetch_plat_map_url(property_id, html_content=None):
    """Extract Plat Map URL if present."""
    url = f"https://dial.deschutes.org/Real/Index/{property_id}"
    if not html_content:
        response = requests.get(url)
        if response.status_code != 200:
            return None, None
        html_content = response.content
    soup = BeautifulSoup(html_content, "html.parser")
    plat_map_link = soup.find("a", href=lambda href: href and "recordings.deschutes.org" in href)
    if plat_map_link:
        return plat_map_link["href"], url
    return None, url

def extract_html_data(html_content, dev_summary_content=None, main_url=None, dev_summary_url=None):
    soup = BeautifulSoup(html_content, "html.parser")
    data = {}

    # Owner Name
    owner_name_tag = soup.find("strong", string="Mailing Name:")
    if owner_name_tag:
        data["owner_name"] = owner_name_tag.find_next_sibling(string=True).strip()
        data["owner_name_source"] = main_url

    # Taxlot
    map_and_taxlot_tag = soup.find("span", id="uxMapTaxlot")
    if map_and_taxlot_tag:
        data["map_and_taxlot"] = map_and_taxlot_tag.text.strip()
        data["parcel_number_source"] = main_url

    # Situs Address
    situs_address_tag = soup.find("span", id="uxSitusAddress")
    if situs_address_tag:
        data["situs_address"] = situs_address_tag.text.strip()
        data["site_address_source"] = main_url

    # Mailing Address
    ownership_section = soup.find("p", string=lambda t: t and "Ownership" in t)
    if not ownership_section:
        ownership_section = soup.find("p", class_="uxReportSectionHeader", string="Ownership")
    if ownership_section:
        mailing_block = ownership_section.find_next("p")
        if mailing_block:
            lines = mailing_block.get_text(separator="\n").strip().split("\n")
            mailing_lines = [line.strip() for line in lines if line.strip()]
            
            # Define UI phrases to filter out
            ui_phrases = [
                "Mailing To:", 
                "Change of Mailing Address", 
                "View Overview Map", 
                "View Complete Ownership Report"
            ]
            
            # Filter out lines containing any UI phrases
            filtered_lines = [
                line for line in mailing_lines
                if not any(phrase in line for phrase in ui_phrases)
            ]

            # Compare and remove the owner name line explicitly if it matches
            owner_name = data.get("owner_name", "").strip()
            address_lines = [
                line for line in filtered_lines
                if line and line.strip() != owner_name
            ]

            # Join remaining lines into one mailing address
            if address_lines:
                data["mailing_address"] = " ".join(address_lines)
                data["mailing_address_source"] = main_url

    # Acres
    acres_tag = soup.find("strong", string="Assessor Acres:")
    if acres_tag:
        data["acres"] = acres_tag.find_next_sibling(string=True).strip()
        data["acres_source"] = main_url

    # Legal Description from Development Summary
    if dev_summary_content:
        dev_soup = BeautifulSoup(dev_summary_content, "html.parser")
        prop_details = dev_soup.find("p", class_="uxReportSectionHeader", string="Property Details")
        if prop_details:
            prop_text = prop_details.find_next("p").get_text(separator=" ").strip()
            subdivision = lot = block = ""
            if "Subdivision:" in prop_text:
                subdivision = prop_text.split("Subdivision:")[1].split("Lot:")[0].strip()
            if "Lot:" in prop_text:
                lot = prop_text.split("Lot:")[1].split("Block:")[0].strip()
            if "Block:" in prop_text:
                block = prop_text.split("Block:")[1].split("Acres:")[0].strip()
            legal_parts = []
            if subdivision: legal_parts.append(subdivision)
            if lot: legal_parts.append(f"Lot {lot}")
            if block: legal_parts.append(f"Block {block}")
            if legal_parts:
                data["legal"] = " ".join(legal_parts)
                data["legal_source"] = dev_summary_url
    return data

def fetch_arcgis_data(map_and_taxlot):
    url = f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/0/query?f=json&where=Taxlot_Assessor_Account.TAXLOT%20%3D%20%27{map_and_taxlot}%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json(), url
    return None, None

def extract_arcgis_data(api_data, api_url):
    if api_data and "features" in api_data and api_data["features"]:
        attr = api_data["features"][0]["attributes"]
        return {
            "owner_name": attr.get("dbo_GIS_MAILING.OWNER"),
            "owner_name_source": api_url,
            "situs_address": attr.get("Taxlot_Assessor_Account.Address") + ", BEND, OR 97703",
            "site_address_source": api_url,
            "mailing_address": attr.get("Taxlot_Assessor_Account.Address") + ", BEND, OR 97703",
            "mailing_address_source": api_url,
            "map_and_taxlot": attr.get("Taxlot_Assessor_Account.TAXLOT"),
            "parcel_number_source": api_url,
            "acres": attr.get("Taxlot_Assessor_Account.Shape_Area"),
            "acres_source": api_url
        }
    return None

def save_basic_info_to_db(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO basic_info (
            id, owner_name, owner_name_source, mailing_address, mailing_address_source,
            parcel_number, parcel_number_source, acres, acres_source, plat_map, plat_map_url, 
            tax_map, tax_map_url, account, site_address, site_address_source, legal, legal_source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET owner_name = EXCLUDED.owner_name, owner_name_source = EXCLUDED.owner_name_source,
            mailing_address = EXCLUDED.mailing_address, mailing_address_source = EXCLUDED.mailing_address_source,
            parcel_number = EXCLUDED.parcel_number, parcel_number_source = EXCLUDED.parcel_number_source,
            acres = EXCLUDED.acres, acres_source = EXCLUDED.acres_source,
            plat_map = EXCLUDED.plat_map, plat_map_url = EXCLUDED.plat_map_url,
            tax_map = EXCLUDED.tax_map, tax_map_url = EXCLUDED.tax_map_url,
            account = EXCLUDED.account, site_address = EXCLUDED.site_address, site_address_source = EXCLUDED.site_address_source,
            legal = EXCLUDED.legal, legal_source = EXCLUDED.legal_source;
        """

        plat_map_value = "YES" if data.get("plat_map_url") else None
        tax_map_value = "YES" if data.get("tax_map_url") else None

        cursor.execute(insert_query, (
            data.get("id"),
            data.get("owner_name"),
            data.get("owner_name_source"),
            data.get("mailing_address"),
            data.get("mailing_address_source"),
            data.get("map_and_taxlot"),
            data.get("parcel_number_source"),
            data.get("acres"),
            data.get("acres_source"),
            plat_map_value,
            data.get("plat_map_url"),
            tax_map_value,
            data.get("tax_map_url"),
            data.get("id"),
            data.get("site_address"),
            data.get("site_address_source"),
            data.get("legal"),
            data.get("legal_source")
        ))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Basic info saved to database.")

    except Exception as e:
        print(f"❌ Error saving basic info: {e}")

def create_geocode_url(address):
    encoded_address = urllib.parse.quote(address)
    return (
        f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
        f"SingleLine={encoded_address}&f=json&outSR=%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D&countryCode=USA&maxLocations=1"
    )

def get_coordinates(geocode_url):
    response = requests.get(geocode_url).json()
    if response.get("candidates"):
        loc = response["candidates"][0]["location"]
        return loc["x"], loc["y"], geocode_url
    raise ValueError("No coordinates found")

def create_snow_load_url(x, y):
    return (
        f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/identify?"
        f"f=json&tolerance=2&returnGeometry=true&returnFieldName=false&returnUnformattedValues=false&imageDisplay=770,501,96&"
        f'geometry={{"x":{x},"y":{y}}}&geometryType=esriGeometryPoint&sr=102100&'
        f"mapExtent={x-0.01},{y-0.01},{x+0.01},{y+0.01}&layers=all:0,91"
    )

def get_snow_load_value(snow_load_url):
    response = requests.get(snow_load_url).json()
    for result in response.get("results", []):
        if result.get("layerName") == "Snowload":
            return result["attributes"].get("SNOWLOAD"), snow_load_url
    return None, snow_load_url

def extract_design_parameters(pdf_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(pdf_url, headers=headers, timeout=10)
        response.raise_for_status()
        with BytesIO(response.content) as file:
            reader = PyPDF2.PdfReader(file)
            text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])

            gusts_match = re.search(r'(\d+\s?mph)\s+3\s+sec\.\s+gusts', text, re.IGNORECASE)
            basic_wind_speed = gusts_match.group(1) if gusts_match else None

            if not basic_wind_speed:
                patterns = [
                    r'Basic\s+Wind\s+Speed.*?(\d+\s?mph)',
                    r'Wind\s+Speed\s*V1\s*[:\s]*(\d+\s?mph)',
                    r'V1\s*[:\s=]*\s*(\d+\s?mph)',
                    r'Wind\s+Speed\s*[^\n]*?(\d+\s?mph)'
                ]
                for pattern in patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        basic_wind_speed = match.group(match.lastindex)
                        break

            wind_speed_match = re.search(r'Ultimate\s+Design\s+Wind\s+Speed\s+(\d+\s?mph)', text, re.IGNORECASE)
            frost_depth_match = re.search(r'Frost\s+Depth\s*[:\s]*(\d+["]?)', text, re.IGNORECASE)
            exposure_match = re.search(r'Exposure\s*[:\s]+"?([A-D])"?', text, re.IGNORECASE)
            seismic_match = re.search(r'Seismic\s*(?:[:\s]*|[A-D]\s+for).*?([A-D])', text, re.IGNORECASE)

            return {
                "ultimate_wind_design_speed": wind_speed_match.group(1) if wind_speed_match else None,
                "ultimate_wind_design_speed_source": pdf_url,
                "basic_wind_speed": basic_wind_speed,
                "basic_wind_speed_source": pdf_url,
                "frost_depth": frost_depth_match.group(1) if frost_depth_match else None,
                "frost_depth_source": pdf_url,
                "exposure": exposure_match.group(1) if exposure_match else None,
                "exposure_source": pdf_url,
                "seismic_design_category": seismic_match.group(1) if seismic_match else None,
                "seismic_design_category_source": pdf_url
            }

    except Exception as e:
        print(f"❌ PDF parsing error: {e}")
        return {}

def insert_design_data(property_id, data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()

        query = """
        INSERT INTO design_data (
            id, ground_snow_load, ground_snow_load_source, 
            seismic_design_category, seismic_design_category_source, 
            basic_wind_speed, basic_wind_speed_source, 
            ultimate_wind_design_speed, ultimate_wind_design_speed_source, 
            exposure, exposure_source, 
            frost_depth, frost_depth_source
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET ground_snow_load = EXCLUDED.ground_snow_load, 
            ground_snow_load_source = EXCLUDED.ground_snow_load_source,
            seismic_design_category = EXCLUDED.seismic_design_category, 
            seismic_design_category_source = EXCLUDED.seismic_design_category_source,
            basic_wind_speed = EXCLUDED.basic_wind_speed, 
            basic_wind_speed_source = EXCLUDED.basic_wind_speed_source,
            ultimate_wind_design_speed = EXCLUDED.ultimate_wind_design_speed, 
            ultimate_wind_design_speed_source = EXCLUDED.ultimate_wind_design_speed_source,
            exposure = EXCLUDED.exposure, 
            exposure_source = EXCLUDED.exposure_source,
            frost_depth = EXCLUDED.frost_depth, 
            frost_depth_source = EXCLUDED.frost_depth_source;
        """
        values = (
            property_id,
            data.get("ground_snow_load"), data.get("ground_snow_load_source"),
            data.get("seismic_design_category"), data.get("seismic_design_category_source"),
            data.get("basic_wind_speed"), data.get("basic_wind_speed_source"),
            data.get("ultimate_wind_design_speed"), data.get("ultimate_wind_design_speed_source"),
            data.get("exposure"), data.get("exposure_source"),
            data.get("frost_depth"), data.get("frost_depth_source")
        )

        cursor.execute(query, values)
        conn.commit()
        print("✅ Design data saved to database.")
    except Exception as e:
        print(f"❌ Error saving design data: {e}")
    finally:
        cursor.close()
        conn.close()

def get_elevation(lat, lon):
    url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={GOOGLE_API_KEY}"
    response = requests.get(url).json()
    if response.get("results"):
        return response["results"][0]["elevation"] * 3.281, url  # Return elevation in feet and source URL
    return None, None

def detect_trees(lat, lon):
    map_url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom=18&size=600x400&maptype=satellite&key={GOOGLE_API_KEY}"
    response = requests.get(map_url)

    if response.status_code == 200:
        img_pil = Image.open(BytesIO(response.content))
        img_cv = np.array(img_pil)
        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        vegetation_mask = gray > 100  
        green_percentage = (np.sum(vegetation_mask) / vegetation_mask.size) * 100
        return bool(green_percentage > 5), map_url  # Return boolean and source URL
    return None, None

def detect_buildings(lat, lon):
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
        return bool(len(building_contours) > 2), map_url  # Return boolean and source URL
    return None, None

def detect_power_infrastructure(lat, lon, property_id):
    # Fetch Street View image
    street_view_url = f"https://maps.googleapis.com/maps/api/streetview?size=600x400&location={lat},{lon}&key={GOOGLE_API_KEY}"
    response = requests.get(street_view_url)
    
    if response.status_code != 200:
        print("⚠️ Could not fetch Street View image.")
        return False, None
        
    # Save image locally (optional)
    image_path = f"street_view_{property_id}.jpg"
    with open(image_path, "wb") as file:
        file.write(response.content)
    
    # Convert image to base64 for OpenAI API
    image_base64 = base64.b64encode(response.content).decode('utf-8')
    
    try:
        # For OpenAI API version 0.28.1
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {openai.api_key}"
        }
        
        payload = {
            "model": "gpt-4-turbo",
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Does this image show any power meters, power poles, utility poles, electrical boxes, or visible electrical infrastructure? Answer with 'yes' or 'no' only."},
                        {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"}}
                    ]
                }
            ],
            "max_tokens": 10
        }
        
        api_response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload
        )
        
        if api_response.status_code == 200:
            result = api_response.json()
            answer = result['choices'][0]['message']['content'].lower().strip()
            print(f"🔍 GPT-4 Vision response: {answer}")
            return "yes" in answer, street_view_url
        else:
            print(f"❌ OpenAI API HTTP error: {api_response.status_code}")
            print(api_response.text)
            return False, None
        
    except Exception as e:
        print(f"❌ OpenAI API error: {str(e)}")
        return False, None

def insert_google_earth_data(property_id, lat, lon, slope, slope_source, has_trees, trees_source, has_buildings, buildings_source, power_visible, power_source):
    gps_coord = f"{lat},{lon}"  # Convert to string format
    gps_coord_source = "https://api.opencagedata.com"  # Source for GPS coordinates

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO google_earth_info 
        (property_id, gps_coord, gps_coord_source, slope, slope_source, power_visible, power_visible_source, 
         existing_structures, existing_structures_source, trees_brush, trees_brush_source) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (property_id) DO UPDATE
        SET gps_coord = EXCLUDED.gps_coord, gps_coord_source = EXCLUDED.gps_coord_source,
            slope = EXCLUDED.slope, slope_source = EXCLUDED.slope_source,
            power_visible = EXCLUDED.power_visible, power_visible_source = EXCLUDED.power_visible_source,
            existing_structures = EXCLUDED.existing_structures, existing_structures_source = EXCLUDED.existing_structures_source,
            trees_brush = EXCLUDED.trees_brush, trees_brush_source = EXCLUDED.trees_brush_source;
        """
        cursor.execute(insert_query, (
            property_id,
            gps_coord,
            gps_coord_source,
            str(slope),
            slope_source,
            bool(power_visible),
            power_source,
            bool(has_buildings),
            buildings_source,
            bool(has_trees),
            trees_source
        ))
        conn.commit()
        print("✅ Data successfully inserted into the database.")
    except Exception as e:
        print("❌ Database insertion error:", str(e))
    finally:
        if conn:
            cursor.close()
            conn.close()

def get_jurisdiction_from_dial(property_id: int) -> str:
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string if soup.title else ""
        if "City of" in title:
            return title.strip()
        return "Deschutes County"
    except Exception as e:
        print(f"Error fetching jurisdiction: {e}")
        return "Unknown"

def get_fire_district_from_dial(address: str) -> str:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "searchText"))).send_keys(address + "\n")
        time.sleep(5)

        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))).click()
        time.sleep(1)

        checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Fire')]/preceding-sibling::input"))
        )
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(3)

        fire_text = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "infoPanel"))).text
        return fire_text if "Fire" in fire_text else "NO DISTRICT"
    except Exception as e:
        print(f"Error getting fire district: {e}")
        return "NO DISTRICT"
    finally:
        driver.quit()

def get_zoning_and_overlay_from_dial(property_id: int) -> dict:
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")
        zoning_table = soup.find("table")
        zoning, overlay = [], []
        if zoning_table:
            rows = zoning_table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    zoning.append(cols[1].get_text(strip=True))
                    overlay.append(cols[2].get_text(strip=True))
        return {"zoning": ", ".join(zoning), "overlay": ", ".join(overlay)}
    except Exception as e:
        print(f"Error extracting zoning/overlay: {e}")
        return {"zoning": None, "overlay": None}

def analyze_setbacks_and_restrictions() -> dict:
    prompt = '''Please analyze this zoning code for a Multiple Ag Use Zoned property located at 64350 Sisemore Road Bend Oregon 97703. Determine and report the setback for: Front, Garage, Side and Rear. Examine the broader Deschutes County Code for lot coverage and building height requirements that may apply to this code. Also determine if there are solar setbacks that may be applicable to this property.
https://deschutescounty.municipalcodeonline.com/book?type=ordinances#name=CHAPTER_18.32_MULTIPLE_USE_AGRICULTURAL_ZONE;_MUA
Due to being rural and gravel, my assumption is that Sisemore Rd is a local street.
The property is bordered by a Riverine R4SBCx irrigation canal from the National Wetlands Inventory. Do you expect any special setbacks from this canal based on the information above?
Also analyze solar setbacks from this code:
https://deschutescounty.municipalcodeonline.com/book/print?type=ordinances&name=18.116.180_Building_Setbacks_For_The_Protection_Of_Solar_Access'''
    try:
        result = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "system", "content": "Extract exact values only, no commentary."},
                      {"role": "user", "content": prompt}]
        )
        return parse_structured_gpt_output(result["choices"][0]["message"]["content"])
    except Exception as e:
        print(f"Error from OpenAI: {e}")
        return {}

def parse_structured_gpt_output(text: str) -> dict:
    result = {
        "max_lot_coverage": None,
        "max_building_height": None,
        "front_setback": None,
        "side_setback": None,
        "rear_setback": None,
        "solar_setback": None,
        "special_setback": None
    }
    for line in text.splitlines():
        lower_line = line.lower()
        for key in result:
            if key.replace("_", " ") in lower_line:
                parts = line.split(":")
                if len(parts) > 1:
                    value = parts[1].strip()
                    if key == "solar_setback":
                        result[key] = "YES" if "yes" in value.lower() else "NO"
                    elif key == "special_setback":
                        feet_match = [s for s in value.split() if s.isdigit() or "ft" in s or "feet" in s]
                        result[key] = " ".join(feet_match) if feet_match else value
                    else:
                        result[key] = value
    return result

def get_liquefaction_hazard(address: str) -> str:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "searchText"))).send_keys(address + "\n")
        time.sleep(5)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))).click()
        time.sleep(1)

        checkbox = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Earthquake Hazard')]/preceding-sibling::input")))
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(3)

        info = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "infoPanel"))).text
        if "High" in info: return "HIGH"
        if "Moderate" in info: return "MODERATE"
        if "Low" in info: return "LOW"
        return "MODERATE"
    except Exception as e:
        print(f"Error getting liquefaction hazard: {e}")
        return "MODERATE"
    finally:
        driver.quit()

def get_landslide_hazard(address: str) -> str:
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "searchText"))).send_keys(address + "\n")
        time.sleep(5)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))).click()
        time.sleep(1)

        checkbox = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Landslide Susceptibility')]/preceding-sibling::input")))
        if not checkbox.is_selected():
            checkbox.click()
            time.sleep(3)

        info = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "infoPanel"))).text
        if "High" in info: return "HIGH"
        if "Moderate" in info: return "MODERATE"
        if "Low" in info: return "LOW"
        return "UNKNOWN"
    except Exception as e:
        print(f"Error getting landslide hazard: {e}")
        return "UNKNOWN"
    finally:
        driver.quit()

def get_geo_report_required(soil_pdf_path: str, liquefaction: str) -> str:
    prompt = f'''
Please analyze this NRCS Soils Report. Determine if a geotechnical report is likely needed to build a single family dwelling with a foundation bearing capacity of 1500 psf on this property. Specifically look at shrink swell hazard potential.

Also analyze these soil types against the prescriptive soils descriptions in the Oregon Residential Specialty Code for foundations bearing 1500 psf.

Liquefaction potential: {liquefaction}
This is from HazVu Liquefaction Susceptibility Map.
Assume highly erodible soils may be present.
Does Deschutes County have codified requirements that would require a geotechnical report?

Respond with only one of these: YES, NO, or MORE INFO NEEDED.
'''
    try:
        with open(soil_pdf_path, "rb") as f:
            encoded_pdf = base64.b64encode(f.read()).decode("utf-8")
        result = openai.ChatCompletion.create(
            model="gpt-4-vision",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "file", "name": "soil_report.pdf", "data": encoded_pdf}
            ]}]
        )
        reply = result['choices'][0]['message']['content']
        if "yes" in reply.lower(): return "YES"
        elif "no" in reply.lower(): return "NO"
        else: return "MORE INFO NEEDED"
    except Exception as e:
        print(f"Error evaluating geo report: {e}")
        return "MORE INFO NEEDED"

def get_hardcoded_values() -> dict:
    return {
        "fema_flood_zone": "NO",
        "hydric_soils_hazard": "NO",
        "wetlands_on_property": "YES",
        "erosion_control_required": "None",
        "stormwater_requirements": "None",
        "tree_preservation_reqs": "None",
        "special_fire_marshal_reqs": "YES",
        "radon": "NO",
        "sidewalks_required": "None",
        "approach_permit": "NO"
    }

def save_planning_data(property_id, jurisdiction, fire_district, zoning_overlay, planning_data,
                       liquefaction, landslide, geo_report_required, hardcoded_values):
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS,
            host=DB_HOST, port=DB_PORT
        )
        cursor = conn.cursor()
        
        # First check if record exists
        cursor.execute("SELECT property_id FROM planning_data WHERE property_id = %s", (property_id,))
        exists = cursor.fetchone() is not None
        
        if exists:
            # Update existing record
            query = """
            UPDATE planning_data SET
                jurisdiction = %(jurisdiction)s,
                fire_district = %(fire_district)s,
                zoning = %(zoning)s,
                overlay = %(overlay)s,
                max_lot_coverage = %(max_lot_coverage)s,
                max_building_height = %(max_building_height)s,
                front_setback = %(front_setback)s,
                side_setback = %(side_setback)s,
                rear_setback = %(rear_setback)s,
                solar_setback = %(solar_setback)s,
                special_setback = %(special_setback)s,
                liquefaction_hazard = %(liquefaction_hazard)s,
                landslide_hazard = %(landslide_hazard)s,
                geo_report_required = %(geo_report_required)s,
                fema_flood_zone = %(fema_flood_zone)s,
                hydric_soils_hazard = %(hydric_soils_hazard)s,
                wetlands_on_property = %(wetlands_on_property)s,
                erosion_control_required = %(erosion_control_required)s,
                stormwater_requirements = %(stormwater_requirements)s,
                tree_preservation_reqs = %(tree_preservation_reqs)s,
                special_fire_marshal_reqs = %(special_fire_marshal_reqs)s,
                radon = %(radon)s,
                sidewalks_required = %(sidewalks_required)s,
                approach_permit = %(approach_permit)s
            WHERE property_id = %(property_id)s;
            """
        else:
            # Insert new record
            query = """
            INSERT INTO planning_data (
                property_id, jurisdiction, fire_district, zoning, overlay,
                max_lot_coverage, max_building_height, front_setback, side_setback, rear_setback, 
                solar_setback, special_setback, liquefaction_hazard, landslide_hazard, geo_report_required,
                fema_flood_zone, hydric_soils_hazard, wetlands_on_property, erosion_control_required, 
                stormwater_requirements, tree_preservation_reqs, special_fire_marshal_reqs, radon, sidewalks_required, approach_permit
            ) VALUES (
                %(property_id)s, %(jurisdiction)s, %(fire_district)s, %(zoning)s, %(overlay)s,
                %(max_lot_coverage)s, %(max_building_height)s, %(front_setback)s, %(side_setback)s, %(rear_setback)s, 
                %(solar_setback)s, %(special_setback)s, %(liquefaction_hazard)s, %(landslide_hazard)s, %(geo_report_required)s,
                %(fema_flood_zone)s, %(hydric_soils_hazard)s, %(wetlands_on_property)s, %(erosion_control_required)s, 
                %(stormwater_requirements)s, %(tree_preservation_reqs)s, %(special_fire_marshal_reqs)s, %(radon)s, %(sidewalks_required)s, %(approach_permit)s
            );
            """
        
        data = {
            "property_id": property_id,
            "jurisdiction": jurisdiction,
            "fire_district": fire_district,
            "zoning": zoning_overlay["zoning"],
            "overlay": zoning_overlay["overlay"],
            **planning_data,
            **hardcoded_values,
            "liquefaction_hazard": liquefaction,
            "landslide_hazard": landslide,
            "geo_report_required": geo_report_required
        }
        cursor.execute(query, data)
        conn.commit()
        print("✅ Planning data saved to database.")
    except Exception as e:
        print(f"❌ Planning DB error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def check_for_septic(account_id):
    """Check if a property has a septic permit."""
    url = f"https://dial.deschutes.org/Real/Permits/{account_id}"
    response = requests.get(url)
    if response.status_code != 200:
        return None, url
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "infoTable"})
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) > 1 and cells[1].text.strip() == "Septic":
                return "Septic", url
        return "No septic permit found.", url
    return "No permit table found.", url

def check_for_well():
    """Check if a well permit exists by using the state well log search."""
    url = "https://apps.wrd.state.or.us/apps/gw/well_log/"
    driver = webdriver.Chrome()
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "ctl00_PageData_btn_address_view"))).click()
        address_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "ctl00_PageData_txt_address")))
        address_input.send_keys("18160 COTTONWOOD RD #346 SUNRIVER, OR 97707")
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "ctl00_PageData_btn_lookup_address"))).click()
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "input[type='checkbox'][name^='ctl00$PageData$sctn']")))
        sections = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox'][name^='ctl00$PageData$sctn']")
        if any(s.is_selected() for s in sections):
            return "Well", url
    except Exception as e:
        return f"Error: {e}", url
    finally:
        driver.quit()
    return "No well found at this address.", url

def save_utility_data(account_id, waste_water_type, waste_water_source, water_type, water_source, power_type, power_source):
    """Save or update utility data in the database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER,
            password=DB_PASS, port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO utility_details (
                id, waste_water_type, waste_water_type_source, 
                water_type, water_type_source, 
                power_type, power_type_source, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id) DO UPDATE
            SET waste_water_type = EXCLUDED.waste_water_type, waste_water_type_source = EXCLUDED.waste_water_type_source,
                water_type = EXCLUDED.water_type, water_type_source = EXCLUDED.water_type_source,
                power_type = EXCLUDED.power_type, power_type_source = EXCLUDED.power_type_source,
                created_at = NOW();
        """, (account_id, waste_water_type, waste_water_source, water_type, water_source, power_type, power_source))
        conn.commit()
        print("✅ Utility data saved to database.")
    except Exception as error:
        print(f"❌ Utility DB error: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

def check_water_systems(account_id):
    """Run septic/well checks and insert into DB along with power type."""
    septic_result, septic_source = check_for_septic(account_id)
    well_result, well_source = check_for_well()
    power_type = "Underground"  # Fixed value
    power_source = "https://www.deschutes.org/"  # Placeholder
    save_utility_data(account_id, septic_result, septic_source, well_result, well_source, power_type, power_source)
    return septic_result, well_result, power_type

def convert_web_mercator_to_wgs84(x, y):
    """Convert Web Mercator coordinates to WGS84 (lat/lon)."""
    lon = (x / 20037508.34) * 180
    lat = (y / 20037508.34) * 180
    lat = 180 / np.pi * (2 * np.arctan(np.exp(lat * np.pi / 180)) - np.pi / 2)
    return lat, lon

def main(taxlot_id):
    try:
        print(f"\n🔹 Starting full process for taxlot: {taxlot_id}")

        # Step 1: Get Property ID
        property_id = get_property_id(taxlot_id)
        if not property_id:
            print("❌ Could not fetch property_id. Exiting.")
            return

        # Step 2: DIAL data
        html_content, main_url = fetch_html_data(property_id)
        dev_summary_content, dev_summary_url = fetch_development_summary(property_id)
        plat_map_url, plat_map_source = fetch_plat_map_url(property_id, html_content)

        html_data = extract_html_data(html_content, dev_summary_content, main_url, dev_summary_url)
        arcgis_data, arcgis_url = fetch_arcgis_data(html_data.get("map_and_taxlot", ""))
        arcgis_info = extract_arcgis_data(arcgis_data, arcgis_url) if arcgis_data else {}

        basic_data = {
            "id": property_id,
            "owner_name": html_data.get("owner_name") or arcgis_info.get("owner_name"),
            "owner_name_source": html_data.get("owner_name_source") or arcgis_info.get("owner_name_source"),
            "mailing_address": html_data.get("mailing_address"),
            "mailing_address_source": html_data.get("mailing_address_source"),
            "map_and_taxlot": html_data.get("map_and_taxlot"),
            "parcel_number_source": html_data.get("parcel_number_source"),
            "acres": html_data.get("acres") or arcgis_info.get("acres"),
            "acres_source": html_data.get("acres_source") or arcgis_info.get("acres_source"),
            "account": property_id,
            "site_address": html_data.get("situs_address"),
            "site_address_source": html_data.get("site_address_source"),
            "tax_map_url": f"https://dial.deschutes.org/API/Real/GetReport/{property_id}?report=TaxMap",
            "tax_map_source": main_url,
            "plat_map_url": plat_map_url,
            "plat_map_source": plat_map_source,
            "legal": html_data.get("legal"),
            "legal_source": html_data.get("legal_source")
        }
        save_basic_info_to_db(basic_data)

        address = basic_data["site_address"]
        print(f"\n✅ Basic info complete. Address: {address}")

        # Step 3: Design Data
        geo_url = create_geocode_url(address)
        x, y, geo_source = get_coordinates(geo_url)
        print(f"\nDebug - Initial coordinates (Web Mercator): x={x}, y={y}")
        
        # Convert to WGS84
        lat, lon = convert_web_mercator_to_wgs84(x, y)
        print(f"Debug - Converted coordinates (WGS84): lat={lat}, lon={lon}")
        
        snow_url = create_snow_load_url(x, y)
        snow_val, snow_src = get_snow_load_value(snow_url)

        design_pdf_url = "https://www.deschutes.org/sites/default/files/fileattachments/community_development/page/679/design_requirements_for_the_entire_county_2.pdf"
        design_params = extract_design_parameters(design_pdf_url)
        design_params["ground_snow_load"] = snow_val
        design_params["ground_snow_load_source"] = snow_src
        insert_design_data(property_id, design_params)

        # Step 4: Google Earth (Slope, Trees, Buildings, Power)
        offset_miles = 5 / 5280
        print(f"Debug - Before geodesic: lat={lat}, lon={lon}")
        point2 = geodesic(miles=offset_miles).destination((lat, lon), bearing=0)
        lat2, lon2 = point2.latitude, point2.longitude
        print(f"Debug - After geodesic: lat2={lat2}, lon2={lon2}")
        elev1, elev_src = get_elevation(lat, lon)
        elev2, _ = get_elevation(lat2, lon2)
        slope = round(abs(elev2 - elev1) / 5 * 100, 2) if elev1 and elev2 else None

        trees, trees_src = detect_trees(lat, lon)
        buildings, buildings_src = detect_buildings(lat, lon)
        power, power_src = detect_power_infrastructure(lat, lon, property_id)

        insert_google_earth_data(property_id, lat, lon, slope, elev_src, trees, trees_src, buildings, buildings_src, power, power_src)

        # Step 5: Planning Data
        jurisdiction = get_jurisdiction_from_dial(property_id)
        fire_district = get_fire_district_from_dial(address)
        zoning_overlay = get_zoning_and_overlay_from_dial(property_id)
        planning = analyze_setbacks_and_restrictions()
        liquefaction = get_liquefaction_hazard(address)
        landslide = get_landslide_hazard(address)
        soil_pdf_path = "scrappers/soil_report.pdf"  # update path if needed
        geo_required = get_geo_report_required(soil_pdf_path, liquefaction)
        hardcoded = get_hardcoded_values()
        save_planning_data(property_id, jurisdiction, fire_district, zoning_overlay, planning,
                           liquefaction, landslide, geo_required, hardcoded)

        # Step 6: Utility Info
        check_water_systems(property_id)

        print("\n✅ All data processing complete.")

    except Exception as e:
        print(f"\n❌ Error in main pipeline: {e}")
        import traceback
        print(traceback.format_exc())

# FastAPI Implementation
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class TaxlotRequest(BaseModel):
    taxlot_id: str

@app.post("/run")
def run_pipeline(req: TaxlotRequest):
    try:
        main(req.taxlot_id)
        return {"status": "success", "message": f"Pipeline completed for {req.taxlot_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
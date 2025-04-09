import os
import time
import json
import psycopg2
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from geopy.distance import geodesic
from PIL import Image
from io import BytesIO
import cv2
import numpy as np
from flask import Flask, request, jsonify, make_response
from flask_cors import CORS
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Load environment variables
load_dotenv()

# Database connection details
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("sagedatabase")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")

# API Keys
OPENCAGE_API_KEY = "e96ce384fa9d460680568dba6a5fc6d3"
GOOGLE_API_KEY = "AIzaSyBWSO84ehJ8AHQwi0hHqLn5aE6bFWSC0tI"

def save_to_database(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()
        
        # Map the fields correctly before saving
        mapped_data = {
            "id": data.get("id"),
            "owner_name": data.get("owner_name"),
            "mailing_address": data.get("situs_address"),  # Map situs_address to mailing_address
            "parcel_number": data.get("map_and_taxlot"),  # Map map_and_taxlot to parcel_number
            "acres": data.get("acres"),
            "legal": data.get("legal"),
            "site_address": data.get("site_address"),
            "plat_map": None,
            "plat_map_url": data.get("plat_map_url"),
            "tax_map": None,
            "tax_map_url": data.get("tax_map_url")
        }
        
        # Conditional logic for maps
        if mapped_data["plat_map_url"]:
            mapped_data["plat_map"] = 'YES'
        if mapped_data["tax_map_url"]:
            mapped_data["tax_map"] = 'YES'

        # Insert data into the table
        insert_query = """
        INSERT INTO basic_info (id, owner_name, mailing_address, parcel_number, acres, plat_map, plat_map_url, tax_map, tax_map_url, legal, site_address)
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
            legal = EXCLUDED.legal,
            site_address = EXCLUDED.site_address;
        """

        cursor.execute(insert_query, (
            mapped_data["id"],
            mapped_data["owner_name"],
            mapped_data["mailing_address"],
            mapped_data["parcel_number"],
            mapped_data["acres"],
            mapped_data["plat_map"],
            mapped_data["plat_map_url"],
            mapped_data["tax_map"],
            mapped_data["tax_map_url"],
            mapped_data["legal"],
            mapped_data["site_address"]
        ))

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Data saved successfully!")
    except Exception as e:
        print(f"❌ Database error: {e}")

def get_json_data(data):
    """Convert the extracted data to JSON format."""
    return json.dumps(data, indent=4)

def get_coordinates(address):
    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={OPENCAGE_API_KEY}"
    response = requests.get(url).json()
    
    if response['results']:
        lat = response['results'][0]['geometry']['lat']
        lon = response['results'][0]['geometry']['lng']
        return lat, lon
    return None, None

def get_elevation(lat, lon):
    url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={GOOGLE_API_KEY}"
    response = requests.get(url).json()
    if response.get("results"):
        return response["results"][0]["elevation"] * 3.281  # Convert meters to feet
    return None

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
        return bool(green_percentage > 5)  # Convert to native Python bool
    return None

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
        return bool(len(building_contours) > 2)  # Convert to native Python bool
    return None

def insert_data_into_db(property_id, lat, lon, slope, has_trees, has_buildings):
    gps_coord = f"{lat},{lon}"  # Convert to string format
    power_visible = False  # Default value

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            dbname=DB_NAME,
            user=DB_USER,
            p
            ssword=DB_PASS,
            port=DB_PORT
        )
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO google_earth_info 
        (property_id, gps_coord, slope, power_visible, existing_structures, trees_brush) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            property_id,
            gps_coord,
            str(slope), 
            bool(power_visible), 
            bool(has_buildings), 
            bool(has_trees)
        ))
        conn.commit()
        print("✅ Data successfully inserted into the database.")
    except Exception as e:
        print("❌ Database insertion error:", str(e))
    finally:
        if conn:
            cursor.close()
            conn.close()

def generate_pdf(json_data):
    """Generate a PDF from JSON data using ReportLab."""
    # Create a buffer to hold the PDF
    pdf_buffer = BytesIO()

    # Create a PDF document
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
    elements = []

    # Add a title
    styles = getSampleStyleSheet()
    elements.append(Paragraph("Property Report", styles['Title']))

    # Convert JSON data to a table
    table_data = [["Field", "Value"]]
    for key, value in json_data.items():
        table_data.append([key, str(value)])

    # Create the table
    table = Table(table_data)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),  # Header row background
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),  # Header row text color
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),  # Center align all cells
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),  # Header row font
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),  # Header row padding
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),  # Table body background
        ('GRID', (0, 0), (-1, -1), 1, colors.black),  # Add grid lines
    ]))

    # Add the table to the elements
    elements.append(table)

    # Build the PDF
    doc.build(elements)

    # Reset the buffer position to the beginning
    pdf_buffer.seek(0)
    return pdf_buffer

@app.route('/get-property-info', methods=['GET', 'POST'])
def get_property_info():
    if request.method == 'POST':
        return fetch_property_data()
    return jsonify({"message": "Please use POST method with property data"}), 200

@app.route('/fetch-property-data', methods=['POST'])
def fetch_property_data():
    property_id = request.json.get('property_id')
    address = request.json.get('address')

    # STEP 1: SEARCH PROPERTY DATA
    print("🔹 Fetching property data...")
    driver = webdriver.Chrome()
    driver.get("https://dial.deschutes.org/Search/General")
    wait = WebDriverWait(driver, 10)
    
    search_box = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    search_box.send_keys(property_id)
    search_box.send_keys(Keys.RETURN)
    time.sleep(5)

    page_html = driver.page_source
    soup = BeautifulSoup(page_html, "html.parser")

    fields = {
        "owner_name": "Mailing Name:",
        "situs_address": "Situs Address:",
        "map_and_taxlot": "Map and Taxlot:",
        "legal": "Legal:",
        "site_address": "Site Address:",
        "acres": "Assessor Acres:",
    }

    extracted_data = {"id": property_id}  # Set the property_id
    for field, label in fields.items():
        label_tag = soup.find("strong", string=label)
        extracted_data[field] = label_tag.find_next_sibling(string=True).strip() if label_tag else None

    # Extract additional fields using Method 2
    old_fields = ["Map and Taxlot:", "Situs Address:"]
    for field in old_fields:
        try:
            element = soup.find(string=field)
            if element:
                next_element = element.find_next()
                extracted_data[field.lower().replace(" ", "_")] = next_element.text.strip() if next_element else "Next element not found"
            else:
                extracted_data[field.lower().replace(" ", "_")] = "Field not found in page"
        except Exception as e:
            extracted_data[field.lower().replace(" ", "_")] = f"Error: {str(e)}"

    # STEP 2: DOWNLOAD TAX MAP
    print("🔹 Fetching Tax Map URL...")
    driver.get(f"https://dial.deschutes.org/Real/Index/{property_id}")
    wait = WebDriverWait(driver, 10)

    try:
        tax_map_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Tax Map")))
        extracted_data["tax_map_url"] = tax_map_link.get_attribute("href")
    except Exception:
        extracted_data["tax_map_url"] = None

    # STEP 3: SAVE TO DATABASE
    print("🔹 Extracted Data:")
    print(extracted_data)
    save_to_database(extracted_data)

    # STEP 4: GET GEOGRAPHICAL DATA
    lat, lon = get_coordinates(address)
    if lat and lon:
        offset_distance = 5 / 5280  # Convert 5 feet to miles
        point2 = geodesic(miles=offset_distance).destination((lat, lon), bearing=0)
        lat2, lon2 = point2.latitude, point2.longitude

        elevation1 = get_elevation(lat, lon)
        elevation2 = get_elevation(lat2, lon2)

        if elevation1 is not None and elevation2 is not None:
            rise = abs(elevation2 - elevation1)
            run = 5  # 5 feet
            slope = (rise / run) * 100
        else:
            slope = None

        has_trees = detect_trees(lat, lon)
        has_buildings = detect_buildings(lat, lon)

        # Prepare data for JSON
        geographical_data = {
            "property_id": property_id,
            "gps_coord": f"{lat},{lon}",
            "slope": slope,
            "has_trees": has_trees,
            "has_buildings": has_buildings,
            "power_visible": False  # Default value
        }

        # Insert Data into DB
        insert_data_into_db(property_id, lat, lon, slope, has_trees, has_buildings)

        # Combine both datasets
        combined_data = {**extracted_data, **geographical_data}

        # Return JSON Data
        json_data = get_json_data(combined_data)
        print("🔹 JSON Data:")
        print(json_data)
        return jsonify(combined_data)
    else:
        return jsonify({"error": "Could not fetch coordinates."}), 400

@app.route('/generate-pdf', methods=['POST'])
def generate_pdf_api():
    """
    Simple API endpoint to generate and return a PDF file.
    Expects JSON data in the request body.
    """
    try:
        # Get JSON data from the request
        json_data = request.json

        # Generate PDF
        pdf_buffer = generate_pdf(json_data)

        # Return PDF as a file response
        response = make_response(pdf_buffer.getvalue())
        response.headers['Content-Type'] = 'application/pdf'
        response.headers['Content-Disposition'] = 'attachment; filename=report.pdf'
        return response
    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
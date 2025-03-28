import os
import json  # Import the json module
import psycopg2
import requests
from geopy.distance import geodesic
from PIL import Image
from io import BytesIO
import cv2
import numpy as np
import base64
import openai

# API Keys
OPENCAGE_API_KEY = "e96ce384fa9d460680568dba6a5fc6d3"
GOOGLE_API_KEY = "AIzaSyBWSO84ehJ8AHQwi0hHqLn5aE6bFWSC0tI"
OPENAI_API_KEY = os.getenv("OPEN_AI_API_KEY")  # Get from environment variable

# Database Connection Variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = "sagedatabase"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")

# Ensure all required env variables are present
if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT]):
    raise EnvironmentError("Missing database connection environment variables.")

# Address to Fetch Data For
address = "20556 KLAHANI DR BEND, OR 97702"
property_id = 120456  # Fixed Property ID

# Step 1: Get Coordinates
def get_coordinates(address):
    url = f"https://api.opencagedata.com/geocode/v1/json?q={address}&key={OPENCAGE_API_KEY}"
    response = requests.get(url).json()
    
    if response['results']:
        lat = response['results'][0]['geometry']['lat']
        lon = response['results'][0]['geometry']['lng']
        return lat, lon, url  # Return source URL
    return None, None, None

# Step 2: Get Elevation Data
def get_elevation(lat, lon):
    url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={GOOGLE_API_KEY}"
    response = requests.get(url).json()
    if response.get("results"):
        return response["results"][0]["elevation"] * 3.281, url  # Return elevation in feet and source URL
    return None, None

# Step 3: Detect Trees
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

# Step 4: Detect Buildings
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

# Step 5: Detect Power Infrastructure using GPT-4 Vision
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
        # Set up OpenAI API key
        openai.api_key = OPENAI_API_KEY
        
        # For OpenAI API version 0.28.1
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {OPENAI_API_KEY}"
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

# Insert Data into PostgreSQL
def insert_data_into_db(property_id, lat, lon, slope, slope_source, has_trees, trees_source, has_buildings, buildings_source, power_visible, power_source):
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

# Function to return JSON data
def get_json_data(data):
    """Convert the extracted data to JSON format."""
    # Convert NumPy types to native Python types
    if isinstance(data, dict):
        return json.dumps({k: (bool(v) if isinstance(v, np.bool_) else v) for k, v in data.items()}, indent=4)
    return json.dumps(data, indent=4)

# Main Execution
lat, lon, coord_source = get_coordinates(address)
if lat and lon:
    offset_distance = 5 / 5280  # Convert 5 feet to miles
    point2 = geodesic(miles=offset_distance).destination((lat, lon), bearing=0)
    lat2, lon2 = point2.latitude, point2.longitude

    elevation1, elev1_source = get_elevation(lat, lon)
    elevation2, _ = get_elevation(lat2, lon2)

    if elevation1 is not None and elevation2 is not None:
        rise = abs(elevation2 - elevation1)
        run = 5  # 5 feet
        slope = (rise / run) * 100
    else:
        slope = None
        elev1_source = None

    has_trees, trees_source = detect_trees(lat, lon)
    has_buildings, buildings_source = detect_buildings(lat, lon)
    power_visible, power_source = detect_power_infrastructure(lat, lon, property_id)

    # Prepare data for JSON
    extracted_data = {
        "property_id": property_id,
        "gps_coord": f"{lat},{lon}",
        "gps_coord_source": coord_source,
        "slope": slope,
        "slope_source": elev1_source,
        "has_trees": has_trees,
        "trees_source": trees_source,
        "has_buildings": has_buildings,
        "buildings_source": buildings_source,
        "power_visible": power_visible,
        "power_source": power_source
    }

    # Insert Data into DB
    insert_data_into_db(property_id, lat, lon, slope, elev1_source, has_trees, trees_source, has_buildings, buildings_source, power_visible, power_source)

    # Return JSON Data
    json_data = get_json_data(extracted_data)
    print("🔹 JSON Data:")
    print(json_data)
else:
    print("⚠️ Could not fetch coordinates.")

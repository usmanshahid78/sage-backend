import os
import requests
import urllib.parse
import PyPDF2
import re
import psycopg2
import json
from io import BytesIO
from datetime import datetime

# Database connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_NAME = "sagedatabase"  # Explicitly setting the DB name
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT", "5432")  # Default to PostgreSQL port

# Ensure all required env variables are present
required_env_vars = [DB_HOST, DB_NAME, DB_USER, DB_PASS, DB_PORT]
if not all(required_env_vars):
    raise EnvironmentError("One or more environment variables for database connection are missing.")

def create_geocode_url(address):
    encoded_address = urllib.parse.quote(address)
    return (
        f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?"
        f"SingleLine={encoded_address}&f=json&outSR=%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D&countryCode=USA&maxLocations=1"
    )

def get_coordinates(geocode_url):
    response = requests.get(geocode_url).json()
    if response.get("candidates"):
        x = response["candidates"][0]["location"]["x"]
        y = response["candidates"][0]["location"]["y"]
        return x, y, geocode_url  # Return source URL as well
    else:
        raise ValueError("No coordinates found for the given address")

def create_snow_load_url(x, y):
    return (
        f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/identify?"
        f"f=json&tolerance=2&returnGeometry=true&returnFieldName=false&returnUnformattedValues=false&imageDisplay=770,501,96&"
        f'geometry={{"x":{x},"y":{y}}}&geometryType=esriGeometryPoint&sr=102100&'
        f"mapExtent={x-0.01},{y-0.01},{x+0.01},{y+0.01}&layers=all:0,91"
    )

def get_snow_load_value(snow_load_url):
    response = requests.get(snow_load_url).json()
    for result in response.get('results', []):
        if result['layerName'] == "Snowload":
            return result['attributes'].get('SNOWLOAD', None), snow_load_url
    return None, None

def extract_design_parameters(pdf_url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(pdf_url, headers=headers, timeout=10)
        response.raise_for_status()
        with BytesIO(response.content) as file:
            reader = PyPDF2.PdfReader(file)
            text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
            
            # Save extracted text for debugging
            with open("extracted_pdf_text.txt", "w", encoding="utf-8") as f:
                f.write(text)
            
            # Print a snippet of the extracted text for immediate debugging
            print("🔹 Extracted PDF Text Snippet:\n", text[:1000])  # Print first 1000 characters
            
            # Based on the PDF content, look for the "3 sec. gusts" value which is likely the basic wind speed
            gusts_match = re.search(r'(\d+\s?mph)\s+3\s+sec\.\s+gusts', text, re.IGNORECASE)
            if gusts_match:
                basic_wind_speed = gusts_match.group(1)
                print(f"✅ Found basic wind speed from gusts pattern: {basic_wind_speed}")
            else:
                # Try multiple regex patterns for basic wind speed as a fallback
                basic_wind_patterns = [
                    r'Basic\s+Wind\s+Speed\s*(V1)?\s*[:\s]*(\d+\s?mph)',
                    r'Wind\s+Speed\s*V1\s*[:\s]*(\d+\s?mph)',
                    r'Basic\s+Wind\s+Speed\s*[:\s]*(\d+)',
                    r'Wind[^:]*V1\s*[:\s]*(\d+)',
                    r'V1\s*[:\s=]*\s*(\d+\s?mph)',
                    r'Wind\s+Speed\s*[^\n]*?(\d+\s?mph)'  # Generic wind speed pattern
                ]
                
                basic_wind_speed = None
                
                # Try the multiple pattern approach
                for pattern in basic_wind_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        # Get the last group, which should contain the value
                        basic_wind_speed = match.group(match.lastindex)
                        print(f"✅ Found basic wind speed with fallback pattern: {pattern}")
                        print(f"Matched value: {basic_wind_speed}")
                        break
            
            # For thorough debugging, also search for any mentions of wind and mph
            wind_mentions = re.findall(r'[^\n]*wind[^\n]*', text, re.IGNORECASE)
            mph_mentions = re.findall(r'[^\n]*mph[^\n]*', text, re.IGNORECASE)
            seismic_mentions = re.findall(r'[^\n]*seismic[^\n]*', text, re.IGNORECASE)
            
            if wind_mentions:
                print("\n🔍 Lines containing 'wind' in the PDF:")
                for i, line in enumerate(wind_mentions[:10]):  # Limit to first 10 matches
                    print(f"{i+1}. {line.strip()}")
            
            if mph_mentions:
                print("\n🔍 Lines containing 'mph' in the PDF:")
                for i, line in enumerate(mph_mentions[:10]):  # Limit to first 10 matches
                    print(f"{i+1}. {line.strip()}")
                    
            if seismic_mentions:
                print("\n🔍 Lines containing 'seismic' in the PDF:")
                for i, line in enumerate(seismic_mentions[:10]):  # Limit to first 10 matches
                    print(f"{i+1}. {line.strip()}")
            
            wind_speed_match = re.search(r'Ultimate\s+Design\s+Wind\s+Speed\s+(\d+\s?mph)', text, re.IGNORECASE)
            frost_depth_match = re.search(r'Frost\s+Depth\s*[:\s]*(\d+["]?)', text, re.IGNORECASE)
            exposure_match = re.search(r'Exposure\s*[:\s]+"?([A-D])"?', text, re.IGNORECASE)
            
            # Updated seismic pattern to match "Seismic C for..." format
            seismic_match = re.search(r'Seismic\s*[:\s]*([A-D])', text, re.IGNORECASE)
            if not seismic_match:
                # Try alternate pattern that specifically matches the PDF format
                seismic_match = re.search(r'Seismic\s+([A-D])\s+for', text, re.IGNORECASE)

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
    except requests.exceptions.RequestException as e:
        print(f"❌ Error downloading the PDF: {e}")
        return {}

def insert_into_db(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS, port=DB_PORT
        )
        cursor = conn.cursor()

        insert_query = """
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
            frost_depth_source = EXCLUDED.frost_depth_source
        """

        values = (
            125449,  # Fixed id
            data.get("ground_snow_load"), data.get("ground_snow_load_source"),
            data.get("seismic_design_category"), data.get("seismic_design_category_source"),
            data.get("basic_wind_speed"), data.get("basic_wind_speed_source"),
            data.get("ultimate_wind_design_speed"), data.get("ultimate_wind_design_speed_source"),
            data.get("exposure"), data.get("exposure_source"),
            data.get("frost_depth"), data.get("frost_depth_source")
        )

        cursor.execute(insert_query, values)
        conn.commit()
        print("✅ Data inserted successfully into the database.")

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    finally:
        cursor.close()
        conn.close()

def save_to_json(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"design_data_{timestamp}.json"
    with open(filename, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    print(f"✅ Data saved to {filename}")

def main():
    address = "18160 COTTONWOOD RD #346 SUNRIVER, OR 97707"
    pdf_url = "https://www.deschutes.org/sites/default/files/fileattachments/community_development/page/679/design_requirements_for_the_entire_county_2.pdf"
    
    try:
        geocode_url = create_geocode_url(address)
        x, y, geocode_source = get_coordinates(geocode_url)
        
        snow_load_url = create_snow_load_url(x, y)
        snow_load_value, snow_load_source = get_snow_load_value(snow_load_url)

        design_params = extract_design_parameters(pdf_url)
        design_params["ground_snow_load"] = snow_load_value
        design_params["ground_snow_load_source"] = snow_load_source
        
        # Print the data to the screen
        print("Extracted Design Parameters:")
        print(json.dumps(design_params, indent=4))
        
        # Save the data to a JSON file
        save_to_json(design_params)
        
        # Insert the data into the database
        insert_into_db(design_params)
    
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    main()

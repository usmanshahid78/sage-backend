import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import openai
import base64
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Load OpenAI key from env
openai.api_key = os.getenv("OPEN_AI_API_KEY")

# ---------- Step 1: Get Jurisdiction ----------
def get_jurisdiction_from_dial(property_id: int) -> str:
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.title.string if soup.title else ""
        if "City of" in title:
            return title.strip()
        return "Deschutes County"
    except Exception as e:
        print(f"Error: {e}")
        return "Unknown"

# ---------- Step 2: Get Fire District ----------
def get_fire_district_from_dial(address: str) -> str:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        search_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "searchText"))
        )
        search_input.send_keys(address)
        time.sleep(1)
        search_input.send_keys("\n")
        time.sleep(5)

        layer_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))
        )
        layer_button.click()
        time.sleep(1)

        fire_checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Fire')]/preceding-sibling::input"))
        )
        if not fire_checkbox.is_selected():
            fire_checkbox.click()
            time.sleep(3)

        info_panel = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "infoPanel"))
        )
        fire_text = info_panel.text
        if "Fire" in fire_text:
            return fire_text
        else:
            return "NO DISTRICT"
    except Exception as e:
        print(f"Error getting fire district: {e}")
        return "NO DISTRICT"
    finally:
        driver.quit()

# ---------- Step 3: Get Zoning & Overlay ----------
def get_zoning_and_overlay_from_dial(property_id: int) -> dict:
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        zoning_table = soup.find("table")
        zoning = []
        overlay = []
        if zoning_table:
            rows = zoning_table.find_all("tr")[1:]
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    zone = cols[1].get_text(strip=True)
                    over = cols[2].get_text(strip=True)
                    zoning.append(zone)
                    overlay.append(over)
        return {
            "zoning": ", ".join(zoning) if zoning else None,
            "overlay": ", ".join(overlay) if overlay else None
        }
    except Exception as e:
        print(f"Error extracting zoning/overlay: {e}")
        return {"zoning": None, "overlay": None}

# ---------- Step 4: Get Setbacks and Restrictions from Code (GPT) ----------
def analyze_setbacks_and_restrictions() -> dict:
    prompt = '''Please analyze this zoning code for a Multiple Ag Use Zoned property located at 64350 Sisemore Road Bend Oregon 97703. Determine and report the setback for: Front, Garage, Side and Rear. Examine the broader Deschutes County Code for lot coverage and building height requirements that may apply to this code. Also determine if there are solar setbacks that may be applicable to this property.

https://deschutescounty.municipalcodeonline.com/book?type=ordinances#name=CHAPTER_18.32_MULTIPLE_USE_AGRICULTURAL_ZONE;_MUA

Is Sisemore Road at this location a Local Street, Collector Street or Arterial Right of Way?

Due to being rural and gravel, my assumption is that Sisemore Rd is a local street.

Also, the property is bordered by a Riverine R4SBCx irrigation canal from the National Wetlands Inventory. Do you expect any special setbacks from this canal based on the information above?

Additionally, analyze the solar setback requirements for Deschutes County by referring to the following code section:
https://deschutescounty.municipalcodeonline.com/book/print?type=ordinances&name=18.116.180_Building_Setbacks_For_The_Protection_Of_Solar_Access'''
    try:
        result = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Extract exact values only, no commentary."},
                {"role": "user", "content": prompt}
            ]
        )
        answer = result['choices'][0]['message']['content']
        return parse_structured_gpt_output(answer)
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
        line_lower = line.lower()
        for key in result:
            if key.replace("_", " ") in line_lower:
                parts = line.split(":")
                if len(parts) > 1:
                    value = parts[1].strip()
                    if key == "solar_setback":
                        if "yes" in value.lower():
                            result[key] = "YES"
                        else:
                            result[key] = "NO"
                    elif key == "special_setback":
                        feet_match = [s for s in value.split() if s.isdigit() or "ft" in s or "feet" in s]
                        result[key] = " ".join(feet_match) if feet_match else value
                    else:
                        result[key] = value
    return result

# ---------- Step 5: Get Liquefaction Hazard ----------
def get_liquefaction_hazard(address: str) -> str:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        search_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "searchText"))
        )
        search_input.send_keys(address)
        time.sleep(1)
        search_input.send_keys("\n")
        time.sleep(5)

        layer_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))
        )
        layer_button.click()
        time.sleep(1)

        # Turn on Earthquake Hazard layer (Liquefaction map)
        eq_checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Earthquake Hazard')]/preceding-sibling::input"))
        )
        if not eq_checkbox.is_selected():
            eq_checkbox.click()
            time.sleep(3)

        # Extract liquefaction info from info panel
        info_panel = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "infoPanel"))
        )
        info_text = info_panel.text
        if "High" in info_text:
            return "HIGH"
        elif "Moderate" in info_text:
            return "MODERATE"
        elif "Low" in info_text:
            return "LOW"
        return "MODERATE"
    except Exception as e:
        print(f"Error getting liquefaction hazard: {e}")
        return "MODERATE"
    finally:
        driver.quit()

# ---------- Step 6: Get Landslide Hazard ----------
def get_landslide_hazard(address: str) -> str:
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://dial.deschutes.org/Real/InteractiveMap")
        search_input = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "searchText"))
        )
        search_input.send_keys(address)
        time.sleep(1)
        search_input.send_keys("\n")
        time.sleep(5)

        layer_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Layers')]"))
        )
        layer_button.click()
        time.sleep(1)

        # Turn on Landslide Susceptibility layer
        landslide_checkbox = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//label[contains(text(), 'Landslide Susceptibility')]/preceding-sibling::input"))
        )
        if not landslide_checkbox.is_selected():
            landslide_checkbox.click()
            time.sleep(3)

        info_panel = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "infoPanel"))
        )
        info_text = info_panel.text
        if "High" in info_text:
            return "HIGH"
        elif "Moderate" in info_text:
            return "MODERATE"
        elif "Low" in info_text:
            return "LOW"
        return "UNKNOWN"
    except Exception as e:
        print(f"Error getting landslide hazard: {e}")
        return "UNKNOWN"
    finally:
        driver.quit()

# ---------- Step 7: Determine if Geotechnical Report is Required ----------
def get_geo_report_required(soil_pdf_path: str, liquefaction: str) -> str:
    prompt = f'''
Please analyze this NRCS Soils Report. Determine if a geotechnical report is likely needed to build a single family dwelling with a foundation bearing capacity of 1500 psf on this property. Specifically look at shrink swell hazard potential.

Also analyze these soil types against the prescriptive soils descriptions in the Oregon Residential Specialty Code for foundations bearing 1500 psf.

Additionally, consider this information:
- Liquefaction potential: {liquefaction}
- This is from HazVu Liquefaction Susceptibility Map
- Assume highly erodible soils may be present (no screenshot uploaded)

Does Deschutes County have codified requirements related to either liquefaction or highly erodible soils that would require a geotechnical report?

Respond with only one of these: YES, NO, or MORE INFO NEEDED.
'''

    try:
        with open(soil_pdf_path, 'rb') as pdf_file:
            pdf_content = pdf_file.read()

        base64_pdf = base64.b64encode(pdf_content).decode('utf-8')
        result = openai.ChatCompletion.create(
            model="gpt-4-vision-preview",
            messages=[
                {"role": "user", "content": [
                    {"type": "text", "text": prompt},
                    {"type": "file", "name": "soil_report.pdf", "data": base64_pdf}
                ]}
            ]
        )
        reply = result['choices'][0]['message']['content']
        if "yes" in reply.lower():
            return "YES"
        elif "no" in reply.lower():
            return "NO"
        else:
            return "MORE INFO NEEDED"
    except Exception as e:
        print(f"Error evaluating geo report requirement: {e}")
        return "MORE INFO NEEDED"

# ---------- Step 8: Get Hardcoded Values for Missing Data ----------
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

# ---------- Step 9: Save to Database ----------
def save_to_database(property_id, jurisdiction, fire_district, zoning_overlay, planning_data, 
                     liquefaction, landslide, geo_report_required, hardcoded_values):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="sagedatabase",  # Corrected from "sagedatabase" to "DB_NAME"
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432")
        )
        cursor = conn.cursor()

        # SQL Query to Insert Data
        sql = """
        INSERT INTO planning_data (
            property_id, jurisdiction, fire_district, zoning, overlay,
            max_lot_coverage, max_building_height, front_setback, side_setback, rear_setback, 
            solar_setback, special_setback, liquefaction_hazard, landslide_hazard, geo_report_required,
            fema_flood_zone, hydric_soils_hazard, wetlands_on_property, erosion_control_required, 
            stormwater_requirements, tree_preservation_reqs, special_fire_marshal_reqs, radon, sidewalks_required, approach_permit
        ) 
        VALUES (
            %(property_id)s, %(jurisdiction)s, %(fire_district)s, %(zoning)s, %(overlay)s,
            %(max_lot_coverage)s, %(max_building_height)s, %(front_setback)s, %(side_setback)s, %(rear_setback)s, 
            %(solar_setback)s, %(special_setback)s, %(liquefaction_hazard)s, %(landslide_hazard)s, %(geo_report_required)s,
            %(fema_flood_zone)s, %(hydric_soils_hazard)s, %(wetlands_on_property)s, %(erosion_control_required)s, 
            %(stormwater_requirements)s, %(tree_preservation_reqs)s, %(special_fire_marshal_reqs)s, %(radon)s, %(sidewalks_required)s, %(approach_permit)s
        );
        """

        # Data Mapping
        data = {
            "property_id": property_id,
            "jurisdiction": jurisdiction,
            "fire_district": fire_district,
            "zoning": zoning_overlay["zoning"],
            "overlay": zoning_overlay["overlay"],
            "max_lot_coverage": planning_data["max_lot_coverage"],
            "max_building_height": planning_data["max_building_height"],
            "front_setback": planning_data["front_setback"],
            "side_setback": planning_data["side_setback"],
            "rear_setback": planning_data["rear_setback"],
            "solar_setback": planning_data["solar_setback"],
            "special_setback": planning_data["special_setback"],
            "liquefaction_hazard": liquefaction,
            "landslide_hazard": landslide,
            "geo_report_required": geo_report_required,
            "fema_flood_zone": hardcoded_values["fema_flood_zone"],
            "hydric_soils_hazard": hardcoded_values["hydric_soils_hazard"],
            "wetlands_on_property": hardcoded_values["wetlands_on_property"],
            "erosion_control_required": hardcoded_values["erosion_control_required"],
            "stormwater_requirements": hardcoded_values["stormwater_requirements"],
            "tree_preservation_reqs": hardcoded_values["tree_preservation_reqs"],
            "special_fire_marshal_reqs": hardcoded_values["special_fire_marshal_reqs"],
            "radon": hardcoded_values["radon"],
            "sidewalks_required": hardcoded_values["sidewalks_required"],
            "approach_permit": hardcoded_values["approach_permit"]
        }

        # Execute and commit
        cursor.execute(sql, data)
        conn.commit()
        print("Data successfully saved to database!")
        return True
    except Exception as e:
        print(f"Error saving to database: {e}")
        return False
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

# ---------- Run Everything ----------
if __name__ == "__main__":
    property_id = 125449
    address = "18160 COTTONWOOD RD 346 SUNRIVER, OR 97707"
    soil_pdf_path = "scrappers/soil_report.pdf"

    # Dynamically fetch available data
    jurisdiction = get_jurisdiction_from_dial(property_id)
    fire_district = get_fire_district_from_dial(address)
    zoning_overlay = get_zoning_and_overlay_from_dial(property_id)
    planning_data = analyze_setbacks_and_restrictions()
    liquefaction = get_liquefaction_hazard(address)
    landslide = get_landslide_hazard(address)
    geo_report_required = get_geo_report_required(soil_pdf_path, liquefaction)
    
    # Get hardcoded values for missing data
    hardcoded_values = get_hardcoded_values()

    # Print results
    print("Jurisdiction:", jurisdiction)
    print("Fire District:", fire_district)
    print("Zoning:", zoning_overlay["zoning"])
    print("Overlay:", zoning_overlay["overlay"])
    print("Liquefaction Hazard:", liquefaction)
    print("Landslide Hazard:", landslide)
    print("Geotechnical Report Required:", geo_report_required)

    # Print planning data
    for k, v in planning_data.items():
        label = k.replace("_", " ").title()
        print(f"{label}: {v}")
    
    # Print hardcoded values
    for key, value in hardcoded_values.items():
        label = key.replace("_", " ").title()
        print(f"{label}: {value}")
    
    # Save data to database
    save_to_database(property_id, jurisdiction, fire_district, zoning_overlay, planning_data,
                    liquefaction, landslide, geo_report_required, hardcoded_values)


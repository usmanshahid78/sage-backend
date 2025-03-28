import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import psycopg2

# Database connection details
DB_HOST = "sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com"
DB_NAME = "sagedatabase"
DB_USER = "postgres"
DB_PASS = "12345678"
DB_PORT = "5432"

def check_for_septic(account_id):
    """Check if a property has a septic permit."""
    url = f"https://dial.deschutes.org/Real/Permits/{account_id}"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"❌ Failed to fetch septic data for account ID {account_id}.")
        return None, None  # Return data and source URL

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "infoTable"})

    if table:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) > 1 and cells[1].text.strip() == "Septic":
                return "Septic", url  # Return data and source URL
        return "No septic permit found.", url
    else:
        return "No permit table found in the response.", url

def check_for_well():
    """Check if a property has a well permit."""
    url = "https://apps.wrd.state.or.us/apps/gw/well_log/"
    driver = webdriver.Chrome()
    driver.get(url)

    try:
        find_trs_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ctl00_PageData_btn_address_view"))
        )
        find_trs_button.click()

        address_input = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "ctl00_PageData_txt_address"))
        )
        address_input.send_keys("18160 COTTONWOOD RD #346 SUNRIVER, OR 97707")

        lookup_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "ctl00_PageData_btn_lookup_address"))
    )
        lookup_button.click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "input[type='checkbox'][name^='ctl00$PageData$sctn']"))
        )

        sections = driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox'][name^='ctl00$PageData$sctn']")
        any_section_ticked = any(section.is_selected() for section in sections)

        if any_section_ticked:
            return "Well", url  # Return data and source URL

    except Exception as e:
        return f"An error occurred: {e}", url

    finally:
        driver.quit()

    return "No well found at this address.", url

def save_to_db(account_id, waste_water_type, waste_water_source, water_type, water_source, power_type, power_source):
    """Save or update utility data in the database."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
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
        cur.close()
        print("✅ Data saved successfully.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"❌ Database error: {error}")
    finally:
        if conn is not None:
            conn.close()

def check_water_systems(account_id):
    """Check septic and well status, and save the data along with power type."""
    septic_result, septic_source = check_for_septic(account_id)
    well_result, well_source = check_for_well()
    power_type = "Underground"  # Fixed value for power type
    power_source = "https://www.deschutes.org/"  # Placeholder for power source

    save_to_db(account_id, septic_result, septic_source, well_result, well_source, power_type, power_source)
    return septic_result, well_result, power_type

# Example usage
account_id = "125449"  # Replace with the desired account ID
septic_result, well_result, power_type = check_water_systems(account_id)
print("Septic Check:", septic_result)
print("Well Check:", well_result)
print("Power Type:", power_type)

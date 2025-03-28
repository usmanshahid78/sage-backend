import os
import time
import json
import psycopg2
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST", "sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com")
DB_NAME = "sagedatabase"
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "12345678")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_property_id(taxlot_id):
    """Fetches the Property ID by using the Taxlot ID."""
    search_url = f"https://dial.deschutes.org/results/general?value={taxlot_id}"
    session = requests.Session()
    response = session.get(search_url, allow_redirects=True)
    if response.status_code == 200:
        return response.url.split("/")[-1]  # Extracts Property ID from the final redirected URL
    print(f"❌ Failed to fetch Property ID for Taxlot ID: {taxlot_id}")
    return None

def get_json_data(data):
    """Convert the extracted data to JSON format."""
    return json.dumps(data, indent=4)

def fetch_html_data(property_id):
    """Fetches HTML content for the given Property ID."""
    url = f"https://dial.deschutes.org/Real/Index/{property_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content, url
    else:
        print(f"❌ Failed to fetch HTML data for property_id: {property_id}")
        return None, None

def fetch_plat_map_url(property_id, html_content=None):
    """Fetch the Plat Map URL for a given property ID."""
    url = f"https://dial.deschutes.org/Real/Index/{property_id}"
    if not html_content:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"❌ Failed to fetch property page for ID: {property_id}")
            return None, None
        html_content = response.content
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Look for any links pointing to recordings.deschutes.org
    plat_map_link = soup.find("a", href=lambda href: href and "recordings.deschutes.org" in href)
    
    if plat_map_link:
        plat_map_url = plat_map_link["href"]
        print(f"✅ Plat Map Found: {plat_map_url}")
        return plat_map_url, url
    else:
        print("❌ No Plat Map found for this property.")
        return None, url

def fetch_development_summary(property_id):
    """Fetch HTML from the Development Summary page."""
    url = f"https://dial.deschutes.org/Real/DevelopmentSummary/{property_id}"
    response = requests.get(url)
    if response.status_code == 200:
        print("🔹 Development Summary Page Fetched Successfully")
        print(f"🔹 URL: {url}")
        return response.content, url
    else:
        print(f"❌ Failed to fetch Development Summary for property_id: {property_id}")
        return None, None

def extract_html_data(html_content, dev_summary_content=None, main_url=None, dev_summary_url=None):
    """Extracts relevant property data from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")
    data = {}

    # Extract owner_name
    owner_name_tag = soup.find("strong", string="Mailing Name:")
    if owner_name_tag:
        data["owner_name"] = owner_name_tag.find_next_sibling(string=True).strip()
        data["owner_name_source"] = main_url

    # Extract map_and_taxlot
    map_and_taxlot_tag = soup.find("span", id="uxMapTaxlot")
    if map_and_taxlot_tag:
        data["map_and_taxlot"] = map_and_taxlot_tag.text.strip()
        data["parcel_number_source"] = main_url

    # Extract situs_address
    situs_address_tag = soup.find("span", id="uxSitusAddress")
    if situs_address_tag:
        data["situs_address"] = situs_address_tag.text.strip()
        data["site_address_source"] = main_url

    # Extract mailing address (from 'Ownership' section below the name)
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
                mailing_address = " ".join(address_lines)
                data["mailing_address"] = mailing_address
                data["mailing_address_source"] = main_url

    # Extract acres
    acres_tag = soup.find("strong", string="Assessor Acres:")
    if acres_tag:
        data["acres"] = acres_tag.find_next_sibling(string=True).strip()
        data["acres_source"] = main_url
    
    # Extract assessor property description (legal column)
    assessor_property_tag = soup.find("strong", string="Assessor Property Description:")
    if assessor_property_tag:
        legal_description_tag = assessor_property_tag.find_next("strong")
        if legal_description_tag:
            legal_description = legal_description_tag.next_sibling.strip()
            data["legal"] = legal_description
            data["legal_source"] = main_url
            print(f"🔹 Legal from main page: {legal_description}")
    
    # Extract subdivision and lot information from Development Summary page
    if dev_summary_content:
        dev_soup = BeautifulSoup(dev_summary_content, "html.parser")
        print("🔹 Extracting from Development Summary...")
        
        # Find the Property Details section
        property_details_section = dev_soup.find("p", class_="uxReportSectionHeader", string="Property Details")
        if property_details_section:
            property_details = property_details_section.find_next("p")
            if property_details:
                property_details_text = property_details.get_text(separator=" ").strip()
                print(f"🔹 Property Details Text: {property_details_text}")
                
                # Extract subdivision, lot, and block values
                subdivision = ""
                lot = ""
                block = ""
                
                if "Subdivision:" in property_details_text:
                    subdivision = property_details_text.split("Subdivision:")[1].split("Lot:")[0].strip()
                if "Lot:" in property_details_text:
                    lot = property_details_text.split("Lot:")[1].split("Block:")[0].strip()
                if "Block:" in property_details_text:
                    block = property_details_text.split("Block:")[1].split("Acres:")[0].strip()
                
                print(f"🔹 Subdivision: {subdivision}")
                print(f"🔹 Lot: {lot}")
                print(f"🔹 Block: {block}")
                
                # Construct legal information
                legal_parts = []
                if subdivision:
                    legal_parts.append(subdivision)
                if lot:
                    legal_parts.append(f"Lot {lot}")
                if block:
                    legal_parts.append(f"Block {block}")
                
                if legal_parts:
                    data["legal"] = " ".join(legal_parts)
                    data["legal_source"] = dev_summary_url
                    print(f"🔹 Constructed legal: {data['legal']}")
                else:
                    print("❌ No legal parts found to construct legal field")
                
                # Also extract acres from Development Summary if not already found
                if "acres" not in data or not data["acres"]:
                    if "Acres:" in property_details_text:
                        acres = property_details_text.split("Acres:")[1].strip()
                        data["acres"] = acres
                        data["acres_source"] = dev_summary_url
                        print(f"🔹 Acres from Development Summary: {data['acres']}")
        else:
            print("❌ Property Details section not found")

    return data

def fetch_arcgis_data(map_and_taxlot):
    """Fetches GIS data from ArcGIS API."""
    api_url = f"https://maps.deschutes.org/arcgis/rest/services/Operational_Layers/MapServer/0/query?f=json&where=Taxlot_Assessor_Account.TAXLOT%20%3D%20%27{map_and_taxlot}%27&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*&outSR=102100"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json(), api_url
    else:
        print(f"❌ Failed to fetch ArcGIS data for map_and_taxlot: {map_and_taxlot}")
        return None, None

def extract_arcgis_data(api_data, api_url):
    """Extracts relevant GIS data from the API response."""
    if api_data and "features" in api_data and len(api_data["features"]) > 0:
        attributes = api_data["features"][0]["attributes"]
        return {
            "owner_name": attributes.get("dbo_GIS_MAILING.OWNER"),
            "owner_name_source": api_url,
            "situs_address": attributes.get("Taxlot_Assessor_Account.Address") + ", BEND, OR 97703",
            "site_address_source": api_url,
            "mailing_address": attributes.get("Taxlot_Assessor_Account.Address") + ", BEND, OR 97703",
            "mailing_address_source": api_url,
            "map_and_taxlot": attributes.get("Taxlot_Assessor_Account.TAXLOT"),
            "parcel_number_source": api_url,
            "acres": attributes.get("Taxlot_Assessor_Account.Shape_Area"),  # Assuming Shape_Area represents acres
            "acres_source": api_url
        }
    else:
        print("❌ No data found in ArcGIS API response")
        return None

def save_to_database(data):
    """Saves or updates property data in PostgreSQL database."""
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
            
        print(f"🔹 Final legal value to be saved: {data.get('legal')}")
        
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
        print("✅ Data saved successfully!")
    except Exception as e:
        print(f"❌ Database error: {e}")

def main(taxlot_id=None, property_id=None):
    """Main function to process data from either taxlot_id or property_id."""
    try:
        # If taxlot_id is provided, get property_id from it
        if taxlot_id and not property_id:
            print("🔹 Fetching Property ID...")
            property_id = get_property_id(taxlot_id)
            if not property_id:
                return
        
        # If no property_id was provided or found, exit
        if not property_id:
            print("❌ No property_id provided or found")
            return
            
        # STEP 1: FETCH HTML DATA
        print("🔹 Fetching HTML data...")
        html_content, main_url = fetch_html_data(property_id)
        
        # STEP 2: FETCH DEVELOPMENT SUMMARY DATA
        print("🔹 Fetching Development Summary data...")
        dev_summary_content, dev_summary_url = fetch_development_summary(property_id)
        
        # STEP 3: FETCH PLAT MAP URL
        print("🔹 Fetching Plat Map URL...")
        plat_map_url, plat_map_source = fetch_plat_map_url(property_id, html_content)

        if html_content:
            html_data = extract_html_data(html_content, dev_summary_content, main_url, dev_summary_url)
            print("🔹 Extracted HTML Data:")
            print(html_data)

            # STEP 4: FETCH ARCGIS DATA USING MAP AND TAXLOT
            if "map_and_taxlot" in html_data:
                print("🔹 Fetching ArcGIS data...")
                arcgis_data, arcgis_url = fetch_arcgis_data(html_data["map_and_taxlot"])

                if arcgis_data:
                    extracted_arcgis_data = extract_arcgis_data(arcgis_data, arcgis_url)
                    print("🔹 Extracted ArcGIS Data:")
                    print(extracted_arcgis_data)

                    # STEP 5: COMBINE DATA FROM ALL SOURCES
                    # Prefer HTML data over ArcGIS data where available
                    combined_data = {
                        "id": property_id,
                        "owner_name": html_data.get("owner_name") or extracted_arcgis_data.get("owner_name"),
                        "owner_name_source": html_data.get("owner_name_source") or extracted_arcgis_data.get("owner_name_source"),
                        "mailing_address": html_data.get("mailing_address"),
                        "mailing_address_source": html_data.get("mailing_address_source"),
                        "map_and_taxlot": html_data.get("map_and_taxlot"),
                        "parcel_number_source": html_data.get("parcel_number_source"),
                        "acres": html_data.get("acres") or extracted_arcgis_data.get("acres"),
                        "acres_source": html_data.get("acres_source") or extracted_arcgis_data.get("acres_source"),
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

                    # STEP 6: SAVE TO DATABASE
                    print("🔹 Combined Data:")
                    for key, value in combined_data.items():
                        print(f"  {key}: {value}")
                    save_to_database(combined_data)

                    # STEP 7: RETURN JSON DATA
                    json_data = get_json_data(combined_data)
                    print("🔹 JSON Data:")
                    print(json_data)
                    
                    return combined_data
                else:
                    print("❌ No ArcGIS data extracted")
            else:
                print("❌ map_and_taxlot not found in HTML data")
        else:
            print("❌ Failed to fetch HTML data")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
        import traceback
        print(traceback.format_exc())
        return None

if __name__ == "__main__":
    # Example usage with taxlot_id
    taxlot_id = "201118B010000"
    main(taxlot_id=taxlot_id)
    
    # Example usage with property_id directly
    # property_id = "131214"
    # main(property_id=property_id)

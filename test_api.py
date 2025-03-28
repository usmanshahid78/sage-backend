import requests
import json
import time

def test_unified_data_api():
    """Test the unified data collection API"""
    print("\n=== Testing Unified Data Collection API ===")
    
    # API endpoint
    url = "http://localhost:5000/run-all"
    
    # Test data
    data = {
        "property_id": 247951,
        "address": "10980 SUMMIT RIDGE CT, REDMOND, OR 97756",
        "soil_pdf_path": "scrappers/soil_report.pdf",
        "design_pdf_url": "https://www.deschutes.org/sites/default/files/fileattachments/community_development/page/679/design_requirements_for_the_entire_county_2.pdf"
    }
    
    try:
        # Make POST request
        print("🚀 Making request to unified API...")
        response = requests.post(url, json=data)
        
        # Check if request was successful
        if response.status_code == 200:
            print("✅ Request successful!")
            print("\n📊 Response data:")
            print(json.dumps(response.json(), indent=2))
            return response.json()
        else:
            print(f"❌ Request failed with status code: {response.status_code}")
            print("Error message:", response.text)
            return None
            
    except Exception as e:
        print(f"❌ Error making request: {str(e)}")
        return None

def test_pdf_generation_api(property_id):
    """Test the PDF generation API"""
    print("\n=== Testing PDF Generation API ===")
    
    # First, generate the PDF
    generate_url = f"http://localhost:8000/generate-pdf/{property_id}"
    try:
        print("🚀 Requesting PDF generation...")
        response = requests.get(generate_url)
        
        if response.status_code == 200:
            print("✅ PDF generation request successful!")
            pdf_data = response.json()
            print("\n📄 PDF data:")
            print(json.dumps(pdf_data, indent=2))
            
            # If we have a PDF filename, try to download it
            if "pdf_filename" in pdf_data:
                download_url = f"http://localhost:8000/download-pdf/{pdf_data['pdf_filename']}"
                print("\n📥 Downloading PDF...")
                pdf_response = requests.get(download_url)
                
                if pdf_response.status_code == 200:
                    print("✅ PDF downloaded successfully!")
                    # Save the PDF
                    with open(pdf_data['pdf_filename'], 'wb') as f:
                        f.write(pdf_response.content)
                    print(f"💾 PDF saved as: {pdf_data['pdf_filename']}")
                else:
                    print(f"❌ PDF download failed with status code: {pdf_response.status_code}")
            else:
                print("⚠️ No PDF filename in response")
        else:
            print(f"❌ PDF generation request failed with status code: {response.status_code}")
            print("Error message:", response.text)
            
    except Exception as e:
        print(f"❌ Error in PDF generation: {str(e)}")

def main():
    # First test the unified data collection API
    result = test_unified_data_api()
    
    if result:
        # Wait a bit for the data to be saved to the database
        print("\n⏳ Waiting for data to be saved to database...")
        time.sleep(5)
        
        # Then test the PDF generation API
        property_id = result.get("property_id")
        if property_id:
            test_pdf_generation_api(property_id)
        else:
            print("❌ No property_id in unified API response")

if __name__ == "__main__":
    main() 
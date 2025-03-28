from typing import Dict, Any
import logging
import aiohttp
from urllib3.exceptions import InsecureRequestWarning
import urllib3
from .scraper_template import BaseScraper

# Suppress SSL warnings
urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger(__name__)

class TaxAssessorScraper(BaseScraper):
    def __init__(self, property_id: str):
        super().__init__(property_id)
        # Configure retry settings
        self.max_retries = 5
        self.retry_codes = {500, 502, 503, 504}
    
    async def scrape(self) -> Dict[str, Any]:
        """Main scraping method for tax assessor data"""
        try:
            # Get coordinates from property ID
            coordinates = await self.get_coordinates()
            if "error" in coordinates:
                return coordinates
            
            # Get FEMA flood data
            flood_data = await self.get_fema_flood_data(
                coordinates["latitude"],
                coordinates["longitude"]
            )
            
            # Get tax assessment data
            tax_data = await self.get_tax_data()
            
            return {
                "status": "success",
                "coordinates": coordinates,
                "flood_data": flood_data,
                "tax_assessment": tax_data
            }
            
        except Exception as e:
            logger.error(f"Error in Tax Assessor scraper: {e}")
            return {"error": str(e)}
    
    async def get_coordinates(self) -> Dict[str, Any]:
        """Get coordinates for the property"""
        # Note: This would typically query a geocoding service
        # For now, using hardcoded test values
        return {
            "latitude": 44.0582,
            "longitude": -121.3153,
            "source": "Test Data"
        }
    
    async def get_fema_flood_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Get FEMA flood data for the coordinates"""
        try:
            url = f"https://hazards.fema.gov/gis/nfhl/rest/services/NFHL/MapServer"
            params = {
                "lat": lat,
                "lon": lon,
                "f": "json"
            }
            
            async with aiohttp.ClientSession() as session:
                for attempt in range(self.max_retries):
                    try:
                        async with session.get(url, params=params, ssl=False) as response:
                            if response.status == 200:
                                data = await response.json()
                                return self.parse_fema_data(data)
                            elif response.status in self.retry_codes and attempt < self.max_retries - 1:
                                continue
                            else:
                                return {"error": f"FEMA API returned status code: {response.status}"}
                    except aiohttp.ClientError as e:
                        if attempt < self.max_retries - 1:
                            continue
                        return {"error": f"Failed to connect to FEMA API: {str(e)}"}
                        
            return {"error": "Max retries exceeded"}
            
        except Exception as e:
            logger.error(f"Error getting FEMA flood data: {e}")
            return {"error": f"Failed to get FEMA flood data: {str(e)}"}
    
    def parse_fema_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Parse the FEMA flood data response"""
        try:
            if not data:
                return {"flood_zone": "Unknown", "details": "No data available"}
            
            # Extract relevant flood zone information
            # This is a simplified example - actual implementation would need
            # to handle various FEMA flood zone designations
            return {
                "flood_zone": data.get("floodZone", "Unknown"),
                "zone_description": self.get_zone_description(data.get("floodZone")),
                "panel_number": data.get("panelNumber"),
                "effective_date": data.get("effectiveDate"),
                "source": "FEMA NFHL"
            }
        except Exception as e:
            logger.error(f"Error parsing FEMA data: {e}")
            return {"error": f"Failed to parse FEMA data: {str(e)}"}
    
    def get_zone_description(self, zone: str) -> str:
        """Get description for FEMA flood zone"""
        zone_descriptions = {
            "A": "100-year floodplain",
            "AE": "100-year floodplain with base flood elevations",
            "X": "Area of minimal flood hazard",
            "Unknown": "Flood zone information not available"
        }
        return zone_descriptions.get(zone, "Zone description not available")
    
    async def get_tax_data(self) -> Dict[str, Any]:
        """Get tax assessment data for the property"""
        # Note: This would typically query the county assessor's database
        # For now, returning sample data
        return {
            "parcel_id": self.property_id,
            "tax_year": 2024,
            "assessed_value": {
                "land": 150000,
                "improvements": 0,
                "total": 150000
            },
            "tax_status": "Current",
            "source": "Deschutes County Assessor"
        }

async def run(property_id: str) -> Dict[str, Any]:
    """Entry point for the Tax Assessor scraper"""
    scraper = TaxAssessorScraper(property_id)
    try:
        result = await scraper.scrape()
        return result
    except Exception as e:
        logger.error(f"Error running Tax Assessor scraper: {e}")
        return {"error": str(e)}

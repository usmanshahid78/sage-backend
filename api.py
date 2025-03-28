from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any
import asyncio
from scrappers.utilityInformation import check_water_systems
from scrappers.deschutesDIAL import main as deschutes_main
from scrappers.designData import main as design_main
from scrappers.googleEarth import main as google_earth_main
from scrappers.planningData import main as planning_main

app = FastAPI(title="Property Data API", description="API for fetching property data from various sources")

class PropertyRequest(BaseModel):
    property_id: str
    address: str
    taxlot_id: Optional[str] = None

class PropertyResponse(BaseModel):
    basic_info: Dict[str, Any]
    utility_info: Dict[str, Any]
    design_data: Dict[str, Any]
    google_earth_data: Dict[str, Any]
    planning_data: Dict[str, Any]

@app.post("/property-data", response_model=PropertyResponse)
async def get_property_data(request: PropertyRequest):
    try:
        # Run all scrapers concurrently
        tasks = [
            asyncio.create_task(run_utility_info(request.property_id)),
            asyncio.create_task(run_deschutes_dial(request.property_id, request.taxlot_id)),
            asyncio.create_task(run_design_data(request.address)),
            asyncio.create_task(run_google_earth(request.address, request.property_id)),
            asyncio.create_task(run_planning_data(request.property_id, request.address))
        ]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Extract results
        utility_info, deschutes_data, design_data, google_earth_data, planning_data = results
        
        return PropertyResponse(
            basic_info=deschutes_data,
            utility_info=utility_info,
            design_data=design_data,
            google_earth_data=google_earth_data,
            planning_data=planning_data
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def run_utility_info(property_id: str) -> Dict[str, Any]:
    try:
        septic_result, well_result, power_type = check_water_systems(property_id)
        return {
            "septic_status": septic_result,
            "well_status": well_result,
            "power_type": power_type
        }
    except Exception as e:
        print(f"Error in utility info: {e}")
        return {}

async def run_deschutes_dial(property_id: str, taxlot_id: Optional[str] = None) -> Dict[str, Any]:
    try:
        if taxlot_id:
            result = deschutes_main(taxlot_id=taxlot_id)
        else:
            result = deschutes_main(property_id=property_id)
        return result
    except Exception as e:
        print(f"Error in Deschutes DIAL: {e}")
        return {}

async def run_design_data(address: str) -> Dict[str, Any]:
    try:
        result = design_main()
        return result
    except Exception as e:
        print(f"Error in design data: {e}")
        return {}

async def run_google_earth(address: str, property_id: str) -> Dict[str, Any]:
    try:
        result = google_earth_main()
        return result
    except Exception as e:
        print(f"Error in Google Earth data: {e}")
        return {}

async def run_planning_data(property_id: str, address: str) -> Dict[str, Any]:
    try:
        result = planning_main()
        return result
    except Exception as e:
        print(f"Error in planning data: {e}")
        return {}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from typing import List, Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database config
DB_HOST = os.getenv("DB_HOST")
DB_NAME = "sagedatabase"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

app = FastAPI(title="Sage Backend API", description="APIs to fetch property data from various tables")

class BasicInfo(BaseModel):
    id: str
    owner_name: Optional[str]
    owner_name_source: Optional[str]
    mailing_address: Optional[str]
    mailing_address_source: Optional[str]
    parcel_number: Optional[str]
    parcel_number_source: Optional[str]
    acres: Optional[str]
    acres_source: Optional[str]
    plat_map: Optional[str]
    plat_map_url: Optional[str]
    tax_map: Optional[str]
    tax_map_url: Optional[str]
    account: Optional[str]
    site_address: Optional[str]
    site_address_source: Optional[str]
    legal: Optional[str]
    legal_source: Optional[str]

class DesignData(BaseModel):
    id: str
    ground_snow_load: Optional[str]
    ground_snow_load_source: Optional[str]
    seismic_design_category: Optional[str]
    seismic_design_category_source: Optional[str]
    basic_wind_speed: Optional[str]
    basic_wind_speed_source: Optional[str]
    ultimate_wind_design_speed: Optional[str]
    ultimate_wind_design_speed_source: Optional[str]
    exposure: Optional[str]
    exposure_source: Optional[str]
    frost_depth: Optional[str]
    frost_depth_source: Optional[str]

class GoogleEarthInfo(BaseModel):
    property_id: str
    gps_coord: Optional[str]
    gps_coord_source: Optional[str]
    slope: Optional[str]
    slope_source: Optional[str]
    power_visible: Optional[bool]
    power_visible_source: Optional[str]
    existing_structures: Optional[bool]
    existing_structures_source: Optional[str]
    trees_brush: Optional[bool]
    trees_brush_source: Optional[str]

class PlanningData(BaseModel):
    property_id: str
    jurisdiction: Optional[str]
    fire_district: Optional[str]
    fire_district_source: Optional[str]
    zoning: Optional[str]
    overlay: Optional[str]
    max_lot_coverage: Optional[str]
    max_building_height: Optional[str]
    front_setback: Optional[str]
    side_setback: Optional[str]
    rear_setback: Optional[str]
    solar_setback: Optional[str]
    special_setback: Optional[str]
    easements: Optional[str]
    easements_source: Optional[str]
    liquefaction_hazard: Optional[str]
    landslide_hazard: Optional[str]
    geo_report_required: Optional[str]
    fema_flood_zone: Optional[str]
    hydric_soils_hazard: Optional[str]
    wetlands_on_property: Optional[str]
    erosion_control_required: Optional[str]
    stormwater_requirements: Optional[str]
    tree_preservation_reqs: Optional[str]
    special_fire_marshal_reqs: Optional[str]
    radon: Optional[str]
    sidewalks_required: Optional[str]
    approach_permit: Optional[str]

class UtilityDetails(BaseModel):
    id: str
    waste_water_type: Optional[str]
    waste_water_type_source: Optional[str]
    water_type: Optional[str]
    water_type_source: Optional[str]
    power_type: Optional[str]
    power_type_source: Optional[str]
    created_at: Optional[str]

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT
    )

@app.get("/basic-info/{property_id}", response_model=BasicInfo)
async def get_basic_info(property_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM basic_info WHERE id = %s", (property_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Basic info not found")
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        return BasicInfo(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

@app.get("/design-data/{property_id}", response_model=DesignData)
async def get_design_data(property_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM design_data WHERE id = %s", (property_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Design data not found")
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        return DesignData(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

@app.get("/google-earth-info/{property_id}", response_model=GoogleEarthInfo)
async def get_google_earth_info(property_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM google_earth_info WHERE property_id = %s", (property_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Google Earth info not found")
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        return GoogleEarthInfo(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

@app.get("/planning-data/{property_id}", response_model=PlanningData)
async def get_planning_data(property_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM planning_data WHERE property_id = %s", (property_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Planning data not found")
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        return PlanningData(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

@app.get("/utility-details/{property_id}", response_model=UtilityDetails)
async def get_utility_details(property_id: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM utility_details WHERE id = %s", (property_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Utility details not found")
        
        columns = [desc[0] for desc in cursor.description]
        data = dict(zip(columns, row))
        return UtilityDetails(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()

@app.get("/all-data/{property_id}")
async def get_all_data(property_id: str):
    try:
        basic_info = await get_basic_info(property_id)
        design_data = await get_design_data(property_id)
        google_earth_info = await get_google_earth_info(property_id)
        planning_data = await get_planning_data(property_id)
        utility_details = await get_utility_details(property_id)
        
        return {
            "basic_info": basic_info,
            "design_data": design_data,
            "google_earth_info": google_earth_info,
            "planning_data": planning_data,
            "utility_details": utility_details
        }
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 
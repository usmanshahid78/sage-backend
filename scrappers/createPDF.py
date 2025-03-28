from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from fastapi.responses import FileResponse
import os

# Database Configuration
DATABASE_URL = "postgresql://postgres:12345678@sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com:5432/sagedatabase"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define Table Models
class BasicInfo(Base):
    __tablename__ = "basic_info"
    id = Column(Integer, primary_key=True, index=True)
    owner_name = Column(String)
    owner_name_source = Column(String)
    mailing_address = Column(String)
    mailing_address_source = Column(String)
    parcel_number = Column(String)
    parcel_number_source = Column(String)
    legal = Column(String)
    legal_source = Column(String)
    account = Column(String)
    site_address = Column(String)
    site_address_source = Column(String)
    acres = Column(Float)
    acres_source = Column(String)
    plat_map = Column(Boolean)
    plat_map_url = Column(String)
    tax_map = Column(Boolean)
    tax_map_url = Column(String)

class GoogleEarthInfo(Base):
    __tablename__ = "google_earth_info"
    id = Column(String, primary_key=True, index=True)
    property_id = Column(String, ForeignKey("basic_info.id"))
    gps_coord = Column(String)
    gps_coord_source = Column(String)
    slope = Column(String)
    slope_source = Column(String)
    power_visible = Column(Boolean)
    power_visible_source = Column(String)
    existing_structures = Column(Boolean)
    existing_structures_source = Column(String)
    trees_brush = Column(Boolean)
    trees_brush_source = Column(String)

class DesignData(Base):
    __tablename__ = "design_data"
    id = Column(Integer, primary_key=True, index=True)
    ground_snow_load = Column(String)
    ground_snow_load_source = Column(String)
    seismic_design_category = Column(String)
    seismic_design_category_source = Column(String)
    basic_wind_speed = Column(String)
    basic_wind_speed_source = Column(String)
    ultimate_wind_design_speed = Column(String)
    ultimate_wind_design_speed_source = Column(String)
    exposure = Column(String)
    exposure_source = Column(String)
    frost_depth = Column(String)
    frost_depth_source = Column(String)

class UtilityDetails(Base):
    __tablename__ = "utility_details"
    id = Column(Integer, primary_key=True, index=True)
    waste_water_type = Column(String)
    waste_water_type_source = Column(String)
    water_type = Column(String)
    water_type_source = Column(String)
    power_type = Column(String)
    power_type_source = Column(String)

class PlanningData(Base):
    __tablename__ = "planning_data"
    id = Column(Integer, primary_key=True, index=True)
    property_id = Column(Integer, ForeignKey("basic_info.id"))
    jurisdiction = Column(String)
    fire_district = Column(String)
    zoning = Column(String)
    overlay = Column(String)
    max_lot_coverage = Column(String)
    max_building_height = Column(String)
    front_setback = Column(String)
    side_setback = Column(String)
    rear_setback = Column(String)
    solar_setback = Column(Boolean)
    special_setback = Column(String)
    easements = Column(String)
    liquefaction_hazard = Column(String)
    geo_report_required = Column(String)
    landslide_hazard = Column(String)
    fema_flood_zone = Column(String)
    hydric_soils_hazard = Column(String)
    wetlands_on_property = Column(String)
    erosion_control_required = Column(String)
    stormwater_requirements = Column(String)
    tree_preservation_reqs = Column(String)
    special_fire_marshal_reqs = Column(String)
    radon = Column(String)
    sidewalks_required = Column(String)
    approach_permit = Column(String)

# FastAPI App
app = FastAPI()

# Dependency to Get DB Session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Function to Generate PDF
def generate_pdf(data, pdf_path):
    c = canvas.Canvas(pdf_path, pagesize=letter)
    width, height = letter
    margin = 50
    line_height = 14  # Adjust line spacing

    def draw_text(x, y, text, bold=False, max_width=500):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 10)

        if len(str(text)) > 80:  
            lines = [str(text)[i:i+80] for i in range(0, len(str(text)), 80)]
        else:
            lines = [str(text)]

        for line in lines:
            if y <= margin:  # If near bottom, create a new page
                c.showPage()
                y = height - margin  # Reset y position

            c.drawString(x, y, line)
            y -= line_height

        return y - line_height  # Return updated y position

    y = height - margin

    # BASIC INFO
    y = draw_text(50, y, "BASIC INFO:", bold=True)
    for key, value in data["basic"].items():
        if "_source" not in key:
            y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
            source_key = f"{key}_source"
            if source_key in data["basic"] and data["basic"][source_key]:
                y = draw_text(50, y, f"Source: {data['basic'][source_key]}")

    # GOOGLE EARTH INFO
    if data["google"]:
        y = draw_text(50, y - 20, "GOOGLE EARTH INFO:", bold=True)
        for key, value in data["google"].items():
            if "_source" not in key:
                y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
                source_key = f"{key}_source"
                if source_key in data["google"] and data["google"][source_key]:
                    y = draw_text(50, y, f"Source: {data['google'][source_key]}")

    # DESIGN DATA
    if data["design"]:
        y = draw_text(50, y - 20, "DESIGN DATA:", bold=True)
        for key, value in data["design"].items():
            if "_source" not in key:
                y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
                source_key = f"{key}_source"
                if source_key in data["design"] and data["design"][source_key]:
                    y = draw_text(50, y, f"Source: {data['design'][source_key]}")

    # UTILITY DETAILS
    if data["utility"]:
        y = draw_text(50, y - 20, "UTILITY DETAILS:", bold=True)
        for key, value in data["utility"].items():
            if "_source" not in key:
                y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
                source_key = f"{key}_source"
                if source_key in data["utility"] and data["utility"][source_key]:
                    y = draw_text(50, y, f"Source: {data['utility'][source_key]}")

    # PLANNING DATA (No Sources)
    if data["planning"]:
        y = draw_text(50, y - 20, "PLANNING DATA:", bold=True)
        for key, value in data["planning"].items():
            y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")

    c.save()

# API Endpoint to Generate PDF and Return a Downloadable Link
@app.get("/generate-pdf/{property_id}")
def fetch_and_generate_pdf(property_id: int, db: Session = Depends(get_db)):
    basic = db.query(BasicInfo).filter(BasicInfo.id == property_id).first()
    google = db.query(GoogleEarthInfo).filter(GoogleEarthInfo.property_id == str(property_id)).first()
    design = db.query(DesignData).filter(DesignData.id == property_id).first()
    planning = db.query(PlanningData).filter(PlanningData.property_id == property_id).first()
    utility = db.query(UtilityDetails).filter(UtilityDetails.id == str(property_id)).first()

    if not basic:
        raise HTTPException(status_code=404, detail="Property ID not found")

    # Convert SQLAlchemy objects to dictionaries (to remove `sa Instance State`)
    def to_dict(obj):
        return {column.name: getattr(obj, column.name) for column in obj.__table__.columns} if obj else {}

    data = {
        "basic": to_dict(basic),
        "google": to_dict(google),
        "design": to_dict(design),
        "utility": to_dict(utility),
        "planning": to_dict(planning),
    }

    pdf_filename = f"property_report_{property_id}.pdf"
    pdf_path = os.path.join(os.getcwd(), pdf_filename)

    generate_pdf(data, pdf_path)

    return {"download_url": f"/download-pdf/{pdf_filename}"}

# Endpoint to Download PDF
@app.get("/download-pdf/{pdf_filename}")
def download_pdf(pdf_filename: str):
    pdf_path = os.path.join(os.getcwd(), pdf_filename)
    
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF not found")

    return FileResponse(pdf_path, media_type="application/pdf", filename=pdf_filename)

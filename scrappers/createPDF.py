from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Boolean, ForeignKey, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from fastapi.responses import FileResponse
import os

# Database Configuration
DATABASE_URL = "postgresql://postgres:12345678@sage-database.cbmq4e26s31g.eu-north-1.rds.amazonaws.com:5432/sagedatabase?client_encoding=utf8"

engine = create_engine(DATABASE_URL, connect_args={'options': '-c timezone=UTC'})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define Table Models
class BasicInfo(Base):
    __tablename__ = "basic_info"
    id = Column(String, primary_key=True, nullable=False)
    owner_name = Column(String, nullable=True)
    owner_name_source = Column(String, nullable=True)
    mailing_address = Column(String, nullable=True)
    mailing_address_source = Column(String, nullable=True)
    parcel_number = Column(String, nullable=True)
    parcel_number_source = Column(String, nullable=True)
    legal = Column(String, nullable=True)
    legal_source = Column(String, nullable=True)
    account = Column(String, nullable=True)
    site_address = Column(String, nullable=True)
    site_address_source = Column(String, nullable=True)
    acres = Column(String, nullable=True)
    acres_source = Column(String, nullable=True)
    plat_map = Column(String, nullable=True)
    plat_map_url = Column(String, nullable=True)
    tax_map = Column(String, nullable=True)
    tax_map_url = Column(String, nullable=True)

class GoogleEarthInfo(Base):
    __tablename__ = "google_earth_info"
    power_visible = Column(Boolean, nullable=True)
    trees_brush = Column(Boolean, nullable=True)
    existing_structures = Column(Boolean, nullable=True)
    slope = Column(String, nullable=True)
    slope_source = Column(String, nullable=True)
    power_visible_source = Column(String, nullable=True)
    existing_structures_source = Column(String, nullable=True)
    property_id = Column(String, primary_key=True, nullable=False)
    trees_brush_source = Column(String, nullable=True)
    gps_coord = Column(String, nullable=True)
    gps_coord_source = Column(String, nullable=True)

class DesignData(Base):
    __tablename__ = "design_data"
    id = Column(String, primary_key=True, nullable=False)
    ground_snow_load = Column(String, nullable=True)
    ground_snow_load_source = Column(String, nullable=True)
    seismic_design_category = Column(String, nullable=True)
    seismic_design_category_source = Column(String, nullable=True)
    basic_wind_speed = Column(String, nullable=True)
    basic_wind_speed_source = Column(String, nullable=True)
    ultimate_wind_design_speed = Column(String, nullable=True)
    ultimate_wind_design_speed_source = Column(String, nullable=True)
    exposure = Column(String, nullable=True)
    exposure_source = Column(String, nullable=True)
    frost_depth = Column(String, nullable=True)
    frost_depth_source = Column(String, nullable=True)

class UtilityDetails(Base):
    __tablename__ = "utility_details"
    id = Column(String, primary_key=True, nullable=False)
    created_at = Column(DateTime, nullable=True)
    waste_water_type = Column(String, nullable=True)
    waste_water_type_source = Column(String, nullable=True)
    water_type = Column(String, nullable=True)
    power_type = Column(String, nullable=True)
    power_type_source = Column(String, nullable=True)
    water_type_source = Column(String, nullable=True)

class PlanningData(Base):
    __tablename__ = "planning_data"
    property_id = Column(String, primary_key=True, nullable=False)
    jurisdiction = Column(String, nullable=True)
    fire_district = Column(String, nullable=True)
    zoning = Column(String, nullable=True)
    overlay = Column(String, nullable=True)
    max_lot_coverage = Column(String, nullable=True)
    max_building_height = Column(String, nullable=True)
    front_setback = Column(String, nullable=True)
    side_setback = Column(String, nullable=True)
    rear_setback = Column(String, nullable=True)
    solar_setback = Column(String, nullable=True)
    special_setback = Column(String, nullable=True)
    liquefaction_hazard = Column(String, nullable=True)
    landslide_hazard = Column(String, nullable=True)
    geo_report_required = Column(String, nullable=True)
    fema_flood_zone = Column(String, nullable=True)
    hydric_soils_hazard = Column(String, nullable=True)
    wetlands_on_property = Column(String, nullable=True)
    erosion_control_required = Column(String, nullable=True)
    stormwater_requirements = Column(String, nullable=True)
    tree_preservation_reqs = Column(String, nullable=True)
    special_fire_marshal_reqs = Column(String, nullable=True)
    radon = Column(String, nullable=True)
    sidewalks_required = Column(String, nullable=True)
    approach_permit = Column(String, nullable=True)
    fire_district_source = Column(String, nullable=True)
    easements = Column(String, nullable=True)
    easements_source = Column(String, nullable=True)

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
    line_height = 14

    def draw_text(x, y, text, bold=False, max_width=500):
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 10)

        if len(str(text)) > 80:  
            lines = [str(text)[i:i+80] for i in range(0, len(str(text)), 80)]
        else:
            lines = [str(text)]

        for line in lines:
            if y <= margin:
                c.showPage()
                y = height - margin

            c.drawString(x, y, line)
            y -= line_height

        return y - line_height

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
            if key != "created_at" and "_source" not in key:
                y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
                source_key = f"{key}_source"
                if source_key in data["utility"] and data["utility"][source_key]:
                    y = draw_text(50, y, f"Source: {data['utility'][source_key]}")

    # PLANNING DATA
    if data["planning"]:
        y = draw_text(50, y - 20, "PLANNING DATA:", bold=True)
        for key, value in data["planning"].items():
            if "_source" not in key and key != "property_id":
                y = draw_text(50, y, f"{key.replace('_', ' ').title()}: {value}")
                source_key = f"{key}_source"
                if source_key in data["planning"] and data["planning"][source_key]:
                    y = draw_text(50, y, f"Source: {data['planning'][source_key]}")

    c.save()

# API Endpoint to Generate PDF and Return a Downloadable Link
@app.get("/generate-pdf/{property_id}")
def fetch_and_generate_pdf(property_id: str, db: Session = Depends(get_db)):
    try:
        # Ensure property_id is a string for database queries
        property_id = str(property_id)
        
        # Use text() to force explicit casting in the queries
        basic = db.query(BasicInfo).filter(text("basic_info.id::text = :property_id")).params(property_id=property_id).first()
        google = db.query(GoogleEarthInfo).filter(text("google_earth_info.property_id::text = :property_id")).params(property_id=property_id).first()
        design = db.query(DesignData).filter(text("design_data.id::text = :property_id")).params(property_id=property_id).first()
        planning = db.query(PlanningData).filter(text("planning_data.property_id::text = :property_id")).params(property_id=property_id).first()
        utility = db.query(UtilityDetails).filter(text("utility_details.id::text = :property_id")).params(property_id=property_id).first()

        if not basic:
            raise HTTPException(status_code=404, detail="Property ID not found")

        def to_dict(obj):
            if not obj:
                return {}
            return {column.name: str(getattr(obj, column.name)) if getattr(obj, column.name) is not None else None 
                   for column in obj.__table__.columns}

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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating PDF: {str(e)}")

# Endpoint to Download PDF
@app.get("/download-pdf/{pdf_filename}")
def download_pdf(pdf_filename: str):
    pdf_path = os.path.join(os.getcwd(), pdf_filename)
    
    if not os.path.exists(pdf_path):
        raise HTTPException(status_code=404, detail="PDF not found")

    return FileResponse(pdf_path, media_type="application/pdf", filename=pdf_filename)

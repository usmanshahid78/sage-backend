from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from unifiedAPI import app as unified_app, TaxlotRequest, run_pipeline
from createPDF import app as pdf_app, fetch_and_generate_pdf, download_pdf, get_db
from sqlalchemy.orm import Session
from sqlalchemy import text

# Create the main FastAPI app
app = FastAPI(title="Sage Property Analysis API")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://51.20.54.131:3000"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Welcome to Sage Property Analysis API",
        "endpoints": {
            "unified_api": "/get-property-info",
            "pdf_api": "/generate-pdf/{property_id}"
        }
    }

# Include the unified API endpoints
@app.post("/get-property-info")
async def run_analysis(req: TaxlotRequest):
    return run_pipeline(req)

# Include the PDF API endpoints
@app.get("/generate-pdf/{property_id}")
async def generate_pdf(property_id: int, db=Depends(get_db)):
    return fetch_and_generate_pdf(property_id, db)

@app.get("/download-pdf/{pdf_filename}")
async def get_pdf(pdf_filename: str):
    return download_pdf(pdf_filename)

@app.get("/get-property-id/{parcel_number}")
async def get_property_id(parcel_number: str, db: Session = Depends(get_db)):
    try:
        # Query to get the ID from basic_info table using parcel number
        query = text("SELECT id FROM basic_info WHERE parcel_number = :parcel_number")
        result = db.execute(query, {"parcel_number": parcel_number}).first()
        
        if not result:
            raise HTTPException(status_code=404, detail="Property not found")
            
        return {"id": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True) 
from flask import Flask, request, jsonify
from scrappers.deschutesDIAL import main as dial_main
from scrappers.designData import main as design_main
from scrappers.googleEarth import main as google_earth_main
from scrappers.planningData import run_all_planning_steps
from scrappers.utilityInformation import check_water_systems

app = Flask(__name__)

@app.route("/run-all", methods=["POST"])
def run_all():
    data = request.get_json()

    property_id = data.get("property_id")
    address = data.get("address")
    soil_pdf_path = data.get("soil_pdf_path", "scrappers/soil_report.pdf")
    design_pdf_url = data.get("design_pdf_url", "https://www.deschutes.org/sites/default/files/fileattachments/community_development/page/679/design_requirements_for_the_entire_county_2.pdf")

    if not property_id or not address:
        return jsonify({"error": "Missing required 'property_id' or 'address'"}), 400

    results = {}

    try:
        # 1. DIAL Data
        print("📦 Running deschutesDIAL...")
        dial_data = dial_main(property_id=property_id)
        results["dial_data"] = dial_data or {}

        # 2. Design Data
        print("📐 Running designData...")
        design_data = design_main(address, design_pdf_url)
        results["design_data"] = design_data or {}

        # 3. Google Earth Analysis
        print("🌍 Running googleEarth...")
        google_earth_data = google_earth_main(property_id, address)
        results["google_earth_data"] = google_earth_data or {}

        # 4. Planning Info
        print("📋 Running planningData...")
        planning_info = run_all_planning_steps(property_id, address, soil_pdf_path)
        results["planning_data"] = planning_info or {}

        # 5. Utility Details
        print("💧 Running utilityInformation...")
        septic, well, power = check_water_systems(str(property_id))
        results["utility_data"] = {
            "septic": septic,
            "well": well,
            "power": power
        }

        return jsonify({
            "status": "Success",
            "property_id": property_id,
            "address": address,
            "results": results
        })

    except Exception as e:
        return jsonify({
            "status": "Error",
            "message": str(e)
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True) 
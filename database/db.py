import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure all required env variables are present
required_env_vars = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASS", "DB_PORT"]
if not all(os.getenv(var) for var in required_env_vars):
    raise EnvironmentError("One or more environment variables for database connection are missing.")

DB_HOST = os.getenv("DB_HOST")
# DB_NAME = os.getenv("DB_NAME")  # Make sure your .env contains "sagedatabase"
DB_NAME = "sagedatabase"
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")

def get_db_connection():
    """Establish and return a PostgreSQL database connection"""
    print('🔄 Trying to connect to the database...')
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        print("✅ Connected to the database successfully!")
        return conn
    except Exception as e:
        print("❌ Error connecting to the database:", e)
        return None

def create_tables(conn):
    """Create tables in the PostgreSQL database"""
    with conn.cursor() as cursor:
        cursor.execute("""
        -- Property Information
        CREATE TABLE IF NOT EXISTS property_information (
            property_id SERIAL PRIMARY KEY,
            parcel_number VARCHAR(50),
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            zip_code VARCHAR(20),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            owner_name VARCHAR(100),
            assessed_value DECIMAL(12,2),
            tax_value DECIMAL(12,2),
            last_sold_price DECIMAL(12,2),
            last_sold_date DATE
        );

        -- Zoning Data
        CREATE TABLE IF NOT EXISTS zoning_data (
            zoning_id SERIAL PRIMARY KEY,
            property_id INT REFERENCES property_information(property_id) ON DELETE CASCADE,
            zoning_code VARCHAR(50),
            zoning_description TEXT,
            land_use_type VARCHAR(50),
            setbacks_front DOUBLE PRECISION,
            setback_side DOUBLE PRECISION,
            height_restrictions DOUBLE PRECISION,
            density_restrictions DOUBLE PRECISION
        );

        -- Environmental & Flood Data
        CREATE TABLE IF NOT EXISTS environmental_flood_data (
            env_flood_id SERIAL PRIMARY KEY,
            property_id INT REFERENCES property_information(property_id) ON DELETE CASCADE,
            flood_zone_type VARCHAR(50),
            base_flood_elevation DOUBLE PRECISION,
            special_flood_hazard_area BOOLEAN,
            epa_air_quality_index DOUBLE PRECISION,
            epa_water_quality_index DOUBLE PRECISION,
            hazardous_waste_presence BOOLEAN -- Fixed typo
        );

        -- Permit & Land Use Approval
        CREATE TABLE IF NOT EXISTS permit_land_use_approval (
            permit_id SERIAL PRIMARY KEY,
            property_id INT REFERENCES property_information(property_id) ON DELETE CASCADE,
            permit_type VARCHAR(50),
            approval_type VARCHAR(50),
            date_issued DATE,
            date_expired DATE,
            permit_document_url VARCHAR(255)
        );

        -- AI Processed Data
        CREATE TABLE IF NOT EXISTS ai_processed_data (
            ai_result_id SERIAL PRIMARY KEY,
            property_id INT REFERENCES property_information(property_id) ON DELETE CASCADE,
            extracted_text TEXT,
            confidence_score DOUBLE PRECISION,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ai_analysis_summary TEXT,
            related_document_url VARCHAR(255)
        );

        -- User Activity Audit Logs
        CREATE TABLE IF NOT EXISTS user_activity_audit_logs (
            log_id SERIAL PRIMARY KEY,
            user_id INT,
            action_performed TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            affected_record_id INT,
            change_summary TEXT
        );
        """)
        conn.commit()
        print("✅ Tables created successfully!")

# Establish connection
conn = get_db_connection()
if conn:
    create_tables(conn)
    conn.close()

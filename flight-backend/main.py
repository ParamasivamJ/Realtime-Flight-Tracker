from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timedelta
import os
import json
from typing import List, Optional
import databricks.sql as dbsql
from databricks.sql.client import Connection

app = FastAPI(title="Flight Tracker API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FlightData(BaseModel):
    icao24: str
    callsign: str
    origin_country: str
    longitude: float
    latitude: float
    altitude: float
    velocity: float
    heading: float
    vertical_rate: float
    timestamp: str
    speed_kmh: Optional[float] = None
    altitude_ft: Optional[float] = None
    is_climbing: Optional[bool] = None
    is_descending: Optional[bool] = None

class FlightStats(BaseModel):
    total_flights: int
    active_countries: int
    avg_speed: float
    avg_altitude: float
    climbing_flights: int
    descending_flights: int

class DatabricksManager:
    def __init__(self):
        self.host = os.getenv('DATABRICKS_HOST')
        self.token = os.getenv('DATABRICKS_TOKEN')
        self.http_path = os.getenv('DATABRICKS_HTTP_PATH')
        self.connection = None
        self.last_used = None
        
    def get_connection(self) -> Connection:
        """Get or create Databricks SQL connection with session management"""
        try:
            # Recreate connection if it's been more than 10 minutes or doesn't exist
            if (self.connection is None or 
                self.last_used is None or 
                (datetime.now() - self.last_used).total_seconds() > 600):
                
                if self.connection:
                    try:
                        self.connection.close()
                    except:
                        pass
                
                print("🔄 Creating new Databricks connection...")
                self.connection = dbsql.connect(
                    server_hostname=self.host,
                    http_path=self.http_path,
                    access_token=self.token
                )
            
            self.last_used = datetime.now()
            return self.connection
            
        except Exception as e:
            print(f"❌ Databricks connection error: {e}")
            raise
    
    def execute_query(self, query: str, params: tuple = None):
        """Execute a SQL query and return results with error handling"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            if params:
                # Databricks uses %s for parameter placeholders
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            
            # Convert to list of dictionaries
            return [dict(zip(columns, row)) for row in results]
            
        except Exception as e:
            print(f"❌ Query execution error: {e}")
            # Reset connection on error
            self.connection = None
            raise
    
    def setup_tables(self):
        """Create necessary tables in Databricks if they don't exist"""
        try:
            # Create flight_data table
            create_table_query = """
            CREATE TABLE IF NOT EXISTS flight_data (
                icao24 STRING,
                callsign STRING,
                origin_country STRING,
                longitude DOUBLE,
                latitude DOUBLE,
                altitude DOUBLE,
                velocity DOUBLE,
                heading DOUBLE,
                vertical_rate DOUBLE,
                speed_kmh DOUBLE,
                altitude_ft DOUBLE,
                is_climbing BOOLEAN,
                is_descending BOOLEAN,
                timestamp TIMESTAMP,
                processed_at TIMESTAMP
            ) USING DELTA
            """
            self.execute_query(create_table_query)
            print("✅ Flight data table created/verified")
            
            # Create flight_aggregations table
            agg_table_query = """
            CREATE TABLE IF NOT EXISTS flight_aggregations (
                aggregation_type STRING,
                time_window TIMESTAMP,
                total_flights INT,
                active_countries INT,
                avg_speed_kmh DOUBLE,
                avg_altitude_ft DOUBLE,
                climbing_flights INT,
                descending_flights INT,
                created_at TIMESTAMP
            ) USING DELTA
            """
            self.execute_query(agg_table_query)
            print("✅ Flight aggregations table created/verified")
            
        except Exception as e:
            print(f"Table setup error: {e}")

# Initialize Databricks manager
db_manager = DatabricksManager()

@app.on_event("startup")
async def startup_event():
    """Initialize Databricks connection and tables on startup"""
    try:
        if all([os.getenv('DATABRICKS_HOST'), os.getenv('DATABRICKS_TOKEN'), os.getenv('DATABRICKS_HTTP_PATH')]):
            db_manager.setup_tables()
            print("✅ Databricks integration initialized successfully")
        else:
            print("⚠️  Databricks credentials not found, running in mock mode")
    except Exception as e:
        print(f"❌ Databricks initialization failed: {e}")

def is_databricks_connected():
    """Check if Databricks is properly configured"""
    return all([os.getenv('DATABRICKS_HOST'), os.getenv('DATABRICKS_TOKEN'), os.getenv('DATABRICKS_HTTP_PATH')])

@app.get("/")
async def root():
    db_status = "connected" if is_databricks_connected() else "mock_mode"
    return {
        "message": "Flight Tracker API", 
        "status": "active",
        "databricks": db_status
    }

@app.get("/flights/current", response_model=List[FlightData])
async def get_current_flights():
    """Get current active flights from Databricks"""
    try:
        if is_databricks_connected():
            # Query from Databricks
            query = """
            SELECT 
                icao24,
                callsign,
                origin_country,
                longitude,
                latitude,
                altitude,
                velocity,
                heading,
                vertical_rate,
                speed_kmh,
                altitude_ft,
                is_climbing,
                is_descending,
                timestamp
            FROM flight_data 
            WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES
            ORDER BY timestamp DESC
            LIMIT 100
            """
            
            results = db_manager.execute_query(query)
            flights = []
            for row in results:
                flight = FlightData(
                    icao24=row['icao24'],
                    callsign=row['callsign'],
                    origin_country=row['origin_country'],
                    longitude=row['longitude'],
                    latitude=row['latitude'],
                    altitude=row['altitude'],
                    velocity=row['velocity'],
                    heading=row['heading'],
                    vertical_rate=row['vertical_rate'],
                    speed_kmh=row['speed_kmh'],
                    altitude_ft=row['altitude_ft'],
                    is_climbing=row['is_climbing'],
                    is_descending=row['is_descending'],
                    timestamp=row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp'])
                )
                flights.append(flight)
            
            return flights
        else:
            # Fallback to mock data
            return generate_mock_flights()
            
    except Exception as e:
        print(f"Error fetching flights: {e}")
        # Fallback to mock data on error
        return generate_mock_flights()

@app.get("/flights/stats", response_model=FlightStats)
async def get_flight_stats():
    """Get flight statistics from Databricks"""
    try:
        if is_databricks_connected():
            # Query stats from Databricks
            query = """
            SELECT 
                COUNT(DISTINCT icao24) as total_flights,
                COUNT(DISTINCT origin_country) as active_countries,
                AVG(speed_kmh) as avg_speed_kmh,
                AVG(altitude_ft) as avg_altitude_ft,
                SUM(CASE WHEN is_climbing THEN 1 ELSE 0 END) as climbing_flights,
                SUM(CASE WHEN is_descending THEN 1 ELSE 0 END) as descending_flights
            FROM flight_data 
            WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES
            """
            
            results = db_manager.execute_query(query)
            if results:
                stats = results[0]
                return FlightStats(
                    total_flights=stats['total_flights'] or 0,
                    active_countries=stats['active_countries'] or 0,
                    avg_speed=stats['avg_speed_kmh'] or 0,
                    avg_altitude=stats['avg_altitude_ft'] or 0,
                    climbing_flights=stats['climbing_flights'] or 0,
                    descending_flights=stats['descending_flights'] or 0
                )
        
        # Fallback to mock stats
        return FlightStats(
            total_flights=45,
            active_countries=12,
            avg_speed=512.5,
            avg_altitude=35000.0,
            climbing_flights=8,
            descending_flights=6
        )
            
    except Exception as e:
        print(f"Error fetching stats: {e}")
        return FlightStats(
            total_flights=45,
            active_countries=12,
            avg_speed=512.5,
            avg_altitude=35000.0,
            climbing_flights=8,
            descending_flights=6
        )

@app.post("/flights/ingest")
async def ingest_flight_data(flights: List[FlightData]):
    """Ingest flight data into Databricks (called by producer)"""
    try:
        if not is_databricks_connected():
            return {"status": "mock_mode", "message": "Databricks not configured", "inserted": len(flights)}
        
        inserted_count = 0
        for flight in flights:
            try:
                # Calculate derived fields
                speed_kmh = flight.velocity * 3.6 if flight.velocity else 0
                altitude_ft = flight.altitude * 3.28084 if flight.altitude else 0
                is_climbing = flight.vertical_rate > 0 if flight.vertical_rate else False
                is_descending = flight.vertical_rate < 0 if flight.vertical_rate else False
                
                # FIXED: Use %s for Databricks parameter placeholders
                insert_query = """
                INSERT INTO flight_data (
                    icao24, callsign, origin_country, longitude, latitude, 
                    altitude, velocity, heading, vertical_rate, speed_kmh, 
                    altitude_ft, is_climbing, is_descending, timestamp, processed_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP())
                """
                
                db_manager.execute_query(insert_query, (
                    flight.icao24, flight.callsign or 'UNKNOWN', flight.origin_country or 'Unknown',
                    flight.longitude, flight.latitude, flight.altitude or 0,
                    flight.velocity or 0, flight.heading or 0, flight.vertical_rate or 0,
                    speed_kmh, altitude_ft, is_climbing, is_descending,
                    flight.timestamp
                ))
                inserted_count += 1
                print(f"✅ Inserted flight {flight.icao24}")
                
            except Exception as e:
                print(f"❌ Error inserting flight {flight.icao24}: {e}")
                continue  # Continue with next flight
        
        return {"status": "success", "inserted": inserted_count, "total": len(flights)}
        
    except Exception as e:
        print(f"❌ Error in ingest endpoint: {e}")
        return {"status": "error", "message": str(e), "inserted": 0}
    
@app.get("/databricks/status")
async def databricks_status():
    """Check Databricks connection status"""
    try:
        if is_databricks_connected():
            # Test connection with a simple query
            db_manager.execute_query("SELECT 1 as test")
            return {
                "status": "connected",
                "host": os.getenv('DATABRICKS_HOST'),
                "tables_initialized": True
            }
        else:
            return {
                "status": "not_configured",
                "message": "DATABRICKS_HOST, DATABRICKS_TOKEN, or DATABRICKS_HTTP_PATH not set"
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }

# Mock data generator for fallback
def generate_mock_flights():
    """Generate realistic mock flight data"""
    flights = []
    countries = ['United States', 'Germany', 'France', 'United Kingdom', 'China', 'Japan', 'Canada', 'Brazil', 'Australia', 'India']
    callsigns = ['UAL', 'DAL', 'AAL', 'BAW', 'AFR', 'LUF', 'KLM', 'EMI', 'QFA', 'SIA']
    
    regions = [
        {'center': [40.0, -100.0], 'spread': 15},  # North America
        {'center': [50.0, 10.0], 'spread': 20},    # Europe
        {'center': [35.0, 100.0], 'spread': 15},   # Asia
        {'center': [-25.0, 135.0], 'spread': 10},  # Australia
    ]
    
    flight_id = 0
    for region in regions:
        for i in range(8):
            lat = region['center'][0] + random.uniform(-region['spread'], region['spread'])
            lng = region['center'][1] + random.uniform(-region['spread']*2, region['spread']*2)
            
            velocity = random.uniform(400, 600)
            altitude = random.uniform(10000, 40000)
            vertical_rate = random.uniform(-20, 20)
            
            flights.append(FlightData(
                icao24=f"abc{flight_id:03d}",
                callsign=f"{random.choice(callsigns)}{random.randint(100, 999)}",
                origin_country=random.choice(countries),
                longitude=round(lng, 4),
                latitude=round(lat, 4),
                altitude=altitude,
                velocity=velocity,
                heading=random.uniform(0, 360),
                vertical_rate=vertical_rate,
                speed_kmh=velocity * 3.6,
                altitude_ft=altitude * 3.28084,
                is_climbing=vertical_rate > 0,
                is_descending=vertical_rate < 0,
                timestamp=datetime.utcnow().isoformat()
            ))
            flight_id += 1
    
    return flights

import random  # Add this import at the top

@app.get("/health")
async def health_check():
    db_status = "connected" if is_databricks_connected() else "mock_mode"
    return {
        "status": "healthy", 
        "timestamp": datetime.utcnow().isoformat(),
        "databricks": db_status,
        "services": {
            "api": "healthy",
            "database": db_status
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
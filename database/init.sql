-- =============================================
-- Database Initialization Script
-- Supports both existing log system and new flight tracker
-- =============================================

-- Enable UUID extension if not already enabled
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =============================================
-- 1. EXISTING LOG SYSTEM TABLES (Your current setup)
-- =============================================

-- Log entries table (your existing table)
CREATE TABLE IF NOT EXISTS log_entries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    service_name VARCHAR(100),
    metadata JSONB
);

-- Indexes for log queries
CREATE INDEX IF NOT EXISTS idx_log_entries_timestamp ON log_entries(timestamp);
CREATE INDEX IF NOT EXISTS idx_log_entries_level ON log_entries(level);
CREATE INDEX IF NOT EXISTS idx_log_entries_service ON log_entries(service_name);
CREATE INDEX IF NOT EXISTS idx_log_entries_metadata ON log_entries USING GIN(metadata);

-- =============================================
-- 2. FLIGHT TRACKING SYSTEM TABLES
-- =============================================

-- Raw flight data from OpenSky API
CREATE TABLE IF NOT EXISTS flight_data_raw (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    icao24 VARCHAR(10) NOT NULL,
    callsign VARCHAR(10),
    origin_country VARCHAR(100),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    squawk VARCHAR(10),
    spi BOOLEAN,
    position_source INTEGER,
    time_position BIGINT,
    last_contact BIGINT,
    on_ground BOOLEAN,
    sensors JSONB,
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    batch_id UUID DEFAULT uuid_generate_v4()
);

-- Processed flight data (enriched)
CREATE TABLE IF NOT EXISTS flight_data_processed (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    icao24 VARCHAR(10) NOT NULL,
    callsign VARCHAR(10),
    origin_country VARCHAR(100),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    baro_altitude DOUBLE PRECISION,
    geo_altitude DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    vertical_rate DOUBLE PRECISION,
    
    -- Enriched fields
    speed_kmh DOUBLE PRECISION,
    altitude_ft DOUBLE PRECISION,
    is_climbing BOOLEAN,
    is_descending BOOLEAN,
    flight_status VARCHAR(20),
    
    -- Geolocation fields
    continent VARCHAR(50),
    region VARCHAR(100),
    
    -- Timestamps
    time_position TIMESTAMP,
    last_contact TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Flight aggregations for dashboard
CREATE TABLE IF NOT EXISTS flight_aggregations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregation_type VARCHAR(50) NOT NULL,
    time_window TIMESTAMP NOT NULL,
    total_flights INTEGER,
    active_countries INTEGER,
    avg_speed_kmh DOUBLE PRECISION,
    avg_altitude_ft DOUBLE PRECISION,
    climbing_flights INTEGER,
    descending_flights INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft metadata (static data)
CREATE TABLE IF NOT EXISTS aircraft_metadata (
    icao24 VARCHAR(10) PRIMARY KEY,
    registration VARCHAR(20),
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    owner VARCHAR(200),
    operator VARCHAR(200),
    built_year INTEGER,
    engine_type VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Airport reference data
CREATE TABLE IF NOT EXISTS airports (
    icao VARCHAR(10) PRIMARY KEY,
    iata VARCHAR(10),
    name VARCHAR(200) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    altitude DOUBLE PRECISION,
    timezone VARCHAR(50)
);

-- Flight routes (if available)
CREATE TABLE IF NOT EXISTS flight_routes (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    callsign VARCHAR(10),
    icao24 VARCHAR(10),
    departure_icao VARCHAR(10),
    arrival_icao VARCHAR(10),
    estimated_departure TIMESTAMP,
    estimated_arrival TIMESTAMP,
    actual_departure TIMESTAMP,
    actual_arrival TIMESTAMP,
    route TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- 3. SYSTEM METRICS AND MONITORING TABLES
-- =============================================

-- Pipeline monitoring
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    component VARCHAR(50) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION,
    records_processed INTEGER,
    processing_time_ms INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- API usage tracking
CREATE TABLE IF NOT EXISTS api_usage (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_name VARCHAR(50) NOT NULL,
    endpoint VARCHAR(100),
    request_count INTEGER,
    error_count INTEGER,
    avg_response_time_ms DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- 4. INDEXES FOR PERFORMANCE
-- =============================================

-- Flight data indexes
CREATE INDEX IF NOT EXISTS idx_flight_raw_icao24 ON flight_data_raw(icao24);
CREATE INDEX IF NOT EXISTS idx_flight_raw_timestamp ON flight_data_raw(created_at);
CREATE INDEX IF NOT EXISTS idx_flight_raw_coords ON flight_data_raw(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_flight_raw_callsign ON flight_data_raw(callsign);

CREATE INDEX IF NOT EXISTS idx_flight_proc_icao24 ON flight_data_processed(icao24);
CREATE INDEX IF NOT EXISTS idx_flight_proc_timestamp ON flight_data_processed(processed_at);
CREATE INDEX IF NOT EXISTS idx_flight_proc_coords ON flight_data_processed(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_flight_proc_country ON flight_data_processed(origin_country);
CREATE INDEX IF NOT EXISTS idx_flight_proc_status ON flight_data_processed(flight_status);

-- Aggregation indexes
CREATE INDEX IF NOT EXISTS idx_aggregations_window ON flight_aggregations(time_window);
CREATE INDEX IF NOT EXISTS idx_aggregations_type ON flight_aggregations(aggregation_type);

-- Route indexes
CREATE INDEX IF NOT EXISTS idx_routes_callsign ON flight_routes(callsign);
CREATE INDEX IF NOT EXISTS idx_routes_departure ON flight_routes(departure_icao);
CREATE INDEX IF NOT EXISTS idx_routes_arrival ON flight_routes(arrival_icao);

-- =============================================
-- 5. VIEWS FOR COMMON QUERIES
-- =============================================

-- Current active flights view
CREATE OR REPLACE VIEW current_active_flights AS
SELECT 
    fd.icao24,
    fd.callsign,
    fd.origin_country,
    fd.latitude,
    fd.longitude,
    fd.altitude_ft,
    fd.speed_kmh,
    fd.heading,
    fd.is_climbing,
    fd.is_descending,
    fd.processed_at,
    am.manufacturer,
    am.model,
    am.registration
FROM flight_data_processed fd
LEFT JOIN aircraft_metadata am ON fd.icao24 = am.icao24
WHERE fd.processed_at >= NOW() - INTERVAL '10 minutes'
ORDER BY fd.processed_at DESC;

-- Flight statistics view
CREATE OR REPLACE VIEW flight_statistics AS
SELECT 
    COUNT(DISTINCT icao24) as total_active_flights,
    COUNT(DISTINCT origin_country) as active_countries,
    AVG(speed_kmh) as avg_speed_kmh,
    AVG(altitude_ft) as avg_altitude_ft,
    SUM(CASE WHEN is_climbing THEN 1 ELSE 0 END) as climbing_flights,
    SUM(CASE WHEN is_descending THEN 1 ELSE 0 END) as descending_flights,
    MAX(processed_at) as last_update
FROM flight_data_processed
WHERE processed_at >= NOW() - INTERVAL '10 minutes';

-- Country flight count view
CREATE OR REPLACE VIEW country_flight_counts AS
SELECT 
    origin_country,
    COUNT(DISTINCT icao24) as flight_count,
    AVG(speed_kmh) as avg_speed,
    AVG(altitude_ft) as avg_altitude
FROM flight_data_processed
WHERE processed_at >= NOW() - INTERVAL '10 minutes'
GROUP BY origin_country
ORDER BY flight_count DESC;

-- =============================================
-- 6. STORED PROCEDURES AND FUNCTIONS
-- =============================================

-- Function to calculate distance between two points (Haversine formula)
CREATE OR REPLACE FUNCTION calculate_distance(
    lat1 DOUBLE PRECISION,
    lon1 DOUBLE PRECISION,
    lat2 DOUBLE PRECISION,
    lon2 DOUBLE PRECISION
) RETURNS DOUBLE PRECISION AS $$
DECLARE
    R INTEGER := 6371; -- Earth radius in kilometers
    dlat DOUBLE PRECISION;
    dlon DOUBLE PRECISION;
    a DOUBLE PRECISION;
    c DOUBLE PRECISION;
BEGIN
    dlat := radians(lat2 - lat1);
    dlon := radians(lon2 - lon1);
    
    a := sin(dlat/2) * sin(dlat/2) + 
         cos(radians(lat1)) * cos(radians(lat2)) * 
         sin(dlon/2) * sin(dlon/2);
    
    c := 2 * atan2(sqrt(a), sqrt(1-a));
    
    RETURN R * c;
END;
$$ LANGUAGE plpgsql;

-- Procedure to update flight aggregations
CREATE OR REPLACE PROCEDURE update_flight_aggregations()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Delete old aggregations (keep last 24 hours)
    DELETE FROM flight_aggregations 
    WHERE time_window < NOW() - INTERVAL '24 hours';
    
    -- Insert new 5-minute aggregation
    INSERT INTO flight_aggregations (
        aggregation_type,
        time_window,
        total_flights,
        active_countries,
        avg_speed_kmh,
        avg_altitude_ft,
        climbing_flights,
        descending_flights
    )
    SELECT 
        '5min' as aggregation_type,
        DATE_TRUNC('minute', NOW()) as time_window,
        COUNT(DISTINCT icao24) as total_flights,
        COUNT(DISTINCT origin_country) as active_countries,
        AVG(speed_kmh) as avg_speed_kmh,
        AVG(altitude_ft) as avg_altitude_ft,
        SUM(CASE WHEN is_climbing THEN 1 ELSE 0 END) as climbing_flights,
        SUM(CASE WHEN is_descending THEN 1 ELSE 0 END) as descending_flights
    FROM flight_data_processed
    WHERE processed_at >= NOW() - INTERVAL '5 minutes';
    
    -- Log the aggregation
    INSERT INTO pipeline_metrics (
        component,
        metric_name,
        metric_value,
        records_processed,
        status
    ) VALUES (
        'aggregation',
        'flight_aggregations_updated',
        1,
        1,
        'success'
    );
    
EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO pipeline_metrics (
            component,
            metric_name,
            metric_value,
            status,
            error_message
        ) VALUES (
            'aggregation',
            'flight_aggregations_updated',
            0,
            'error',
            SQLERRM
        );
END;
$$;

-- Function to clean old flight data
CREATE OR REPLACE FUNCTION clean_old_flight_data(retention_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM flight_data_raw 
    WHERE created_at < NOW() - (retention_days || ' days')::INTERVAL;
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================
-- 7. SAMPLE DATA INSERTION (Optional)
-- =============================================

-- Insert sample airport data
INSERT INTO airports (icao, iata, name, city, country, latitude, longitude, altitude, timezone) VALUES
('KJFK', 'JFK', 'John F Kennedy International Airport', 'New York', 'United States', 40.639801, -73.7789, 4, 'America/New_York'),
('EGLL', 'LHR', 'Heathrow Airport', 'London', 'United Kingdom', 51.4706, -0.461941, 25, 'Europe/London'),
('LFPG', 'CDG', 'Charles de Gaulle Airport', 'Paris', 'France', 49.012798, 2.55, 119, 'Europe/Paris'),
('EDDF', 'FRA', 'Frankfurt am Main Airport', 'Frankfurt', 'Germany', 50.033333, 8.570556, 111, 'Europe/Berlin'),
('RJTT', 'HND', 'Tokyo Haneda Airport', 'Tokyo', 'Japan', 35.552258, 139.779694, 6, 'Asia/Tokyo')
ON CONFLICT (icao) DO NOTHING;

-- Insert sample aircraft metadata
INSERT INTO aircraft_metadata (icao24, registration, manufacturer, model, operator) VALUES
('a0a1a2', 'N12345', 'Boeing', '737-800', 'American Airlines'),
('b1b2b3', 'G-ABCD', 'Airbus', 'A320', 'British Airways'),
('c1c2c3', 'F-EFGH', 'Airbus', 'A380', 'Air France'),
('d1d2d3', 'D-IJKL', 'Boeing', '747-8', 'Lufthansa')
ON CONFLICT (icao24) DO NOTHING;

-- =============================================
-- 8. USER PERMISSIONS AND ROLES
-- =============================================

-- Create read-only user for dashboard (optional)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'flight_reader') THEN
        CREATE USER flight_reader WITH PASSWORD 'reader_password';
    END IF;
END $$;

-- Grant permissions to read-only user
GRANT CONNECT ON DATABASE logdb TO flight_reader;
GRANT USAGE ON SCHEMA public TO flight_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO flight_reader;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO flight_reader;

-- Grant permissions to application user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;

-- =============================================
-- 9. TABLE COMMENTS FOR DOCUMENTATION
-- =============================================

COMMENT ON TABLE log_entries IS 'Stores application log entries from various services';
COMMENT ON TABLE flight_data_raw IS 'Raw flight data ingested from OpenSky Network API';
COMMENT ON TABLE flight_data_processed IS 'Processed and enriched flight data ready for analytics';
COMMENT ON TABLE flight_aggregations IS 'Pre-aggregated flight statistics for dashboard performance';
COMMENT ON TABLE aircraft_metadata IS 'Static aircraft information and metadata';
COMMENT ON TABLE airports IS 'Reference data for airports worldwide';
COMMENT ON TABLE flight_routes IS 'Flight route and schedule information';

COMMENT ON COLUMN flight_data_processed.speed_kmh IS 'Velocity converted to kilometers per hour';
COMMENT ON COLUMN flight_data_processed.altitude_ft IS 'Altitude converted to feet';
COMMENT ON COLUMN flight_data_processed.is_climbing IS 'True if vertical rate is positive';
COMMENT ON COLUMN flight_data_processed.is_descending IS 'True if vertical rate is negative';

-- =============================================
-- 10. INITIAL METRICS AND SYSTEM RECORDS
-- =============================================

-- Insert initial pipeline metrics record
INSERT INTO pipeline_metrics (component, metric_name, metric_value, status) 
VALUES ('database', 'initialization', 1, 'success')
ON CONFLICT DO NOTHING;

-- Insert initial API usage record
INSERT INTO api_usage (api_name, endpoint, request_count, error_count, avg_response_time_ms)
VALUES ('opensky', '/api/states/all', 0, 0, 0)
ON CONFLICT DO NOTHING;

-- =============================================
-- COMPLETION MESSAGE
-- =============================================

DO $$ 
BEGIN
    RAISE NOTICE 'Database initialization completed successfully';
    RAISE NOTICE 'Created tables: log_entries, flight_data_raw, flight_data_processed, flight_aggregations, aircraft_metadata, airports, flight_routes, pipeline_metrics, api_usage';
    RAISE NOTICE 'Created views: current_active_flights, flight_statistics, country_flight_counts';
    RAISE NOTICE 'Created functions: calculate_distance, clean_old_flight_data';
    RAISE NOTICE 'Created procedure: update_flight_aggregations';
END $$;
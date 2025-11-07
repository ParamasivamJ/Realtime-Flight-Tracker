-- Verification script to check database setup
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.tables t WHERE t.table_schema = 'public' AND t.table_name = tables.table_name) as table_exists,
    (SELECT COUNT(*) FROM information_schema.views v WHERE v.table_schema = 'public' AND v.view_name = tables.table_name) as view_exists
FROM (VALUES 
    ('log_entries'),
    ('flight_data_raw'),
    ('flight_data_processed'),
    ('flight_aggregations'),
    ('current_active_flights'),
    ('flight_statistics')
) AS tables(table_name);

-- Check row counts
SELECT 'log_entries' as table_name, COUNT(*) as row_count FROM log_entries
UNION ALL
SELECT 'flight_data_raw', COUNT(*) FROM flight_data_raw
UNION ALL
SELECT 'flight_data_processed', COUNT(*) FROM flight_data_processed;

-- Test the distance calculation function
SELECT calculate_distance(40.6892, -74.0445, 34.0522, -118.2437) as distance_km;
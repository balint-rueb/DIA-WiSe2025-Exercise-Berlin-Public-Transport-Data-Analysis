-- ==========================================
-- 1. Dimension: Stations
-- Stores the physical location, name, and ID of stations.
-- ==========================================
CREATE TABLE dim_stations (
    station_eva BIGINT PRIMARY KEY,    -- The EVA number 
    station_name VARCHAR(255),         -- The human-readable name
    latitude DECIMAL(9, 6),            -- DECIMAL(precision, scale) where precision=total digits, scale=digits after decimal. Longitude goes from -180 to 180, Latitude from -90 to 90. For uniformity, we use DECIMAL(9,6) for both.
    longitude DECIMAL(9, 6)
);

-- ==========================================
-- 2. Dimension: Train Profiles
-- Stores train number, train line, and train type.
-- ==========================================
CREATE TABLE dim_train_profiles (
    profile_id SERIAL PRIMARY KEY,
    train_number VARCHAR(50),          -- e.g., "73768". Decided to keep it as a string in case of unexpected letters 
    train_line VARCHAR(50),            -- e.g., "1", "7", "S5"
    train_type VARCHAR(50),            -- e.g., "ICE", "RE", "RB"
    
    UNIQUE (train_number, train_line, train_type) -- In case a train number is reused across lines/types or the same train is used for different services
);

-- ==========================================
-- 4. Dimension: Time (Date)
-- Standard Date dimension with one row per day.
-- Granularity: Daily (Time of day is kept on the Fact table).
-- ==========================================
CREATE TABLE dim_time (
    date_id DATE PRIMARY KEY,          -- '2025-09-02'
    year INT,                          -- 2025
    month INT,                         -- 9
    day INT,                           -- 2
    day_of_week VARCHAR(20)            -- 'Tuesday'. Could also be INT (1=Monday, 7=Sunday) but string is more readable. Added into table for potentially interesting queries.
);

-- ==========================================
-- 5. Fact Table: Train Stops
-- ==========================================
CREATE TABLE fact_train_stops (
    stop_id SERIAL PRIMARY KEY,
    
    -- Foreign Keys to Dimension Tables
    station_eva BIGINT REFERENCES dim_stations(station_eva),
    profile_id INT REFERENCES dim_train_profiles(profile_id),
    date_id DATE REFERENCES dim_time(date_id),
    
    -- Timestamps (Time of Day is stored here)
    planned_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    planned_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    
    -- Calculated Metrics
    arrival_delay_minutes INT DEFAULT 0,
    departure_delay_minutes INT DEFAULT 0,
    
    -- Status Flags
    is_cancelled BOOLEAN DEFAULT FALSE
);


-- ==========================================
-- NOTES
-- ==========================================
-- This star schema was designed first before the rest of the tasks were done. Some changes were made to the database schema through alembic migrations
-- that arent refleced here, but the core star schema design remains the same.

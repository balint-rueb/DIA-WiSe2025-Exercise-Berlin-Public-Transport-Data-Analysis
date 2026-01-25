import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from datetime import date

from utils import normalize_station_names

DB_URL = os.getenv("DB")

engine = create_engine(DB_URL)

def task_2_1_get_station_coords(session):
    """Task 2.1: Given a station name, return its coordinates and identifier."""
    print("\n--- Task 2.1: Get Station Coordinates ---")
    raw_input = input("Enter station name [default: Berlin Hauptbahnhof]: ")

    # If user hits Enter without typing, use the default
    station_name = raw_input or "Berlin Hauptbahnhof"
    print(f"\n--- Task 2.1: Coordinates for '{station_name}' ---")
    station_name = normalize_station_names(station_name)
    print(station_name)
    
    sql = text("""
        SELECT station_eva, latitude, longitude
        FROM dim_stations
        WHERE station_name = :name
    """)
    
    result = session.execute(sql, {"name": station_name}).fetchone()
    
    if result:
        print(f"EVA: {result.station_eva} | Lat: {result.latitude} | Lon: {result.longitude}")
    else:
        print("Station not found.")

def task_2_2_find_closest_station(session):
    """Task 2.2: Given latitude/longitude, return the name of the closest station."""

    print("\n--- Task 2.2: Find Closest Station based on lat and lon ---")
    user_input = input("Enter lat,lon (default: 52.516266,13.377775 - Brandenburger Tor): ").strip() or "52.516266,13.377775"
    try:
        # map(float, ...) handles the conversion for both values at once
        lat, lon = map(float, user_input.split(','))
    except ValueError:
        print("Invalid format. Please enter numbers separated by a comma (e.g., 52.5,13.4).")
        return
    
    print(f"\n--- Task 2.2: Closest Station to ({lat}, {lon}) ---")

    # We use Euclidean distance for sorting. 
    # Logic: (lat - target_lat)^2 + (lon - target_lon)^2 = squared distance to station
    # Sort by the distance metric ascending and get the closest one.
    # We can use Euclidian distance since Berlin is small enough that curvature of Earth is negligible.
    sql = text("""
        SELECT station_name, 
               (POWER(latitude - :lat, 2) + POWER(longitude - :lon, 2)) as distance_metric
        FROM dim_stations
        ORDER BY distance_metric ASC
        LIMIT 1
    """)
    
    result = session.execute(sql, {"lat": lat, "lon": lon}).fetchone()
    

    print(f"Closest Station: {result.station_name}")

def task_2_3_count_cancellations(session):
    """Task 2.3: Return total number of canceled trains for a specific date and hour."""
    print("\n--- Task 2.3: Count Cancellations for a Given Date and Hour ---")
    raw_input = input("Enter date_hour (e.g., MM_DD_HH, default:09_02_16): ").strip() or "09_02_16"
    try:
        month, day, hour = raw_input.split('_')
        
        # Convert to a format Postgres DATE understands (YYYY-MM-DD)
        formatted_date = f"2025-{month}-{day}" 
        
    except ValueError:
        print("Invalid format! Please use MM_DD_HH (e.g., 09_24_16).")
        return
    
    print(f"\n--- Task 2.3: Cancellations on {formatted_date} at {hour}:00 ---")
        
    sql = text("""
        SELECT COUNT(*) 
        FROM fact_train_stops
        WHERE date_id = :date
        AND (
            EXTRACT(HOUR FROM planned_arrival) = :hour 
            OR 
            EXTRACT(HOUR FROM planned_departure) = :hour
        )
        AND is_cancelled = TRUE
    """)
    
    result = session.execute(sql, {
        "date": formatted_date, 
        "hour": int(hour)
    }).scalar()

    print(f"Total Cancelled Trains: {result}")

def task_2_4_average_delay(session):
    """Task 2.4: Given a station name, return the average train delay (excluding cancellations)."""
    print("\n--- Task 2.4: Average Delay for a Given Station ---")
    raw_input = input("Enter station name [default: Berlin Hauptbahnhof]: ")
    # If user hits Enter without typing, use the default
    station_name = raw_input or "Berlin Hauptbahnhof"
    station_name = normalize_station_names(station_name)
    print(f"\n--- Task 2.4: Average Delay for '{station_name}' ---")
    
    # We sum the two columns row-by-row, then take the average of those sums.
    # Since we set default=0 in the ETL, we don't need to worry about NULLs here.
    # Join the station data table with the facts table by eva which we fetch from the station table name.
    sql = text("""
        SELECT AVG(arrival_delay_minutes + departure_delay_minutes) as avg_delay
        FROM fact_train_stops fts
        JOIN dim_stations ds ON fts.station_eva = ds.station_eva
        WHERE ds.station_name = :name
          AND fts.is_cancelled = FALSE
    """)
    
    result = session.execute(sql, {"name": station_name}).scalar()
    
    if result is not None:
        print(f"Average Delay: {round(result, 2)} minutes")
    else:
        print("No data available for this station.")

if __name__ == "__main__":
    try:
        with Session(engine) as session:
            
            task_2_1_get_station_coords(session)

            task_2_2_find_closest_station(session)

            task_2_3_count_cancellations(session)

            task_2_4_average_delay(session)
            
    except Exception as e:
        print(f"An error occurred: {e}")
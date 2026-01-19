import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from datetime import date

from utils import normalize_station_names

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------
DB_URL = os.getenv("DB")

if not DB_URL:
    raise ValueError("Please set the 'DB' environment variable with your connection string.")

engine = create_engine(DB_URL)

# ---------------------------------------------------------
# TASK FUNCTIONS
# ---------------------------------------------------------

def task_2_1_get_station_coords(session, station_name):
    """Task 2.1: Given a station name, return its coordinates and identifier."""
    print(f"\n--- Task 2.1: Coordinates for '{station_name}' ---")
    
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

def task_2_2_find_closest_station(session, lat, lon):
    """Task 2.2: Given latitude/longitude, return the name of the closest station."""
    print(f"\n--- Task 2.2: Closest Station to ({lat}, {lon}) ---")
    
    # We use Euclidean distance (squared) for sorting. 
    # Logic: (lat - target_lat)^2 + (lon - target_lon)^2 = squared distance to station
    # Sort by the distance metric ascending and get the closest one.
    sql = text("""
        SELECT station_name, 
               (POWER(latitude - :lat, 2) + POWER(longitude - :lon, 2)) as distance_metric
        FROM dim_stations
        ORDER BY distance_metric ASC
        LIMIT 1
    """)
    
    result = session.execute(sql, {"lat": lat, "lon": lon}).fetchone()
    

    print(f"Closest Station: {result.station_name}")

def task_2_3_count_cancellations(session, check_date, check_hour):
    """Task 2.3: Return total number of canceled trains for a specific date and hour."""
    print(f"\n--- Task 2.3: Cancellations on {check_date} during hour {check_hour} ---")
    
    # count all rows that pass the filers defined later
    # 
    sql = text("""
        SELECT COUNT(*) as total_cancelled
        FROM fact_train_stops
        WHERE date_id = :date
          AND EXTRACT(HOUR FROM planned_arrival) = :hour
          AND is_cancelled = TRUE
    """)
    
    result = session.execute(sql, {"date": check_date, "hour": check_hour}).scalar()
    print(f"Total Cancelled Trains: {result}")

def task_2_4_average_delay(session, station_name):
    """Task 2.4: Given a station name, return the average train delay (excluding cancellations)."""
    print(f"\n--- Task 2.4: Average Delay for '{station_name}' ---")
    
    # We sum the two columns row-by-row, then take the average of those sums.
    # Since we set default=0 in the ETL, we don't need to worry about NULLs here.
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

# ---------------------------------------------------------
# MAIN EXECUTION
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        with Session(engine) as session:
            # Task 2.1 Example: Berlin Hbf
            task_2_1_get_station_coords(session, normalize_station_names("Berlin Hauptbahnhof"))

            # Task 2.2 Example: Coordinates near the Brandenburg Gate (52.5163, 13.3777)
            task_2_2_find_closest_station(session, 52.5163, 13.3777)

            # Task 2.3 Example: Check cancellations on a specific date/hour found in your dataset
            # (Adjust the date below to match your XML data range!)
            task_2_3_count_cancellations(session, date(2025, 9, 2), 16)

            # Task 2.4 Example: Average delay for Berlin Hbf
            task_2_4_average_delay(session, normalize_station_names("Berlin Hauptbahnhof"))
            
    except Exception as e:
        print(f"An error occurred: {e}")
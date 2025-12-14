import json
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from models import Station, TrainProfile, Time, TrainStop
import os

#imports for date parsing
from datetime import datetime,timedelta, date
import re



def process_station_data(station_data: json, db_session: Session):
    stations_list = station_data.get("result", [])

    print(f"Found {len(stations_list)} stations in the data.")
    count_inserted = 0
    for station in stations_list:
        station_name = station["name"] # I want the extraction to fail if name is missing

        #find main eva number and coords
        eva_list = station["evaNumbers"] # I want the extraction to fail if evaNumbers is missing
        for eva in eva_list:
            if eva["isMain"] == True:
                station_eva = eva["number"]
                coordinates = eva["geographicCoordinates"]["coordinates"]
                longitude = coordinates[0]
                latitude = coordinates[1]
                break
        
        # Create Station object
        station_record = Station(
            station_eva=station_eva,
            station_name=station_name,
            latitude=latitude,
            longitude=longitude
        )

        # 'merge' looks at the Primary Key (station_eva). 
        # If it exists in DB -> Update it. If not -> Insert it.
        # In our case it doesnt matter since data is static, but not bad practice
        db_session.merge(station_record)
        count_inserted += 1

    # Commit the transaction to DB
    db_session.commit()
    print("------------------------------------------------")
    print("Process Complete.")
    print(f"Upserted: {count_inserted}")

def get_dates_from_filename(filename: str):
    """
    Parses 'YYMMDD_YYMMDD' from a filename.
    Returns (start_date, end_date) as datetime.date objects.
    """
    # Regex to find the pattern YYMMDD_YYMMDD
    # \d{6} matches exactly 6 digits
    # search looks in the string for the pattern
    match = re.search(r"(\d{6})_(\d{6})", filename)

    start_str, end_str = match.groups()

    # use strin parse time function. define format of date in second argument
    # %y = 2-digit year (e.g., 25 -> 2025), %m = month, %d = day
    start_date = datetime.strptime(start_str, "%y%m%d").date()
    end_date = datetime.strptime(end_str, "%y%m%d").date()
    return start_date, end_date

def generate_date_range(start_date: date, end_date: date):
    """Returns a list of dates between start_date and end_date."""

    # Calculate the number of days between the two dates
    delta = (end_date - start_date).days + 1
    # creates a list of dates by adding i days to the start_date
    # needed to be done like this since date objects dont support addition with integers directly
    return [start_date + timedelta(days=i) for i in range(delta)]

def populate_time_dimension(session):
    """
    Scans directory, finds all unique dates, and inserts them into dim_time.

    """

    TIMETABLES_DIR = "./timetables"
    # use set to avoid duplicate dates
    unique_dates = set()

    print(f"Scanning directory: {TIMETABLES_DIR} ...")

    # 1. SCAN AND COLLECT DATES
    for filename in os.listdir(TIMETABLES_DIR):
        # get the start date and the end date from the filename
        start_date, end_date = get_dates_from_filename(filename)
        # for every date in the range between start_date and end_date
        for date in generate_date_range(start_date, end_date):
            # ... add it to unique dates
            unique_dates.add(date)

    print(f"Identified {len(unique_dates)} unique days to process.")

    # 2. INSERT INTO DATABASE
    upsert_count = 0
    
    for current_date in unique_dates:
        time_entry = Time(
            date_id=current_date,
            year=current_date.year,
            month=current_date.month,
            day=current_date.day,
            day_of_week=current_date.strftime("%A") #string format time is used to get a string in the desired format from a date object. %A is for full weekday name
        )

        # Use merge for same reason as before
        session.merge(time_entry)
        upsert_count += 1

    session.commit()
    print(f"Successfully processed {upsert_count} days into dim_time.")

engine = create_engine(os.getenv("DB"))

with open('station_data.json', 'r') as f:
    station_data = json.load(f)

with Session(engine) as session:
    process_station_data(station_data, session)
    populate_time_dimension(session)
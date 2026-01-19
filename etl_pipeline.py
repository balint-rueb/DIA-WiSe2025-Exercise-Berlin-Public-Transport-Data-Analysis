import json
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from models import Station, TrainProfile, Time, TrainStop
import os, time
from datetime import datetime

#import xml parsing library
from lxml import etree

#import utility functions
from utils import load_station_cache, get_dates_from_filename, generate_date_range, stream_xml_files, get_eva_with_fuzzy_match, normalize_station_names



#for multiprocessing
from multiprocessing import Pool, cpu_count
from pipeline_functions import process_timetable_archive, process_timetable_changes_archive


def process_station_data(station_data: json, db_session: Session):
    """
    Docstring for process_station_data
    
    :param station_data: JSON containing all station data
    :type station_data: json
    :param db_session: Description
    :type db_session: DB Session
    """
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

        # normalize station name by stripping leading/trailing spaces
        station_name = normalize_station_names(station_name)
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

def populate_time_dimension(session, dir):
    """
    Scans directory, finds all unique dates, and inserts them into dim_time.

    """

    # use set to avoid duplicate dates
    unique_dates = set()

    print(f"Scanning directory: {dir} ...")

    # 1. SCAN AND COLLECT DATES
    for filename in os.listdir(dir):
        # get the start date and the end date from the filename
        start_date, end_date = get_dates_from_filename(filename)
        # for every date in the range between start_date and end_date
        unique_dates.update(generate_date_range(start_date, end_date))

    print(f"Identified {len(unique_dates)} unique days to process.")

    # 2. INSERT INTO DATABASE
    upsert_count = 0
    time_entries = []
    for d in unique_dates:
        time_entries.append({
            "date_id": d,
            "year": d.year,
            "month": d.month,
            "day": d.day,
            "day_of_week": d.strftime("%A") #string format time is used to get a string in the desired format from a date object. %A is for full weekday name
        })
    # Perform a Bulk Upsert so that duplicates dont cause issues. During normal bulk insert an INSERT SQL command is used, which will throw an error on duplicate PKs. This is a PostgreSQL specific feature
    if time_entries:
        stmt = insert(Time).values(time_entries)
        # "If the date_id exists, do nothing"
        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['date_id'])
        session.execute(upsert_stmt)
        session.commit()

    print(f"Successfully processed {len(time_entries)} days into dim_time.")

def run_timetable_pipeline_parallel(dir_path):
    db_url = os.getenv("DB")
    
    # 1. Global Setup (Single Threaded)
    engine = create_engine(db_url)
    with Session(engine) as session:
        # Load Station Data & Cache FIRST so all workers have it
        print("Loading Station Data...")
        with open('station_data.json', 'r') as f:
            station_data = json.load(f)
        
        process_station_data(station_data, session) 
        
        station_cache = load_station_cache(session)
        
        # Populate Time Dimension
        populate_time_dimension(session, dir_path)

    # 2. Prepare Parallel Tasks
    tar_files = [f for f in os.listdir(dir_path) if f.endswith(".tar.gz")]
    print(f"Starting parallel processing of {len(tar_files)} archives on {cpu_count()} cores...")

    # Create arguments list: (filename, dir, cache, db_url) for each task
    tasks = [(f, dir_path, station_cache, db_url) for f in tar_files]

    start_time = time.time()
    
    # 3. Execute in Parallel
    # We use a Pool to manage worker processes
    with Pool(processes=cpu_count()) as pool:
        # map_async allows us to track progress if needed, simple map blocks until done
        results = pool.map(process_timetable_archive, tasks)

    total_time = time.time() - start_time
    print(f"Parallel ETL Completed in {total_time:.2f}s")

def run_timetable_changes_pipeline_parallel(dir_path):
    db_url = os.getenv("DB")
    
    files = [f for f in os.listdir(dir_path) if f.endswith(".tar.gz")]
    print(f"Found {len(files)} archives. Starting processing on {cpu_count()} cores...")
    
    tasks = [(f, dir_path, db_url) for f in files]
    
    start = time.time()
    
    with Pool(processes=cpu_count()) as pool:
        pool.map(process_timetable_changes_archive, tasks)
        
    print(f"--- Changes ETL Finished in {time.time() - start:.2f}s ---")


if __name__ == "__main__":
    TIMETABLES_DIR = "./timetables"
    CHANGES_DIR = "./timetable_changes"
    run_timetable_pipeline_parallel(TIMETABLES_DIR)
    run_timetable_changes_pipeline_parallel(CHANGES_DIR)
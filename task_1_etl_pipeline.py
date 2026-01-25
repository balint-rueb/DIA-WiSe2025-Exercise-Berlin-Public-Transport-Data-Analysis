#import database session and data models
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from models import Station,Time

#import utility functions
from utils import load_station_cache, get_dates_from_filename, generate_date_range, normalize_station_names

#import misc libraries
import json
import os, time

#for multiprocessing
from multiprocessing import Pool, cpu_count
from pipeline_functions import process_timetable_archive, process_timetable_changes_archive


def process_station_data(station_data: json, db_session: Session):
    """
    Processes station data JSON and upserts into the Station table.
    :param station_data: JSON containing all station data
    :type station_data: json
    :param db_session: DB session object
    :type db_session: DB Session
    """

    stations_list = station_data.get("result", [])

    print(f"Found {len(stations_list)} stations in the data.")
    count_inserted = 0
    for station in stations_list:
        station_name = station["name"]

        #find main eva number and coords
        eva_list = station["evaNumbers"] 
        for eva in eva_list:
            #select only the eva that is marked as main
            if eva["isMain"] == True:
                station_eva = eva["number"]
                coordinates = eva["geographicCoordinates"]["coordinates"]
                longitude = coordinates[0]
                latitude = coordinates[1]
                break
        
        # Use sqlalchemy model to create a new Station object

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
        # In our case it shouldnt matter since data is static, but not bad practice
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

    # 1. COLLECT DATES
    for filename in os.listdir(dir):
        # get the start date and the end date from the filename
        start_date, end_date = get_dates_from_filename(filename)
        # collect every date in the range between start_date and end_date
        unique_dates.update(generate_date_range(start_date, end_date))

    print(f"Identified {len(unique_dates)} unique days to process.")

    # 2. INSERT DATES INTO DATABASE
    date_entries = []
    for d in unique_dates:
        date_entries.append({
            "date_id": d,
            "year": d.year,
            "month": d.month,
            "day": d.day,
            "day_of_week": d.strftime("%A") #string format time is used to get a string in the desired format from a date object. %A is for full weekday name
        })

    # Perform a Bulk Upsert so that duplicates dont cause issues. 
    if date_entries:
        stmt = insert(Time).values(date_entries)
        # If the date_id exists, do nothing
        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['date_id'])
        session.execute(upsert_stmt)
        session.commit()

    print(f"Successfully processed {len(date_entries)} days into dim_time.")

def run_timetable_pipeline_parallel(dir_path,station_cache):
    """
    Creates tasks for each .tar.gz file in the specified directory and processes them with timetable function.
    :param dir_path: Directory containing .tar.gz timetable files
    :type dir_path: str
    """
    db_url = os.getenv("DB")

    # Prepare list of .tar.gz files to process
    tar_files = [f for f in os.listdir(dir_path) if f.endswith(".tar.gz")]
    print(f"Starting parallel processing of {len(tar_files)} archives on {cpu_count()} cores...")

    # Create arguments list: (filename, dir, cache, db_url) for each task
    # each task is made up of:
    # 1. the file name and path, used to open the file
    # 2. the station cache, used to resolve station names to eva numbers
    # 3. the db connection string, used to connect to the db
    tasks = [(f, dir_path, station_cache, db_url) for f in tar_files]

    start_time = time.time()
    
    with Pool(processes=len(tar_files) if cpu_count()>= len(tar_files) else cpu_count()) as pool:
        pool.map(process_timetable_archive, tasks)

    total_time = time.time() - start_time
    print(f"Parallel ETL Completed in {total_time:.2f}s")

def run_timetable_changes_pipeline_parallel(dir_path):
    """
    Creates tasks for each .tar.gz file in the specified directory and processes them with timetable_changes .
    
    :param dir_path: Directory containing .tar.gz timetable changes files
    """
    db_url = os.getenv("DB")
    
    files = [f for f in os.listdir(dir_path) if f.endswith(".tar.gz")]
    print(f"Found {len(files)} archives. Starting processing on {cpu_count()} cores...")
    
    tasks = [(f, dir_path, db_url) for f in files]
    
    start = time.time()
    
    with Pool(processes=len(files) if cpu_count()>= len(files) else cpu_count()) as pool:
        pool.map(process_timetable_changes_archive, tasks)
        
    print(f"--- Changes ETL Finished in {time.time() - start:.2f}s ---")


if __name__ == "__main__":
    TIMETABLES_DIR = "./timetables"
    CHANGES_DIR = "./timetable_changes"
    with Session(create_engine(os.getenv("DB"))) as session:
        dir_path = TIMETABLES_DIR
        with open('station_data.json', 'r') as f:
            station_data = json.load(f)
        
            process_station_data(station_data, session) 
        
            station_cache = load_station_cache(session)
        
            populate_time_dimension(session, dir_path)

    start = time.time()
    run_timetable_pipeline_parallel(TIMETABLES_DIR, station_cache)
    run_timetable_changes_pipeline_parallel(CHANGES_DIR)
    print(f"--- Total pipeline time: {time.time() - start:.2f}s ---")
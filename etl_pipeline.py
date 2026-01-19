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
from utils import load_station_cache, get_dates_from_filename, generate_date_range, stream_xml_files, get_eva_with_fuzzy_match, normalize_station_names, get_or_create_profile_safe

#for multiprocessing
from multiprocessing import Pool, cpu_count
from functools import partial


import tarfile

# --- WORKER FUNCTION (Runs on separate cores) ---
def process_single_archive(args):
    """
    Worker function to process a single tar.gz archive.
    Args:
        args: tuple containing (filename, dir_path, station_cache_dump, db_url)
    """
    filename, dir_path, station_cache, db_url = args
    
    # 1. Setup Independent DB Connection for this Process
    # SQLAlchemy engines/sessions cannot be shared across processes
    engine = create_engine(db_url)
    session = Session(engine)
    
    # Local cache to reduce DB hits for TrainProfiles within this specific file
    # We populate this lazily
    local_profile_cache = {}

    batch = []
    batch_size = 1000
    file_count = 0
    full_path = os.path.join(dir_path, filename)

    try:
        # Stream and process XML files from the archive
        with tarfile.open(full_path, "r:gz") as tar:
            for member in tar:
                if not(member.isfile() and member.name.endswith(".xml")):
                    continue
                
                file_count += 1
                
                # Extract Metadata
                path_parts = member.name.split('/')
                
                hourly_folder = path_parts[0]
                xml_full_name = path_parts[1]
                file_station_name = xml_full_name[:-14] # Removing "_timetable.xml"
                date_obj = datetime.strptime(hourly_folder[:6], '%y%m%d').date()

                # Stream Parse XML
                f = tar.extractfile(member)
                context = etree.iterparse(f, events=('start', 'end'))
                current_station_eva = None

                try:
                    for event, elem in context:
                        if event == 'start' and elem.tag == 'timetable':
                            xml_station_name = elem.get('station')
                            
                            # RESOLVE STATION EVA
                            if xml_station_name:
                                current_station_eva = get_eva_with_fuzzy_match(xml_station_name, station_cache)
                            if current_station_eva is None and file_station_name:
                                current_station_eva = get_eva_with_fuzzy_match(file_station_name, station_cache)
                            
                            if current_station_eva is None:
                                break # Skip this file if no station found

                        elif event == 'end' and elem.tag == 's':
                            tl = elem.find('tl')
                            ar = elem.find('ar')
                            dp = elem.find('dp')
                            
                            if tl is None: continue

                            # --- GET OR CREATE PROFILE (Concurrency Safe) ---
                            t_type = tl.get('c')
                            t_num = tl.get('n')
                            profile_key = (t_type, t_num)

                            if profile_key in local_profile_cache:
                                profile_id = local_profile_cache[profile_key]
                            else:
                                profile_id = get_or_create_profile_safe(session, t_type, t_num)
                                local_profile_cache[profile_key] = profile_id

                            # --- PARSE STOP DATA ---
                            line = (ar.get('l') if ar is not None else None) or \
                                   (dp.get('l') if dp is not None else None)

                            def parse_pt(tag):
                                if tag is None: return None
                                pt = tag.get('pt')
                                return datetime.strptime(pt, "%y%m%d%H%M") if pt else None

                            batch.append({
                                "stop_id": elem.get('id'),
                                "station_eva": current_station_eva,
                                "profile_id": profile_id,
                                "train_line": line, 
                                "date_id": date_obj,
                                "planned_arrival": parse_pt(ar),
                                "planned_departure": parse_pt(dp)
                            })

                            # --- BATCH INSERT ---
                            if len(batch) >= batch_size:
                                perform_bulk_insert(session, batch)
                                batch = []

                            # Memory Cleanup
                            elem.clear()
                            while elem.getprevious() is not None:
                                del elem.getparent()[0]

                except Exception as e:
                    print(f"Error parsing XML in {filename}: {e}")
                    continue
                finally:
                    f.close()
                    del context

        # Insert leftovers
        if batch:
            perform_bulk_insert(session, batch)

    except Exception as e:
        print(f"Failed to process archive {filename}: {e}")
    finally:
        session.close()
        engine.dispose()
    
    return f"Done: {filename} ({file_count} files)"

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


def process_timetable_files(session, dir_path, station_cache):
    batch = []
    batch_size = 1000
    missing_stations = set()
    file_count = 0
    start_time = time.time()

    # --- 1. PRE-LOAD TRAIN PROFILES ---
    # Cache key: (train_type, train_number) -> profile_id
    # This allows quick lookup to avoid duplicate inserts
    # On the first run when DB is empty, this will be an empty dict
    profiles = session.query(TrainProfile).all()
    profile_cache = {(p.train_type, p.train_number): p.profile_id for p in profiles}


    for xml_stream, metadata in stream_xml_files(dir_path):
        file_count += 1
        # setting the events to 'start' and 'end' means we stop at every opening and closing tag.
        context = etree.iterparse(xml_stream, events=('start', 'end'))
        current_station_eva = None
        
        try:
            for event, elem in context:
                # opening tag of timetable xml file
                if event == 'start' and elem.tag == 'timetable':
                    # extract the station name from the 'station' attribute
                    xml_station_name = elem.get('station')
                    file_station_name = metadata['station_name']

                    # 1. Primary Attempt: Use the name found inside the XML
                    if xml_station_name:
                        current_station_eva = get_eva_with_fuzzy_match(xml_station_name, station_cache)
                    
                    # 2. Secondary Attempt: If XML name failed (or was empty), try the Filename
                    if current_station_eva is None and file_station_name:
                        current_station_eva = get_eva_with_fuzzy_match(file_station_name, station_cache)

                    # 3. Final Check
                    if current_station_eva is None:
                        # We tried both XML and Filename (Exact + Fuzzy), and still nothing.
                        error_label = f"XML: '{xml_station_name}' | File: '{file_station_name}'"
                        print(f"Station not found: {error_label}")
                        missing_stations.add(error_label)
                        break

                elif event == 'end' and elem.tag == 's':
                    tl = elem.find('tl')
                    ar = elem.find('ar')
                    dp = elem.find('dp')
                    
                    if tl is None: continue

                    # --- 2. HANDLE TRAIN PROFILE DIMENSION ---
                    t_type = tl.get('c')
                    t_num = tl.get('n')
                    profile_key = (t_type, t_num)

                    if profile_key not in profile_cache:
                        # New Profile found! Insert it immediately to get an ID
                        new_p = TrainProfile(train_type=t_type, train_number=t_num)
                        session.add(new_p)
                        session.flush() # This pushes to DB to generate the ID
                        profile_cache[profile_key] = new_p.profile_id
                    
                    profile_id = profile_cache[profile_key]

                    # --- 3. EXTRACT LINE (from ar or dp) ---
                    # Use 'l' attribute from ar, if missing check dp
                    line = (ar.get('l') if ar is not None else None) or \
                           (dp.get('l') if dp is not None else None)

                    # 4. PARSE PLANNED TIME
                    def parse_planned(tag):
                        if tag is None: return None
                        pt_str = tag.get('pt') 
                        return datetime.strptime(pt_str, "%y%m%d%H%M") if pt_str else None

                    batch.append({
                        "stop_id": elem.get('id'),
                        "station_eva": current_station_eva,
                        "profile_id": profile_id,
                        "train_line": line, 
                        "date_id": metadata['date'],
                        "planned_arrival": parse_planned(ar),
                        "planned_departure": parse_planned(dp),
                        # On first pass, actual_arrival/dep and delay fields are left NULL/0
                        # They can be updated in a later ETL pass if needed
                        # is_cancelled stays False
                    })

                    if len(batch) >= batch_size:
                        # Create the Postgres-specific insert
                        stmt = insert(TrainStop).values(batch)
                        
                        #This is a bit busted but I assume if there are duplicates we just want to skip them since s id should be unique anyway
                        # Tell it to skip duplicates
                        # This works for duplicates within the batch AND 
                        # duplicates already in the DB from previous runs.
                        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['stop_id'])
                        
                        session.execute(upsert_stmt)
                        session.commit()
                        batch = []

                    # Once weve finished processing the XML element, clear it from memory to save RAM
                    # This is also neccesary all previously accesed elements. LXML keeps all elements in memory unless explicitly cleared
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

        except Exception as e:
            print(f"Error in {metadata['station_name']}: {e}")
            session.rollback() # Rollback if a file fails to keep session clean
            # but we keep trying to process the rest
            continue

    # Insert any remaining records in the final batch
    if batch:
        stmt = insert(TrainStop).values(batch)

        upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['stop_id'])
        
        session.execute(upsert_stmt)
        session.commit()

    # Final Summary Report
    total_time = time.time() - start_time
    print(f"\n--- ETL COMPLETED ---")
    print(f"Total Files: {file_count} | Total Time: {total_time:.2f}s")
    if missing_stations:
        print(f"⚠️ Missing Station EVA for: {sorted(list(missing_stations))}")


def perform_bulk_insert(session, batch):
    stmt = insert(TrainStop).values(batch)
    upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['stop_id'])
    session.execute(upsert_stmt)
    session.commit()


def run_pipeline_parallel(dir_path):
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
        results = pool.map(process_single_archive, tasks)

    total_time = time.time() - start_time
    print(f"Parallel ETL Completed in {total_time:.2f}s")

#legacy code for non-parallel execution
def run_pipeline(session: Session, dir: str,station_cache: dict):
    populate_time_dimension(session, dir)
    process_timetable_files(session, dir,station_cache)


if __name__ == "__main__":
    TIMETABLES_DIR = "./timetables"
    run_pipeline_parallel(TIMETABLES_DIR)

    #legacy code of non-parallel execution
"""engine = create_engine(os.getenv("DB"))
with open('station_data.json', 'r') as f:
    station_data = json.load(f)
with Session(engine) as session:
    process_station_data(station_data, session)
    station_cache = load_station_cache(session)
    run_pipeline(session, TIMETABLES_DIR,station_cache) """
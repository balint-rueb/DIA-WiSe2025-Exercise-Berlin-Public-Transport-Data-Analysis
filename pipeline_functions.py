import json
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
import os
from datetime import datetime

#import xml parsing library
from lxml import etree

#import utility functions
from utils import get_eva_with_fuzzy_match,get_or_create_profile_safe, perform_bulk_insert




import tarfile
# --- WORKER FUNCTION for timetables ---
def process_timetable_archive(args):
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


# --- WORKER FUNCTION for timetable changes---
def process_timetable_changes_archive(args):
    """
    Worker to process a single timetable_changes tar.gz file.
    """
    filename, dir_path, db_url = args
    
    # 1. Setup DB Connection
    engine = create_engine(db_url)
    session = Session(engine)
    
    batch = []
    batch_size = 2500 
    file_count = 0
    full_path = os.path.join(dir_path, filename)

    # 2. DEFINE RAW SQL STATEMENT
    # We use :colon_names as placeholders. 
    # These match the keys in our batch dictionary below.
    # We cast the delay calculation to INTEGER to match your table schema.
    
    raw_sql = text("""
        UPDATE fact_train_stops
        SET 
            -- Update Actual Times (Use existing if new value is NULL)
            actual_arrival = COALESCE(:b_act_arr, actual_arrival),
            actual_departure = COALESCE(:b_act_dep, actual_departure),
            
            -- Calculate Arrival Delay: (New Actual - Planned) / 60
            arrival_delay_minutes = CASE 
                WHEN :b_act_arr IS NOT NULL 
                THEN CAST(EXTRACT(EPOCH FROM (:b_act_arr - planned_arrival)) / 60 AS INTEGER)
                ELSE arrival_delay_minutes 
            END,
            
            -- Calculate Departure Delay
            departure_delay_minutes = CASE 
                WHEN :b_act_dep IS NOT NULL 
                THEN CAST(EXTRACT(EPOCH FROM (:b_act_dep - planned_departure)) / 60 AS INTEGER)
                ELSE departure_delay_minutes 
            END,
            
            -- Handle Cancellations (Only update if True, never un-cancel)
            is_cancelled = CASE 
                WHEN :b_cancelled = true THEN true
                ELSE is_cancelled 
            END
            
        WHERE stop_id = :b_stop_id
    """)

    try:
        with tarfile.open(full_path, "r:gz") as tar:
            for member in tar:
                if not(member.isfile() and member.name.endswith(".xml")):
                    continue
                
                file_count += 1
                f = tar.extractfile(member)
                
                # Parse only 's' tags (stops)
                context = etree.iterparse(f, events=('end',))
                
                try:
                    for event, elem in context:
                        if elem.tag == 's':
                            stop_id = elem.get('id')
                            if not stop_id: 
                                continue

                            ar = elem.find('ar')
                            dp = elem.find('dp')

                            # --- Helper Functions ---
                            def parse_ct(tag):
                                if tag is None: return None
                                ct = tag.get('ct') # "YYMMDDHHMM"
                                return datetime.strptime(ct, "%y%m%d%H%M") if ct else None
                            
                            def check_cancel(tag):
                                if tag is None: return False
                                return tag.get('cs') == 'c'

                            # --- Extract Data ---
                            new_arr = parse_ct(ar)
                            new_dep = parse_ct(dp)
                            
                            # A train is cancelled if EITHER arrival or dep is flagged 'c'
                            is_cancelled = check_cancel(ar) or check_cancel(dp)

                            # --- Add to Batch ---
                            # Only add if we have useful data
                            if new_arr or new_dep or is_cancelled:
                                batch.append({
                                    "b_stop_id": stop_id,
                                    "b_act_arr": new_arr,
                                    "b_act_dep": new_dep,
                                    # Pass True if cancelled, else None.
                                    # Passing None causes the SQL 'ELSE' to trigger, keeping the old value.
                                    "b_cancelled": True if is_cancelled else None
                                })

                            # --- Execute Batch ---
                            if len(batch) >= batch_size:
                                session.execute(raw_sql, batch)
                                session.commit()
                                batch = []
                            
                            # Cleanup Memory
                            elem.clear()
                            while elem.getprevious() is not None:
                                del elem.getparent()[0]
                                
                except Exception as e:
                    print(f"XML Error in {member.name}: {e}")
                finally:
                    f.close()
                    del context

        # Process remaining records
        if batch:
            session.execute(raw_sql, batch)
            session.commit()

    except Exception as e:
        print(f"Archive Error {filename}: {e}")
    finally:
        session.close()
        engine.dispose()

    return f"Processed: {filename} ({file_count} files)"
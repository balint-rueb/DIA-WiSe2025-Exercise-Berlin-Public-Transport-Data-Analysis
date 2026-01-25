# db related imports
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text

#misc imports
import os
from datetime import datetime
import tarfile

#import xml parsing library
from lxml import etree

#import utility functions
from utils import get_eva_with_fuzzy_match,get_or_create_profile_safe, perform_bulk_insert

def process_timetable_archive(args):
    """
    Worker function to process a single tar.gz archive of timetable data.
    Args:
        args: tuple containing (filename, dir_path, station_cache_dump, db_url)
    """

    filename, dir_path, station_cache, db_url = args
    
    # Setup Independent DB Connection for this Process
    # SQLAlchemy engines/sessions cannot be shared across processes but can be created independently per process
    # The db handles connection pooling internally
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
        with tarfile.open(full_path, "r:gz") as tarball:
            for file in tarball:
                
                file_count += 1
                
                # file names are as follows:
                # YYMMDD_HHMM/stationname_timetable.xml
                file_path_parts = file.name.split('/')
                
                time_info = file_path_parts[0]
                file_station_name = file_path_parts[1][:-14] # Removing "_timetable.xml"
                date_obj = datetime.strptime(time_info[:6], '%y%m%d').date()

                # parse and stream XML
                f = tarball.extractfile(file)
                context = etree.iterparse(f, events=('start', 'end'))
                current_station_eva = None

                try:
                    # iterate over all elements in the xml
                    for event, elem in context:
                        # if we are at the start of an element and its a timetable, get station name, and eva
                        if event == 'start' and elem.tag == 'timetable':
                            xml_station_name = elem.get('station')
                            
                            # In case the XML does not provide a station name, we fallback to the file name
                            if xml_station_name:
                                current_station_eva = get_eva_with_fuzzy_match(xml_station_name, station_cache)
                            if current_station_eva is None and file_station_name:
                                current_station_eva = get_eva_with_fuzzy_match(file_station_name, station_cache)
                            
                            if current_station_eva is None:
                                break # Skip this file if no station found

                        # if we are at the end of a stop element, parse and store
                        elif event == 'end' and elem.tag == 's':
                            tl = elem.find('tl')
                            ar = elem.find('ar')
                            dp = elem.find('dp')

                            # get or create the train profile id
                            t_type = tl.get('c')
                            t_num = tl.get('n')
                            profile_key = (t_type, t_num)

                            # fetch the train profile from cache or get it from db and place it in cache
                            if profile_key in local_profile_cache:
                                profile_id = local_profile_cache[profile_key]
                            else:
                                profile_id = get_or_create_profile_safe(session, t_type, t_num)
                                local_profile_cache[profile_key] = profile_id

                            # get train line, sometimes ar is missing, sometimes dp is missing
                            line = (ar.get('l') if ar is not None else None) or \
                                   (dp.get('l') if dp is not None else None)


                            # helper to get the planned time from ar and dp tags
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

                            # batch insert for faster performance
                            if len(batch) >= batch_size:
                                perform_bulk_insert(session, batch)
                                batch = []

                            # we need to remove the processed xml tag from memory as lxml otherwise keeps the entire xml in mem
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
        # Close session and dispose engine
        session.close()
        engine.dispose()
    
    return f"Done: {filename} ({file_count} files)"


def process_timetable_changes_archive(args):
    """
    Worker function to process a single tar.gz archive of timetable_changes data.
    Args:
        args: tuple containing (filename, dir_path, station_cache_dump, db_url)
    """
    filename, dir_path, db_url = args
    
    # Setup DB Connection
    engine = create_engine(db_url)
    session = Session(engine)
    
    batch = []
    batch_size = 2500 
    file_count = 0
    full_path = os.path.join(dir_path, filename)

    # now we need to update already exisiting records based on stop_id
    # to do this we use a raw sql update with placeholders.
    # we then pass this sql with a batch of data to the DB and it bulk inserts it
    # much less network overhead and faster than individual updates
    
    raw_sql = text("""
        UPDATE fact_train_stops
        SET 
            -- Update Actual Times (Use existing if new value is NULL)
            -- COALESCE selects the first non-null value from the list
            actual_arrival = COALESCE(:b_act_arr, actual_arrival),
            actual_departure = COALESCE(:b_act_dep, actual_departure),
            
            -- We need to calculate how many minutes late the train is.
            -- Calculate Arrival Delay: (New Actual - Planned) / 60
            arrival_delay_minutes = CASE 
                -- Only do calculation if we have a new actual arrival time
                WHEN :b_act_arr IS NOT NULL 
                THEN CAST(EXTRACT(EPOCH FROM (:b_act_arr - planned_arrival)) -- Subtract timestamps to get difference, convert to total seconds (EPOCH)
                    / 60                                                     -- Convert seconds to minutes
                   AS INTEGER                                                -- Cast to INTEGER for storage
                )
                -- If we dont have a new actual arrival time, change nothing (will be null)
                ELSE arrival_delay_minutes 
            END,
            
            -- Calculate Departure Delay, same as above
            departure_delay_minutes = CASE 
                WHEN :b_act_dep IS NOT NULL 
                THEN CAST(EXTRACT(EPOCH FROM (:b_act_dep - planned_departure)) / 60 AS INTEGER)
                ELSE departure_delay_minutes 
            END,
            
            -- Handle Cancellations (Only update if True, never un-cancel, we assume train stays cancelled)
            is_cancelled = CASE 
                WHEN :b_cancelled = true THEN true
                ELSE is_cancelled 
            END
            
        WHERE stop_id = :b_stop_id
    """)

    try:
        with tarfile.open(full_path, "r:gz") as tarball:
            for file in tarball:
                
                file_count += 1
                f = tarball.extractfile(file)
                
                # Parse only 's' tags (stops)
                context = etree.iterparse(f, events=('end',))
                
                try:
                    for event, elem in context:
                        if elem.tag == 's':
                            stop_id = elem.get('id')

                            ar = elem.find('ar')
                            dp = elem.find('dp')

                            def parse_ct(tag):
                                ct = tag.get('ct') # "YYMMDDHHMM"
                                return datetime.strptime(ct, "%y%m%d%H%M") if ct else None
                            
                            def check_cancel(tag):
                                return tag.get('cs') == 'c'

                            # extract new arr and dep times
                            new_arr = parse_ct(ar)
                            new_dep = parse_ct(dp)
                            
                            # A train is cancelled if EITHER arrival or dep is flagged 'c'
                            is_cancelled = check_cancel(ar) or check_cancel(dp)

                            # add data to batch
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

                            # send batch to db
                            if len(batch) >= batch_size:
                                session.execute(raw_sql, batch)
                                session.commit()
                                batch = []
                            
                            # clean up memory like before
                            elem.clear()
                            while elem.getprevious() is not None:
                                del elem.getparent()[0]
                                
                except Exception as e:
                    print(f"XML Error in {file.name}: {e}")
                finally:
                    f.close()
                    del context

        # process rest of batch
        if batch:
            session.execute(raw_sql, batch)
            session.commit()

    except Exception as e:
        print(f"Archive Error {filename}: {e}")
    finally:
        session.close()
        engine.dispose()

    return f"Processed: {filename} ({file_count} files)"
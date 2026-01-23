#for fuzzy matching and normalization
from rapidfuzz import utils, process

#data model imports
from models import Station, TrainProfile, TrainStop

#imports for date parsing
from datetime import datetime,timedelta, date

#path and tarfile handling
import os,tarfile

#regex import
import re

#sqlalchemy imports for concurrency-safe get or create
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select

#memoization dictionary for fuzzy matching
memoized_matches = {}

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

            
def load_station_cache(session):
    """Fetch all stations from DB and create a normalized lookup map to better fetch station EVA numbers by name."""

    stations = session.query(Station).all()
    
    cache = {utils.default_process(s.station_name): s.station_eva for s in stations}
    #manually add problematic stations that dont match well
    cache[utils.default_process('yorckstra_e__gro_g_rschenstra_e_')] = 8089051
    return cache

def get_eva_with_fuzzy_match(name, cache):
    if not name: return None

    # 1. CHECK MEMOIZATION FIRST (Instant)
    # If we've already figured out this messy name before, return it immediately.
    if name in memoized_matches:
        return memoized_matches[name]

    norm_name = normalize_station_names(name)
    
    # 2. Direct Hit (Fast)
    if norm_name in cache:
        eva = cache[norm_name]
        memoized_matches[name] = eva 
        print(f"Direct matched '{name}' -> '{norm_name}'")
        return eva
    
    # 3. Fuzzy Match (Slow - only runs ONCE per unique station name)
    match = process.extractOne(norm_name, cache.keys())
    if match:
        eva = cache[match[0]]
        print(f"Fuzzy matched '{name}' -> '{match[0]}' (Score: {match[1]})")
        memoized_matches[name] = eva # Remember this result!
        return eva
    
    return None


def normalize_station_names(name: str) -> str:
    """Normalizes station names for better matching."""
    return utils.default_process(name).replace('berlin', '')


def get_or_create_profile_safe(session, t_type, t_num):
    """
    Concurrency-safe Get or Create. Needed since we do multiprocessing. 
    1. Try to select. 
    2. If missing, insert (ignoring conflicts). 
    3. Select again.
    """
    # 1. Try to find existing
    stmt = select(TrainProfile.profile_id).filter_by(train_type=t_type, train_number=t_num)
    result = session.execute(stmt).scalar()
    
    if result:
        return result

    # 2. Insert (On Conflict Do Nothing)
    insert_stmt = insert(TrainProfile).values(train_type=t_type, train_number=t_num)
    do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['train_type', 'train_number'])
    session.execute(do_nothing_stmt)
    session.commit()

    # 3. Fetch ID (It must exist now, either from step 1 or 2)
    return session.execute(stmt).scalar()

def perform_bulk_insert(session, batch):
    stmt = insert(TrainStop).values(batch)
    upsert_stmt = stmt.on_conflict_do_nothing(index_elements=['stop_id'])
    session.execute(upsert_stmt)
    session.commit()

## --- LEGACY CODE FOR STREAMING XML FILES FROM TAR.GZ ARCHIVES ---
def stream_xml_files(tar_path):
    """Generator that yields XML file objects from a tar.gz archive and some metadata."""
    for tar_filename in os.listdir(tar_path):
        #filter out anything that isnt a tar.gz file
        if not tar_filename.endswith(".tar.gz"):
            continue
        print("Processing archive:", tar_filename)
        full_path = os.path.join(tar_path, tar_filename)
        with tarfile.open(full_path, "r:gz") as tar:
            for member in tar.getmembers():
                #tar.getmembers gets all files in the tar regardless of depth and retunrs them as flat list. Here we filter out all non xml files
                if not(member.isfile() and member.name.endswith(".xml")):
                    continue
                
                path_parts = member.name.split('/')
                hourly_folder = path_parts[0] # "2509021300"
                xml_full_name = path_parts[1] # "Berlin-Alt-Reinickendorf_timetable.xml"  
                station_name = xml_full_name[:-14] #used as a back-up in case xml file doesnt contain station name?
                
                #metadata to be used later when processing the xml file
                metadata = {
                        "date": datetime.strptime(hourly_folder[:6], '%y%m%d').date(),
                        "station_name": station_name
                    }
                
                f = tar.extractfile(member)
                yield f, metadata
                f.close()
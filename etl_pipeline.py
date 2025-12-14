import json
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from models import Station, TrainProfile, Time, TrainStop
import os


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
        # in our case it doesnt matter since data is static, but not bad practice
        session.merge(station_record)
        count_inserted += 1

    # Commit the transaction to DB
    session.commit()
    print("------------------------------------------------")
    print("Process Complete.")
    print(f"Upserted: {count_inserted}")
    

engine = create_engine(os.getenv("DB"))

with open('station_data.json', 'r') as f:
    station_data = json.load(f)

with Session(engine) as session:
    process_station_data(station_data, session)
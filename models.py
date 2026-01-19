from sqlalchemy import (Column, Integer, String, Boolean, Date, TIMESTAMP, 
                        Numeric, BigInteger, ForeignKey, UniqueConstraint, Index)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import text  # Needed for server_default

Base = declarative_base()

class Station(Base):
    __tablename__ = "dim_stations"

    station_eva = Column(BigInteger, primary_key=True, autoincrement=False)
    station_name = Column(String(255))
    latitude = Column(Numeric(9, 6))
    longitude = Column(Numeric(9, 6))

class TrainProfile(Base):
    __tablename__ = "dim_train_profiles"

    profile_id = Column(Integer, primary_key=True, autoincrement=True)
    train_number = Column(String(50))

    train_type = Column(String(50))

    __table_args__ = (
        #re-name constraint using alembic migration
        UniqueConstraint('train_number', 'train_type', name='_train_profile_uc'),
    )

class Time(Base):
    __tablename__ = "dim_time"

    date_id = Column(Date, primary_key=True)
    year = Column(Integer)
    month = Column(Integer)
    day = Column(Integer)
    day_of_week = Column(String(20))

class TrainStop(Base):
    __tablename__ = "fact_train_stops"

    stop_id = Column(String, primary_key=True)
    
    station_eva = Column(BigInteger, ForeignKey("dim_stations.station_eva"))
    profile_id = Column(Integer, ForeignKey("dim_train_profiles.profile_id"))
    date_id = Column(Date, ForeignKey("dim_time.date_id"))

    train_line = Column(String(50))
    planned_arrival = Column(TIMESTAMP)
    actual_arrival = Column(TIMESTAMP)
    planned_departure = Column(TIMESTAMP)
    actual_departure = Column(TIMESTAMP)

    # Added server_default to match SQL "DEFAULT 0/FALSE"
    arrival_delay_minutes = Column(Integer, default=0, server_default=text("0"))
    departure_delay_minutes = Column(Integer, default=0, server_default=text("0"))
    is_cancelled = Column(Boolean, default=False, server_default=text("false"))

    station = relationship("Station")
    profile = relationship("TrainProfile")
    time_info = relationship("Time")
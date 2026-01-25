import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
from datetime import datetime


def setup_spark():
    spark = SparkSession.builder \
    .appName("train data") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

    return spark


def convert_time(timestr):
    if not timestr or len(timestr) != 10:
        return None
    year = int(timestr[0:2])
    
    month  = int(timestr[2:4])
    day = int(timestr[4:6])
    hour = int(timestr[6:8])
    minutes = int(timestr[8:10])
    year = year +2000

    return datetime(year, month, day, hour, minutes)


def extract_timetable(path):

    raws = []
    tree = ET.parse(path)
    root = tree.getroot()
    station_name = root.get('station')

    for s in root.findall('s'):
        s_id = s.get("id")
        train_line = s.find('tl')

        if train_line is not None:
            category = train_line.get('c')
            number = train_line.get('n')
            owner = train_line.get('o')

        else:
            category = ''
            number = ''
            owner = ''
        
        ## for avoid the reapting I compact two type of event codes in one loop
        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)

            if element is not None:
                raws.append({
                'sid' : s_id, 
                'station' : station_name,
                'event_type' : event_type, 
                'category' : category,
                'number' : number,
                'owner' : owner,
                ## this info espical for the event
                'line' : element.get('l'),
                'planned_time' : convert_time(element.get('pt')),
                'planned_platform' : element.get('pp'),
                'planned_path': element.get('ppth')
            })
               
    return raws



def extract_changes(path):
    raws = []
    tree = ET.parse(path)
    root = tree.getroot()
    ## get the station name from the root
    station_name = root.get('station')
    
    for s in root.findall('s'):
        s_id = s.get('id')
        #train_line = s.find('tl')

        ### To Do, it is optional to have train info here-- I will add if we need 
        # if train_line is not None:
        #     category = train_line.get('c')
        #     number = train_line.get('n')
        #     owner = train_line.get('o')
            
        # else:
        #     category = ''
        #     number = ''
        #     owner = ''

        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)

            if element is not None:
                raws.append({
                    'sid': s_id ,
                    'station': station_name,
                    'event_type': event_type, 
                    'line': element.get('l', ''),
                    'changed_time': convert_time(element.get('ct')),
                    'cancellation_time': convert_time(element.get('clt')),
                    'cancellation_status': element.get('cs', ''),
                    'planned_platform': element.get('pp', ''),
                    'changed_path': element.get('cpth', '')
                })
    
    return raws


def define_schema():
    time_schema = StructType([
        StructField("sid", StringType()),
        StructField("station", StringType()),
        StructField("event_type", StringType()),
        StructField("category", StringType()),
        StructField("number", StringType()),
        StructField("owner",StringType()),
        StructField("line", StringType()),
        StructField("planned_time", TimestampType()),
        StructField("planned_platform",StringType()),
        StructField("planned_path", StringType())
])

    change_schema = StructType([
        StructField("sid", StringType()),
        StructField("station", StringType()),
        StructField("event_type", StringType()),
        StructField("line", StringType()),
        StructField("changed_time", TimestampType()),
        StructField("cancellation_time", TimestampType()),  # actual time
        StructField("cancellation_status", StringType()),  # p / a / c
        StructField("planned_platform", StringType()),
        StructField("changed_path", StringType())
])
    return time_schema, change_schema


## get the weeks files of the corresponding path : timetables or timetablechanges
def find_weeks_periods(base_path ):
    weeks = []
    for d in os.listdir(base_path):
        if os.path.isdir(os.path.join(base_path, d)):
            #print(f"week :{d}")
            weeks.append(d)

    print("weeks:", weeks)
    return weeks


## extraxt .xml files in timetable or timetable_changes 
def fine_xml_files(weeks , tima_base_path, time_change_path):
    
    tt_files = []
    chg_files = []
    for week in weeks:

        tt_pattern = f"{tima_base_path}/{week}/**/*.xml" 
        chg_pattern = f"{time_change_path}/{week}/**/*.xml"
        
        week_tt = glob.glob(tt_pattern, recursive=True)
        week_chg = glob.glob(chg_pattern, recursive=True)
        
        print(week, len(week_tt), len(week_chg))
        
        tt_files.extend(week_tt)
        chg_files.extend(week_chg)

    print("total:", len(tt_files), len(chg_files))
    return tt_files, chg_files

## process timetable files in bactches and  write data- for getting better speed we used batches
def process_time_files(spark, timetable_files, time_schema, batch_size):
    
    all_tt_data = []
    for i, f in enumerate(timetable_files):
        all_tt_data.extend(extract_timetable(f))
            
        if i % batch_size == 0 and i > 0:
            print(f"processed {i} files")
            ## cretae data frame frame using schmea 
            df = spark.createDataFrame(all_tt_data, time_schema)
            if i == batch_size: 
                df.write.mode("overwrite").parquet("./timetables.parquet")
            else:
                df.write.mode("append").parquet("./timetables.parquet")
            
            ## clear memory
            all_tt_data = [] 
    # save remaining
    if all_tt_data: 
        df = spark.createDataFrame(all_tt_data, time_schema)
        df.write.mode("append").parquet("./timetables.parquet")
    

## process timetable_change files in bactches and  write data- used batches for better speed
def process_change_files(spark, change_files, change_schema, batch_size ):
     # process timetable_changes files 
    all_chg_data = []
    #p#rint("processing changes...")
    for i, f in enumerate(change_files):

        all_chg_data.extend(extract_changes(f))

        if i % batch_size == 0 and i > 0:
            print(f"processed {i} files")
            df = spark.createDataFrame(all_chg_data, change_schema)

            if i == batch_size:
                df.write.mode("overwrite").parquet("./timetable_changes.parquet")
            else:
                df.write.mode("append").parquet("./timetable_changes.parquet")

            # clear memory     
            all_chg_data = []

    if all_chg_data: 
        df = spark.createDataFrame(all_chg_data, change_schema)
        df.write.mode("append").parquet("./timetable_changes.parquet")




def main():

    spark= setup_spark()
    ## deine schemas
    time_schema, change_schema = define_schema()
 
    weeks = []
    tt_base = "./timetables"
    chg_base = "./timetable_changes"

    weeks = find_weeks_periods(tt_base)
    time_files , change_files = fine_xml_files(weeks, tt_base, chg_base )

    # process timetables data in batch size of 1000 since I faced with memory full issue
    batch_size = 1000

    ## after getting all data of time table, want to process them 
    process_time_files(spark, time_files, time_schema, batch_size)
    process_change_files(spark, change_files, change_schema, batch_size )

    # check
    tt_df = spark.read.parquet("./timetables.parquet")
    chg_df = spark.read.parquet("./timetable_changes.parquet") 

    print("finished successfully:", tt_df.count(), chg_df.count())



if __name__== '__main__':
    main()
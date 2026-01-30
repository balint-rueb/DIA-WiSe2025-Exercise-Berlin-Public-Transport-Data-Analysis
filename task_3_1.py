import os
import glob
import tarfile
import io
import xml.etree.ElementTree as ET
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col

def setup_spark():
    return SparkSession.builder \
        .appName("Spark Task 3.1") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

def convert_time(timestr):
    if not timestr or len(timestr) != 10:
        return None
    year = int(timestr[0:2]) + 2000
    month = int(timestr[2:4])
    day = int(timestr[4:6])
    hour = int(timestr[6:8])
    minutes = int(timestr[8:10])
    return datetime(year, month, day, hour, minutes)

def parse_timetable_xml(xml_content):
    raws = []
    root = ET.fromstring(xml_content)
    station_name = root.get('station')
    for s in root.findall('s'):
        s_id = s.get("id")
        train_line = s.find('tl')
        category = train_line.get('c', '') if train_line is not None else ''
        number = train_line.get('n', '') if train_line is not None else ''
        owner = train_line.get('o', '') if train_line is not None else ''
        ## for avoid the reapting I compact two type of event codes in one loop
        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)

            if element is not None:
                raws.append({
                    'sid': s_id, 'station': station_name, 'event_type': event_type,
                    'category': category, 'number': number, 'owner': owner,
                    'line': element.get('l'), 'planned_time': convert_time(element.get('pt')),
                    'planned_platform': element.get('pp'), 'planned_path': element.get('ppth')
                })
    return raws

def parse_changes_xml(xml_content):
    raws = []
    root = ET.fromstring(xml_content)
    ## get the station name from the root
    station_name = root.get('station')
    for s in root.findall('s'):
        s_id = s.get('id')
        for event_tag, event_type in (('ar', 'arrival'), ('dp', 'departure')):
            element = s.find(event_tag)
            if element is not None:
                raws.append({
                    'sid': s_id, 'station': station_name, 'event_type': event_type,
                    'line': element.get('l', ''), 'changed_time': convert_time(element.get('ct')),
                    'cancellation_time': convert_time(element.get('clt')),
                    'cancellation_status': element.get('cs', ''),
                    'planned_platform': element.get('pp', ''), 'changed_path': element.get('cpth', '')
                })
    return raws


def define_schemas():
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

def process_archives(spark, base_path, schema, parser_func, output_path, partition_col_source):
    """Opens tarballs, reads XMLs, and saves them as partitioned Parquet files."""

    # 1. Find all .tar.gz files in the folder
    tar_files = sorted(glob.glob(os.path.join(base_path, "*.tar.gz")))
    if not tar_files:
        print(f"No archives found in {base_path}")
        return

    first_write = True
    for tar_path in tar_files:
        print(f"Processing: {os.path.basename(tar_path)}")
        # collect all data from the XML files in this tarball
        all_data = []
        # 2. Open the compressed tar.gz file
        with tarfile.open(tar_path, "r:gz") as tar:
            for member in tar.getmembers():
                # Only look for XML files inside
                if member.isfile() and member.name.endswith(".xml"):
                    f = tar.extractfile(member)
                    if f:
                        # Extract data from the XML and add it to our giant list
                        all_data.extend(parser_func(f.read()))
        
        if all_data:
            # Slice the data into smaller chunks to keep Spark tasks small otherwise Spark complains and we might run out of memory 
            chunk_size = 5000 
            # we iterate over all data in chunk_size steps
            for j in range(0, len(all_data), chunk_size):
                chunk = all_data[j:j + chunk_size]
                # Turn the Python list into a Spark Table
                df = spark.createDataFrame(chunk, schema)
                # Create a 'date' column so we can organize folders by day. This is what we use for partitioning.
                df = df.withColumn("date", to_date(col(partition_col_source)))
                
                # Only use 'overwrite' for the very first chunk of the very first file
                mode = "overwrite" if first_write else "append"
                df.write.mode(mode).partitionBy("date").parquet(output_path)
                first_write = False

def main():
    spark = setup_spark()
    time_schema, change_schema = define_schemas()

    # Process Timetables (Hourly snapshots)
    process_archives(spark, "./timetables", time_schema, 
                     parse_timetable_xml, "./timetables.parquet", "planned_time")

    # # Process Changes (15-min snapshots)
    # # We partition by changed_time here if planned_time is missing in changes
    process_archives(spark, "./timetable_changes", change_schema, 
                   parse_changes_xml, "./timetable_changes.parquet", "changed_time")
                        
    tt_df = spark.read.parquet("./timetables.parquet")
    chg_df = spark.read.parquet("./timetable_changes.parquet") 

    print("finished successfully:", tt_df.count(), chg_df.count())
    print(tt_df.select("date").distinct().count())
    # 1. Are the dates actually diverse?
    tt_df.groupBy("date").count().show()

    # 2. Are the timestamps actually working?
    tt_df.select("sid", "planned_time").show(5)

    # 3. Are there a lot of Nulls?
    print("Rows with missing times:", tt_df.filter(col("planned_time").isNull()).count())

    print("Task 3.1 Complete. Parquet datasets created with date partitioning.")

if __name__ == '__main__':
    main()
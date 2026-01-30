## task 3.2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = (SparkSession.builder
         .appName('ETL Analysis')
         .getOrCreate()
)

# load timetable and timetable_channges data ##########
timetables_path = './timetables.parquet'
timetables = spark.read.parquet(timetables_path)

timetable_changes_path = './timetable_changes.parquet'
changes = spark.read.parquet(timetable_changes_path)
#########################################################
 ## get the planned time and canclled time of the station 
station = "Berlin Anhalter Bf"

# join two data sets and filter them
joined_data = timetables.join(changes, on=['sid', 'station', 'event_type', 'date'], how='inner')


filtered_data = joined_data.filter(
    (col('station') == station) & 
    (col('cancellation_status') != 'c')
)

#######################################
# step 1: calculate delay by using above filtered data
delay_data = filtered_data.withColumn(
                      "delay_minutes", 
                      (unix_timestamp("changed_time") - unix_timestamp("planned_time")) / 60)   

# step 2: extract the date part of the dely_data
delay_daily_data = delay_data.withColumn("date", to_date("planned_time"))

## step 3: compute average delay per day
daily_average = delay_daily_data.groupBy("date")\
                     .agg(avg("delay_minutes").alias("avg_delay"))\
                     .orderBy("date")

daily_average.show()

## step 4: compute delay on over days
#avg = daily_average.agg(avg("delay_minutes")).collect()[0][0]
overall_avg = daily_average.agg(avg("avg_delay")).collect()[0][0]
print(f"Average delay: {overall_avg} minutes")
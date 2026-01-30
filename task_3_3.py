## task 3.3 - average number of departures per station during peak hours

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from datetime import datetime


spark = SparkSession.builder.appName("Station Peak Analysis").getOrCreate()


#  load parquet file
timetables_path = './timetables.parquet'  
timetables = spark.read.parquet(timetables_path)


# convert planned_time from string if necessary
if isinstance(timetables.schema["planned_time"].dataType, StringType):
    def parse_pt(pt):
        if pt is None or len(pt) != 10:
            return None
        day = int(pt[0:2])
        month = int(pt[2:4])
        year = int(pt[4:6]) + 2000
        hour = int(pt[6:8])
        minute = int(pt[8:10])
        return datetime(year, month, day, hour, minute)

    parse_pt_udf = F.udf(parse_pt, TimestampType())
    timetables = timetables.withColumn("planned_time", parse_pt_udf(F.col("planned_time")))

# check if we have a date column for grouping
timetables = timetables.withColumn("date", F.to_date("planned_time"))


#filter only departures

departures = timetables.filter(F.col("event_type") == "departure")


# compute morning peak average ( 7 to 9)
morning_peak = departures.filter((F.hour(F.col("planned_time")) >= 7) &
                                 (F.hour(F.col("planned_time")) < 9))

morning_avg = (morning_peak.groupBy("station", "date")
                            .count()
                            .groupBy("station")
                            .agg(F.avg("count").alias("morning_avg_departures (7-9)")))


# compute morning peak average ( 17 to 19)
evening_peak = departures.filter((F.hour(F.col("planned_time")) >= 17) &
                                 (F.hour(F.col("planned_time")) < 19))

evening_avg = (evening_peak.groupBy("station", "date")
                            .count()
                            .groupBy("station")
                            .agg(F.avg("count").alias("evening_avg_departures (17-19)")))

## join moring and evening average
avg_departures = (morning_avg.join(evening_avg, on="station", how="outer")
                              .orderBy("station"))

print("Average number of departures per station during peak hours (morning and evening):")
#avg_departures.show(truncate=False)

avg_departures.show(avg_departures.count(), truncate=False)

spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
# had to add these because I kept getting 'undefined' errors
from pyspark.sql.functions import col, array, concat

spark = SparkSession.builder.appName('Task_4_1_Graph').getOrCreate()

# 1. Cleaning the data and fixing the ID
# The sid in the file has a stop index at the end (like -05). 
# We use a regex to chop that off so all stops for one train have the same trip_id.
raw_data = spark.read.parquet('./timetables.parquet')
df = raw_data.withColumn("trip_id", F.regexp_replace(F.col("sid"), "-[0-9]+$", "")) \
             .withColumn("st_norm", F.upper(F.trim(F.col("station"))))

# 2. Building the "Roadmap" (Edges)
# We use a window to look at each trip and see which station follows another.
win = Window.partitionBy("trip_id").orderBy("planned_time")

# lag() lets us link the current station to the one before it.
# the window makes sure we only look within the same trip_id and in time order.
# We filter out Nulls (the first station has no previous) and select src/dst pairs.
# then we remove any duplicate edges.
# in the end we have a list of all direct connections between stations.
edges = df.withColumn("prev", F.lag("st_norm").over(win)) \
          .filter(F.col("prev").isNotNull()) \
          .select(F.col("prev").alias("src"), F.col("st_norm").alias("dst")) \
          .distinct()

# We union the edges with their reverse to make the graph undirected
full_graph = edges.union(edges.select(col("dst"), col("src"))).distinct().cache()

# 3. Finding the path using BFS
def find_path(start_st, end_st,hops=15):
    start, end = start_st.upper(), end_st.upper()
    
    # We start with a dataframe containing just the start station
    paths = spark.createDataFrame([(start, [start])], ["node", "path"])
    visited = {start} # set to keep track of where we've been
    
    for hop in range(1, hops+1):
        # Join current stations with the edge list to find the next set of neighbors
        next_hop = paths.join(full_graph, paths.node == full_graph.src) \
                        .select(F.col("dst").alias("node"), 
                                concat(F.col("path"), array(F.col("dst"))).alias("path"))
        
        # Don't go back to stations we already visited
        next_hop = next_hop.filter(~F.col("node").isin(list(visited)))
        
        # Check if we hit the target
        reached = next_hop.filter(F.col("node") == end).limit(1).collect()
        if reached:
            print(f"Found path in {hop} hops: {' -> '.join(reached[0]['path'])}")
            return
        
        # Update our visited list and continue
        new_nodes = next_hop.select("node").distinct().collect()
        if not new_nodes: break
        for n in new_nodes: visited.add(n['node'])
        
        paths = next_hop.cache() # Cache to keep things fast

find_path("BERLIN WESTEND", "BERLIN RATHAUS STEGLITZ")
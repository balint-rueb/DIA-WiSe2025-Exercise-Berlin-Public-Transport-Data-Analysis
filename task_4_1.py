from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize spark session for the graph task
spark = SparkSession.builder \
    .appName('TrainGraphRouting') \
    .getOrCreate()

raw_df = spark.read.parquet('./timetables.parquet')
# Split by '-' and take everything EXCEPT the last element
df = raw_df.withColumn("trip_id", F.regexp_replace(F.col("sid"), "-[0-9]+$", ""))

# Cleaning station names just to be safe
df = df.withColumn("st_norm", F.upper(F.trim(F.col("station"))))

# We partition by our 'trip_id' which stays the same for the whole journey
graph_win = Window.partitionBy("trip_id").orderBy("planned_time")

# Create edges by using lag to find previous station in the trip
raw_edges = df.withColumn("prev_st", F.lag("st_norm").over(graph_win)) \
    .filter(F.col("prev_st").isNotNull()) \
    .select(F.col("prev_st").alias("src"), F.col("st_norm").alias("dst")) \
    .distinct()

# The graph must be undirected since we care about transfers, not just one-way trips
# So we flip the edges and union them back together
flipped = raw_edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))
full_edges = raw_edges.union(flipped).distinct().cache()

print(f"Graph initialization complete. Unique edges: {full_edges.count()}")

def find_shortest_path(start, end, max_depth=12):
    # Start and end nodes need to be exactly as they appear in the DF
    print(f"Starting BFS search from {start} to {end}")
    
    # Initial state: just the starting station with itself as the path
    # Using a list for path tracking
    frontier = spark.createDataFrame([(start, [start])], ["node", "path"])
    
    # We keep track of visited to avoid infinite loops between stations
    visited = {start}
    
    for i in range(1, max_depth + 1):
        # Step 1: Find all neighbors of the current frontier
        # We join our current nodes with the source of our edges
        next_step = frontier.join(full_edges, frontier.node == full_edges.src) \
            .select(
                F.col("dst").alias("node"),
                F.concat(F.col("path"), F.array(F.col("dst"))).alias("path")
            )
        
        # Step 2: Filter out anything we've already seen to keep the DF small
        # This is a bit slow because we convert the set to a list for Spark
        next_step = next_step.filter(~F.col("node").isin(list(visited)))
        
        # Check if we have anything left to explore
        count = next_step.count()
        print(f"Iteration {i}: found {count} potential next stations")
        
        if count == 0:
            print("Search stopped: no more reachable nodes found.")
            break
            
        # Step 3: See if our target is in this new batch
        # We limit(1) because we only need the first shortest path we find
        match = next_step.filter(F.col("node") == end).limit(1).collect()
        
        if len(match) > 0:
            res_path = match[0]['path']
            print(f"Path found in {i} hops!")
            print(" -> ".join(res_path))
            return res_path

        # Step 4: Prepare for next iteration
        # We collect the new nodes to update the visited set in local python
        new_nodes = next_step.select("node").distinct().collect()
        for r in new_nodes:
            visited.add(r['node'])
            
        # Cache the frontier to stop the lineage from getting too long
        frontier = next_step.cache()

    print("Failed to find path within max depth.")
    return None

# Verify the names one last time before running
full_edges.filter(F.col("src").like("%BERLIN%")).select("src").distinct().show(10)

# Run search
target_start = "BERLIN WESTEND"
target_end = "BERLIN RATHAUS STEGLITZ"
find_shortest_path(target_start, target_end)
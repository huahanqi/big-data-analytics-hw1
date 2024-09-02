from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("White House Visitor Log Analysis - MapReduce Style") \
    .getOrCreate()

# Load the dataset
file_path = "./2022.04_WAVES-ACCESS-RECORDS.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Convert DataFrame to RDD for MapReduce operations
rdd = df.rdd

# record current time
from datetime import datetime
start_time = datetime.now()

# 1. The 10 most frequent visitors
# Map: Create a tuple (Visitor Full Name, 1)
visitor_rdd = rdd.map(lambda row: (f"{row['NAMELAST']}, {row['NAMEFIRST']} {row['NAMEMID'] if row['NAMEMID'] else ''}", 1))

# Reduce: Sum the occurrences
visitor_counts = visitor_rdd.reduceByKey(lambda a, b: a + b)

# Sort and take top 10
top_10_visitors = visitor_counts.sortBy(lambda x: x[1], ascending=False).take(10)

# 2. The 10 most frequently visited people
# Map: Create a tuple (Visitee Full Name, 1)
visitee_rdd = rdd.map(lambda row: (f"{row['VISITEE_NAMELAST']}, {row['VISITEE_NAMEFIRST']}", 1))

# Reduce: Sum the occurrences
visitee_counts = visitee_rdd.reduceByKey(lambda a, b: a + b)

# Sort and take top 10
top_10_visitees = visitee_counts.sortBy(lambda x: x[1], ascending=False).take(10)

# 3. The 10 most frequent visitor-visitee combinations
# Map: Create a tuple (Visitor -> Visitee, 1)
combination_rdd = rdd.map(lambda row: (
    f"{row['NAMELAST']}, {row['NAMEFIRST']} {row['NAMEMID'] if row['NAMEMID'] else ''} -> {row['VISITEE_NAMELAST']}, {row['VISITEE_NAMEFIRST']}", 1))

# Reduce: Sum the occurrences
combination_counts = combination_rdd.reduceByKey(lambda a, b: a + b)

# Sort and take top 10
top_10_combinations = combination_counts.sortBy(lambda x: x[1], ascending=False).take(10)

# 4. The 3 most frequent meeting locations
location_rdd = rdd.map(lambda row: (row['MEETING_LOC'], 1))

location_counts = location_rdd.reduceByKey(lambda a, b: a + b)

top_3_locations = location_counts.sortBy(lambda x: x[1], ascending=False).take(3)

# 5. The 10 most frequent callers
caller_rdd = rdd.map(lambda row: (f"{row['CALLER_NAME_LAST']}, {row['CALLER_NAME_FIRST']}", 1))

caller_counts = caller_rdd.reduceByKey(lambda a, b: a + b)

top_10_callers = caller_counts.sortBy(lambda x: x[1], ascending=False).take(10)


# Display the results
print("Most Frequent Visitors:")
for visitor in top_10_visitors:
    print(visitor)

print("\nMost Frequent Visitees:")
for visitee in top_10_visitees:
    print(visitee)

print("\nMost Frequent Visitor-Visitee Combinations:")
for combination in top_10_combinations:
    print(combination)

print("\nTop 3 Meeting Locations:")
for location, count in top_3_locations:
    print(f"{location}: {count}")

print("\nTop 10 Most Frequent Callers:")
for caller, count in top_10_callers:
    print(f"{caller}: {count}")

# record execution time in seconds
end_time = datetime.now()
execution_time = end_time - start_time
print(f"\nExecution time: {execution_time.total_seconds()} seconds")

# Stop the Spark session
spark.stop()

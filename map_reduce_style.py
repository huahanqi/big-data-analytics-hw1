from pyspark.sql import SparkSession
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()

# Load the dataset
file_path = "./2022.04_WAVES-ACCESS-RECORDS.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Using RDD for MapReduce approach
rdd = df.rdd

# record current time
start_time = datetime.now()

# 1. The 10 most frequent visitors
top_10_visitors = rdd \
    .map(lambda r: ("{}, {}, {}".format(r['NAMELAST'], r['NAMEFIRST'], r['NAMEMID']), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

print("\nTop 10 Most Frequent Visitors:")
for visitor in top_10_visitors:
    print(visitor)

# 2. The 10 most frequently visited people
top_10_visitees = rdd \
    .map(lambda r: ("{}, {}".format(r['VISITEE_NAMELAST'], r['VISITEE_NAMEFIRST']), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10) 

print("\nTop 10 Most Frequent Visitees:")
for visitee in top_10_visitees:
    print(visitee)

# 3. The 10 most frequent visitor-visitee combinations
top_10_combinations = rdd \
    .map(lambda r: ("{}, {}, {} -> {}, {}".format(r['NAMELAST'], r['NAMEFIRST'], r['NAMEMID'], r['VISITEE_NAMELAST'], r['VISITEE_NAMEFIRST']), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

print("\nTop 10 Most Frequent Visitor-Visitee Combinations:")
for combination in top_10_combinations:
    print(combination)


# 4. The 3 most frequent meeting locations
top_3_locations = rdd \
    .map(lambda r: (r['MEETING_LOC'], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(3)

print("\nTop 3 Most Frequent Meeting Locations:")
for location, count in top_3_locations:
    print(f"{location}: {count}")

# 5. The 10 most frequent callers
top_10_callers = rdd \
    .map(lambda r: ("{}, {}".format(r['CALLER_NAME_LAST'], r['CALLER_NAME_FIRST']), 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

print("\nTop 10 Most Frequent Callers:")
for caller, count in top_10_callers:
    print(f"{caller}: {count}")


# record execution time in seconds
end_time = datetime.now()
execution_time = end_time - start_time
print(f"\nExecution time: {execution_time.total_seconds()} seconds")

# Stop the Spark session
spark.stop()

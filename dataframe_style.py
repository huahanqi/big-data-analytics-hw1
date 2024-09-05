from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder.getOrCreate()

# Load the dataset
file_path = "./2022.04_WAVES-ACCESS-RECORDS.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Record current time
start_time = datetime.now()

# 1. The 10 most frequent visitors
# how to create new col using concat/_ws https://stackoverflow.com/questions/31450846/concatenate-columns-in-apache-spark-dataframe
top_10_visitors = df.withColumn("VISITOR_FULL_NAME", 
                                sf.concat_ws(", ", "NAMELAST", "NAMEFIRST", "NAMEMID")) \
                    .groupBy("VISITOR_FULL_NAME") \
                    .count() \
                    .orderBy('count', ascending=False) \
                    .limit(10)

# Display the results
print("Top 10 Most Frequent Visitors:")
top_10_visitors.show(truncate=False)

# 2. The 10 most frequently visited people
top_10_visitees = df.withColumn("VISITEE_FULL_NAME", 
                                sf.concat_ws(", ", "VISITEE_NAMELAST", "VISITEE_NAMEFIRST")) \
                    .groupBy("VISITEE_FULL_NAME") \
                    .count() \
                    .orderBy('count', ascending=False) \
                    .limit(10)

print("\nTop 10 Most Frequent Visitees:")
top_10_visitees.show(truncate=False)

# 3. The 10 most frequent visitor-visitee combinations
top_10_combinations = df.withColumn("VISITOR_VISITEE_COMBO", 
                                    sf.concat_ws(" -> ", 
                                                sf.concat_ws(", ", "NAMELAST", "NAMEFIRST", "NAMEMID"), 
                                                sf.concat_ws(", ", "VISITEE_NAMELAST", "VISITEE_NAMEFIRST"))) \
                        .groupBy("VISITOR_VISITEE_COMBO") \
                        .count() \
                        .orderBy('count', ascending=False) \
                        .limit(10)

print("\nTop 10 Most Frequent Visitor-Visitee Combinations:")
top_10_combinations.show(truncate=False)

# 4. The 3 most frequent meeting locations
top_3_locations = df.groupBy("MEETING_LOC") \
                    .count() \
                    .orderBy('count', ascending=False) \
                    .limit(3)

print("\nTop 3 Meeting Locations:")
top_3_locations.show(truncate=False)

# 5. The 10 most frequent callers
top_10_callers = df.withColumn("CALLER_FULL_NAME", 
                               sf.concat_ws(", ", "CALLER_NAME_LAST", "CALLER_NAME_FIRST")) \
                    .groupBy("CALLER_FULL_NAME") \
                    .count() \
                    .orderBy('count', ascending=False) \
                    .limit(10)

print("\nTop 10 Most Frequent Callers:")
top_10_callers.show(truncate=False)


# Record execution time in seconds
end_time = datetime.now()
execution_time = end_time - start_time
print(f"\nExecution time: {execution_time.total_seconds()} seconds")

# Stop the Spark session
spark.stop()

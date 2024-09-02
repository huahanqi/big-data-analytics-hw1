from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("White House Visitor Log Analysis - DataFrame API") \
    .getOrCreate()

# Load the dataset
file_path = "./2022.04_WAVES-ACCESS-RECORDS.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Record current time
start_time = datetime.now()

# 1. The 10 most frequent visitors
top_10_visitors = df.withColumn("Visitor Full Name", 
                                F.concat_ws(", ", "NAMELAST", "NAMEFIRST", F.coalesce("NAMEMID", F.lit('')))) \
                    .groupBy("Visitor Full Name") \
                    .agg(F.count("*").alias("Visit Count")) \
                    .orderBy(F.col("Visit Count").desc()) \
                    .limit(10)

# 2. The 10 most frequently visited people
top_10_visitees = df.withColumn("Visitee Full Name", 
                                F.concat_ws(", ", "VISITEE_NAMELAST", "VISITEE_NAMEFIRST")) \
                    .groupBy("Visitee Full Name") \
                    .agg(F.count("*").alias("Visit Count")) \
                    .orderBy(F.col("Visit Count").desc()) \
                    .limit(10)

# 3. The 10 most frequent visitor-visitee combinations
top_10_combinations = df.withColumn("Visitor-Visitee Combo", 
                                    F.concat_ws(" -> ", 
                                                F.concat_ws(", ", "NAMELAST", "NAMEFIRST", F.coalesce("NAMEMID", F.lit(''))), 
                                                F.concat_ws(", ", "VISITEE_NAMELAST", "VISITEE_NAMEFIRST"))) \
                        .groupBy("Visitor-Visitee Combo") \
                        .agg(F.count("*").alias("Combo Count")) \
                        .orderBy(F.col("Combo Count").desc()) \
                        .limit(10)

# 4. The 3 most frequent meeting locations
top_3_locations = df.groupBy("MEETING_LOC") \
                    .agg(F.count("*").alias("Location Count")) \
                    .orderBy(F.col("Location Count").desc()) \
                    .limit(3)

# 5. The 10 most frequent callers
top_10_callers = df.withColumn("Caller Full Name", 
                               F.concat_ws(", ", "CALLER_NAME_LAST", "CALLER_NAME_FIRST")) \
                   .groupBy("Caller Full Name") \
                   .agg(F.count("*").alias("Caller Count")) \
                   .orderBy(F.col("Caller Count").desc()) \
                   .limit(10)

# Display the results
print("Most Frequent Visitors:")
top_10_visitors.show(truncate=False)

print("\nMost Frequent Visitees:")
top_10_visitees.show(truncate=False)

print("\nMost Frequent Visitor-Visitee Combinations:")
top_10_combinations.show(truncate=False)

print("\nTop 3 Meeting Locations:")
top_3_locations.show(truncate=False)

print("\nTop 10 Most Frequent Callers:")
top_10_callers.show(truncate=False)

# Record execution time in seconds
end_time = datetime.now()
execution_time = end_time - start_time
print(f"\nExecution time: {execution_time.total_seconds()} seconds")

# Stop the Spark session
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, count

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("White House Visitor Log Analysis") \
    .getOrCreate()

# Load the dataset
file_path = "./2022.01_WAVES-ACCESS-RECORDS.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 1. Create Visitor Full Name and Visitee Full Name columns
df = df.withColumn("Visitor Full Name", concat_ws(", ", col("NAMELAST"), col("NAMEFIRST"), col("NAMEMID"))) \
       .withColumn("Visitee Full Name", concat_ws(", ", col("visitee_namelast"), col("visitee_namefirst")))

# 2. The 10 most frequent visitors
most_frequent_visitors = df.groupBy("Visitor Full Name").agg(count("*").alias("Visit Count")) \
                           .orderBy(col("Visit Count").desc()) \
                           .limit(10)

# 3. The 10 most frequently visited people
most_frequent_visitees = df.groupBy("Visitee Full Name").agg(count("*").alias("Visit Count")) \
                           .orderBy(col("Visit Count").desc()) \
                           .limit(10)

# 4. The 10 most frequent visitor-visitee combinations
df = df.withColumn("Visitor-Visitee Combo", concat_ws(" -> ", col("Visitor Full Name"), col("Visitee Full Name")))
most_frequent_combinations = df.groupBy("Visitor-Visitee Combo").agg(count("*").alias("Visit Count")) \
                               .orderBy(col("Visit Count").desc()) \
                               .limit(10)

# Show the results
print("Most Frequent Visitors:")
most_frequent_visitors.show()

print("Most Frequent Visitees:")
most_frequent_visitees.show()

print("Most Frequent Visitor-Visitee Combinations:")
most_frequent_combinations.show()

# Stop the Spark session
spark.stop()







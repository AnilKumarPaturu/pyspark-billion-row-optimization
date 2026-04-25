#!/usr/bin/env python
# coding: utf-8

# ## ingest_data
import requests
import os
import time
from pyspark.sql.functions import col, to_timestamp


FILE_sAVE_PATH = "/lakehouse/default/Files/raw_data/right_way/"
os.makedirs(FILE_sAVE_PATH, exist_ok=True)


# Sequential Ingestion -- The wrong way

start_time = time.time()
print("start time", start_time)
print("Sequential way of pulling data from NYC Taxi")
for month in range(1,13):
    NY_TAXI_BASE_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month:02d}.parquet"
    filename = f"yellow_tripdata_2023_{month:02d}.parquet"
    local_path = f"{FILE_sAVE_PATH}{filename}"

    print(f"Downloading file {filename}")
    response = requests.get(NY_TAXI_BASE_URL)
    with open(local_path, 'wb') as file:
        file.write(response.content)
    print(f"Done")
end_time = time.time()
print("end time", end_time) 
duration = end_time - start_time
print(f"Seqential File Download Duration: {duration:.2f} seconds")


# Parallel Ingestion - The right way

from concurrent.futures import ThreadPoolExecutor
def download_files(month):
    NY_TAXI_BASE_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-{month:02d}.parquet"
    filename = f"yellow_tripdata_2023_{month:02d}.parquet"
    local_path = f"{FILE_sAVE_PATH}{filename}"

    print(f"Downloading file {filename}")
    with requests.get(NY_TAXI_BASE_URL, stream=True) as r:
        r.raise_for_status()
        with open(local_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return f"Downloaded {filename}"

start_time = time.time()
print("start time", start_time)
print("Parallel way of pulling data from NYC Taxi")
with ThreadPoolExecutor(max_workers=12) as executor:
    results = executor.map(download_files, range(1,13))

for result in results:
    print(result)
end_time = time.time()
print("end time", end_time) 
duration = end_time - start_time
print(f"Parallel File Download Duration: {duration:.2f} seconds")


# Explosion --Creating 100GB Data(Problem)

from pyspark.sql.types import StructField,StructType,LongType,DoubleType,StringType,TimestampNTZType

spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.parquet.mergeSchema", "true")



taxi_schema = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True) # Use lowercase 'a'
])


from pyspark.sql import DataFrame
import traceback
from pyspark.sql.functions import col, lit, coalesce
from functools import reduce

# 1. The Harmonizer Function
# This takes ANY dataframe (Int, Long, weird casing) and forces it to a standard
def standardize_df(df: DataFrame) -> DataFrame:
    # Handle the "Airport_fee" casing issue if it exists
    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")
    
    # If airport_fee is missing completely (older files), add it as NULL
    if "airport_fee" not in df.columns:
        df = df.withColumn("airport_fee", lit(None).cast("double"))

    # Forcecast everything to the "Big" types (Long/Double)
    return df.select(
        col("VendorID").cast("long"),
        col("tpep_pickup_datetime"),
        col("tpep_dropoff_datetime"),
        col("passenger_count").cast("double"),
        col("trip_distance").cast("double"),
        col("RatecodeID").cast("long"),
        col("store_and_fwd_flag"),
        col("PULocationID").cast("long"),
        col("DOLocationID").cast("long"),
        col("payment_type").cast("long"),
        col("fare_amount").cast("double"),
        col("extra").cast("double"),
        col("mta_tax").cast("double"),
        col("tip_amount").cast("double"),
        col("tolls_amount").cast("double"),
        col("improvement_surcharge").cast("double"),
        col("total_amount").cast("double"),
        col("congestion_surcharge").cast("double"),
        col("airport_fee").cast("double")
    )

# 2. Iterative Read (The Loop)
dfs = [] # List to hold our standardized dataframes
root_path = "Files/raw_data/right_way"

print("Starting Iterative Read...")

# Loop through every month we downloaded
for month in range(1, 13):
    # Construct the specific path for this month
    path = f"{root_path}/yellow_tripdata_2023_{month:02d}.parquet"
    try:
        # Read just this ONE month. Spark won't crash because there's no conflict in a single file.
        df_temp = spark.read.parquet(path)
        # Immediately standardize it
        df_clean = standardize_df(df_temp)
        
        # Add to our list
        dfs.append(df_clean)
        print(f"Processed: {path}")
        
    except Exception as e:
        print(traceback.print_exc())
        # If a month is missing (e.g., future dates), just skip it
        print(f"Skipping {path} (Not found or empty)")

# 3. Union All (The Merge)
if dfs:
    print("Unioning all dataframes...")
    # reduce applies unionByName cumulatively to the list
    df_processed = reduce(DataFrame.unionByName, dfs)
    

    print(f"Success! Final Row Count: {df_processed.count()}")
    display(df_processed.limit(10))
else:
    print("Error: No data loaded.")


from pyspark.sql.functions import col, to_timestamp
df_processed = df_processed.toDF(*[c.lower() for c in df_processed.columns])
df_processed = df_processed.withColumn("VendorID", col("VendorID").cast("long")) \
                .withColumn("tpep_pickup_datetime",to_timestamp(col("tpep_pickup_datetime"))) \
                .withColumn("tpep_dropoff_datetime",to_timestamp(col("tpep_dropoff_datetime")))

df_multiplier = spark.range(50).withColumnRenamed("id","multiplier")
df_exploded = df_processed.crossJoin(df_multiplier).drop("multiplier")
df_exploded.write.mode("overwrite").parquet("Files/processed_data/bigdata_unoptimised")
print(f"Original count: {df_processed.count()}")
print(f"Exploded count: {df_exploded.count()}")


from notebookutils import mssparkutils
path = 'Files/processed_data/bigdata_unoptimised'
files = mssparkutils.fs.ls(path)
total_bytes = sum([f.size for f in files if f.name.endswith(".parquet")])
print(f"Total Size on disk: {total_bytes/(1024**3):.2f}")


# 1. Get the Spark Context
sc = spark.sparkContext

# 2. Print Key Resources
print(f"🔹 Application Name: {sc.appName}")
print(f"🔹 Spark Version: {sc.version}")
print(f"🔹 Master URL: {sc.master}")

# 3. Get Configuration Details
conf = sc.getConf()

# Note: In Fabric's dynamic allocation, 'spark.executor.instances' might not show up initially.
# We check the 'spark.executor.cores' (cores per worker) and total nodes.
try:
    print(f"\n⚙️  CONFIGURATION:")
    print(f"   • Driver Memory: {conf.get('spark.driver.memory')}")
    print(f"   • Executor Memory: {conf.get('spark.executor.memory')}")
    print(f"   • Cores per Executor: {conf.get('spark.executor.cores')}")
    print(f"   • Default Parallelism: {conf.get('spark.default.parallelism')}")
except:
    print("Some configs are dynamic and hidden.")

# 4. To see EVERYTHING (Warning: Long list)
# print(spark.conf.getAll())


# 1. Demonstrate the performance difference between a "Full Table Scan" and "Partition Pruning

df_unpartitioned = spark.read.format("parquet").load("Files/processed_data/bigdata_unoptimised")
df_filtered = df_unpartitioned.where(col("VendorId") == 1)
df_filtered.write.format("noop").mode("overwrite").save()


# The goal of Big Data Engineering is to make "Size of files read" as small as possible

# Partition Pruning

df_big = spark.read.format("parquet").load("Files/processed_data/bigdata_unoptimised")
print("Partitioning data by VendorID")
df_big.write.mode("overwrite").partitionBy("VendorID").parquet("Files/processed_data/bigdata_partitioned")


# Predicate PushDown


df_optimised = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df_filtered_opt = df_optimised.where(col("VendorID") == 1)
df_filtered_opt.write.format("noop").mode("overwrite").save()


# The Bad Join and Shuffle Trap


from pyspark.sql.types import StructType, StructField, LongType, StringType

# 1. Define the data (Match your VendorID type, which we cast to Long earlier)
data = [
    (1, "Creative Mobile Tech"),
    (2, "VeriFone Inc.")
]

# 2. Define the schema strictly
schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("VendorName", StringType(), True)
])

# 3. Create the DataFrame
df_vendors = spark.createDataFrame(data, schema)

# 4. Show it to verify
df_vendors.show()


spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set("spark.sql.shuffle.partitions", 2000)



df_optimised = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df_bad_join = df_optimised.join(df_vendors, "VendorID")
# df_grouped = df_bad_join.groupBy("VendorName").count()
print("🌪️ Starting the Shuffle Join (The Slow Way)...")
df_bad_join.write.format("noop").mode("overwrite").save()


# Optimal Way

from pyspark.sql.functions import broadcast
df_optimised = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df_good = df_optimised.join(
    broadcast(df_vendors),
    "VendorID"
)

df_good.write.format("noop").mode("overwrite").save()


# The Caching Trap (Recomputation)

# Create a heavy, complex DataFrame, and then use it to create Report A and Report B, Spark will literally forget the data after Report A and execute the entire recipe from scratch (reading from disk again) for Report B

df = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df_heavy = (df.filter(col("VendorID") ==2) 
            .withColumn("trip_duration", col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long") )
)
df_reportA = df_heavy.groupBy("payment_type").sum("fare_amount")
df_reportA.write.format("noop").mode("overwrite").save()

df_reportB = df_heavy.groupBy("RatecodeID").avg("trip_distance")
df_reportB.write.format("noop").mode("overwrite").save()


# The "Right" Way: Caching and Persistence

from pyspark import StorageLevel

df = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df_heavy = (df.filter(col("VendorID") ==2) 
            .withColumn("trip_duration", col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long") )
)

df_heavy.persist(StorageLevel.MEMORY_AND_DISK)
# count() forces Spark to scan the files and fill the cache NOW
# df_heavy.count()

df_reportA = df_heavy.groupBy("payment_type").sum("fare_amount")
df_reportA.write.format("noop").mode("overwrite").save()

df_reportB = df_heavy.groupBy("RatecodeID").avg("trip_distance")
df_reportB.write.format("noop").mode("overwrite").save()


# Data Skew -- Intentionally skewing a column and then fixing it

from pyspark.sql.functions import when,col


# Before grouping/joining, just skim the distribution of keys



df.groupBy("VendorID") \
.count() \
.orderBy(col("count").desc()) \
.show(10)


df = spark.read.parquet("Files/processed_data/bigdata_partitioned")
df = df.withColumn("skew_key", when(col("VendorID")== 2, "BIG_DATA").otherwise(col("VendorID").cast("string")))
display(df.count())


# Manually create a tiny DataFrame

df2 = spark.createDataFrame(data=[("BIG_DATA","This is the skewed record"),("1","Normal Vendor")],schema=["skew_key","vendor_name"])
display(df2)


spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)


skewed_join_df = df.join(df2, on="skew_key")
skewed_join_df.write.format("noop").mode("overwrite").save()


# Salting

from pyspark.sql.functions import col,lit,concat,rand,floor


df_big_salted = df.withColumn(
    "salted_key", concat(col("skew_key"),lit("-"),floor(rand()*20))
)
display(df_big_salted)


df_salt = spark.range(20).withColumnRenamed("id","salt_val")
df2_exploded = df2.crossJoin(df_salt)
display(df2_exploded)


df2_salted = df2_exploded.withColumn("salted_key",concat(col("skew_key"),lit("-"),col("salt_val")))


df_optimised_join = df_big_salted.join(df2_salted,on="Salted_key")
df_optimised_join.write.format("noop").mode("overwrite").save()




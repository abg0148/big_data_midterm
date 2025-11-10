# part a
from pyspark.sql.functions import *

df = spark.read.json("midterm/data/q4_sensor_data.json")

df = df.withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))

df2 = df.groupBy("sensor_id").agg(avg("temperature").alias("avg_temp"), count("*").alias("num_readings"), max("temperature").alias("max_temperature"), min("temperature").alias("min_temperature"))

df2.write.csv("midterm/data/q4_partA.csv", header=True, mode="overwrite")

# part b

result = (
    df.groupBy(window("timestamp", "5 minutes").alias("w"))
      .agg(
          min("temperature").alias("min_temp"),
          max("temperature").alias("max_temp"),
          avg("temperature").alias("avg_temp"),
          count(lit(1)).alias("count_readings"),
      )
      .select(
          col("w.start").alias("window_start"),
          col("w.end").alias("window_end"),
          round("avg_temp", 2).alias("avg_temp"),
          col("max_temp"),
          col("count_readings"),
      )
      .orderBy(col("avg_temp"))
)

result.write.csv("midterm/data/q4_partB.csv", header=True, mode="overwrite")

# 



from pyspark.sql import functions as F, types as T

# 1) Define schema (matches your JSON lines)
schema = T.StructType([
    T.StructField("sensor_id",  T.StringType(),  True),
    T.StructField("temperature",T.DoubleType(),  True),
    T.StructField("timestamp",  T.StringType(),  True),  # parse next
])

INPUT_DIR = "midterm/data/q4/"   # <-- a DIRECTORY you drop new files into

# 2) Streaming read from JSON files
df_raw = (
    spark.readStream
         .schema(schema)                 # required for file streams
         .option("mode", "DROPMALFORMED")# drop bad rows instead of failing
         .option("pathGlobFilter", "*.json") # only *.json files
         .option("recursiveFileLookup", "false")
         .option("maxFilesPerTrigger", 1)    # ingest a few files per trigger (tunable)
         .json(INPUT_DIR)                # directory, not a single file
)

# 3) Parse to event-time TimestampType + basic cleaning
df = (
    df_raw
      .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
      .filter(F.col("sensor_id").isNotNull() & (F.length("sensor_id") > 0))
      .filter(F.col("timestamp").isNotNull())
      .filter(F.col("temperature").isNotNull())
)

# 4) Example: Part A running aggregates to console every 10s
partA = (
    df.groupBy("sensor_id")
      .agg(
          F.avg("temperature").alias("avg_temp"),
          F.count(F.lit(1)).alias("count_readings"),
          F.min("temperature").alias("min_temp"),
          F.max("temperature").alias("max_temp"),
      )
      .select(
          "sensor_id",
          F.round("avg_temp", 2).alias("avg_temp"),
          "min_temp", "max_temp", "count_readings"
      )
)

qA = (
    partA.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", False)
         .trigger(processingTime="10 seconds")
         # .option("checkpointLocation", "chk/partA")  # use for non-toy runs
         .start()
)

# 5) Example: Part B tumbling window with watermark (append mode)
tumbling_5m = (
    df.withWatermark("timestamp", "2 minutes")
      .groupBy(F.window("timestamp", "5 minutes").alias("w"))
      .agg(
          F.avg("temperature").alias("avg_temp"),
          F.count(F.lit(1)).alias("count")
      )
      .select(
          F.col("w.start").alias("window_start"),
          F.col("w.end").alias("window_end"),
          F.round("avg_temp", 2).alias("avg_temp"),
          "count"
      )
)

qB1 = (
    tumbling_5m.writeStream
               .outputMode("append")
               .format("console")
               .option("truncate", False)
               .trigger(processingTime="10 seconds")
               # .option("checkpointLocation", "chk/tumbling_5m")
               .start()
)

# qA.awaitTermination()
# qB1.awaitTermination()

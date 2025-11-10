from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from functools import reduce
from operator import add
import sys

# Spark session
spark = SparkSession.builder.appName("Q3 JSON Processing").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Load
df = spark.read.json("midterm/data/q3_orders.json")

# Explode products
df2 = (
    df.select("*", F.explode("products").alias("product"))
      .drop("products")
      .withColumn("total_amount_per_order_id", F.col("total_amount"))
      .drop("total_amount")
)

# Flatten product struct
df3 = (
    df2.select(
        "order_id",
        "Customer_name",
        "order_date",
        F.col("product.product_id").alias("product_id"),
        F.col("product.name").alias("name"),
        F.col("product.quantity").alias("quantity"),
        F.col("product.price").alias("price"),
        "total_amount_per_order_id",
    )
    .withColumn("quantity", F.col("quantity").cast("long"))
    .withColumn("price",    F.col("price").cast("double"))
)

# --------------------
# PART A
# --------------------
df4 = (
    df3.groupBy("product_id", "name", "price")
       .agg(
           F.sum("quantity").alias("total_quantity"),
           F.countDistinct("order_id").alias("num_orders")
       )
       .withColumn("total_revenue", F.round(F.col("total_quantity") * F.col("price"), 2))
       .orderBy("product_id")
)

df4.coalesce(1).write.csv("midterm/output/q3_partA.csv", header=True, mode="overwrite")

# --------------------
# PART B
# --------------------
df5 = (
    df3.groupBy("Customer_name")
       .pivot("product_id")
       .agg(F.sum("quantity"))
       .fillna(0)
)

prod_cols = [c for c in df5.columns if c != "Customer_name"]
final_df = df5.withColumn(
    "Total",
    (reduce(add, [F.col(c) for c in prod_cols]) if prod_cols else F.lit(0))
)

final_df.coalesce(1).write.csv("midterm/output/q3_partB.csv", header=True, mode="overwrite")

# Stop spark and exit
spark.stop()
sys.exit(0)

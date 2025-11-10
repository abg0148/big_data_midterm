from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys

# Initialize Spark session
spark = SparkSession.builder.appName("Q2 E-commerce Processing").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Load data
df = spark.read.csv("midterm/data/online_retail.csv", inferSchema=True, header=True)

# Clean and cast columns
df2 = (
    df.withColumn("InvoiceDateTS", to_timestamp(trim(col("InvoiceDate")), "M/d/yyyy H:mm"))
      .withColumn("Quantity", col("Quantity").cast("int"))
      .withColumn("UnitPrice", col("UnitPrice").cast("double"))
      .withColumn("CustomerID", col("CustomerID").cast("int"))
)

# Filter invalid records
df2 = df2.filter(col("CustomerID").isNotNull() & ~col("InvoiceNo").startswith("C"))

# Compute total amount per line
df2 = df2.withColumn("TotalAmount", round(col("Quantity") * col("UnitPrice"), 2))

# ============================
# PART A
# ============================

# Total orders per customer (distinct invoices)
customer_order_count = df2.groupBy("CustomerID").agg(countDistinct("InvoiceNo").alias("num_orders"))

# Total spent per customer
customer_total_spent = df2.groupBy("CustomerID").agg(round(sum("TotalAmount"), 2).alias("total_spent"))

# Average order value
customer_avg_order_value = (
    customer_total_spent.join(customer_order_count, on="CustomerID")
    .select(
        col("CustomerID"),
        col("total_spent"),
        round(col("total_spent") / col("num_orders"), 2).alias("avg_order_value")
    )
)

# Most frequently purchased product per customer (by quantity)
customer_item_purchase_count = (
    df2.groupBy("CustomerID", "StockCode")
       .agg(sum("Quantity").alias("num_purchase"))
)

# Find the maximum quantity per customer
most_purchased_item_count_per_customer = (
    customer_item_purchase_count.groupBy("CustomerID")
    .agg(max("num_purchase").alias("most_purchased_item_count"))
)

# Join back to get product name
c = customer_item_purchase_count.alias("c")
m = most_purchased_item_count_per_customer.alias("m")

most_purchased_item_per_customer = (
    c.join(
        m,
        (c.CustomerID == m.CustomerID) &
        (c.num_purchase == m.most_purchased_item_count),
        "inner"
    )
    .select(
        c.CustomerID.alias("CustomerID"),
        c.StockCode.alias("StockCode"),
        c.num_purchase.alias("num_purchase")
    )
)

# Combine all features
final_result = (
    customer_order_count.alias("oc")
    .join(most_purchased_item_per_customer.alias("mp"), on="CustomerID", how="left")
    .join(customer_total_spent.alias("ts"), on="CustomerID", how="left")
    .join(customer_avg_order_value.alias("aov"), on="CustomerID", how="left")
    .select(
        col("oc.CustomerID").alias("CustomerID"),
        col("oc.num_orders").alias("num_orders"),
        col("mp.StockCode").alias("top_product"),
        col("mp.num_purchase").alias("most_purchased_item_count"),
        col("ts.total_spent").alias("total_spent"),
        col("aov.avg_order_value").alias("avg_order_value")
    )
)

final_result.coalesce(1).write.csv("midterm/output/q2_partA_output", header=True, mode="overwrite")

# ============================
# PART B
# ============================

# Window specification for each customer
window_spec = Window.partitionBy("CustomerID").orderBy("InvoiceDateTS")

# Add ranking and days since last order
df3 = (
    df2.withColumn("order_number", dense_rank().over(window_spec))
       .withColumn("last_order", lag(col("InvoiceDateTS"), 1).over(window_spec))
       .withColumn("days_since_last_order",
                   when(col("last_order").isNull(), 0)
                   .otherwise(datediff(col("InvoiceDateTS"), col("last_order"))))
)

# First and last product purchased
first_product_per_customer = (
    df3.filter(col("order_number") == 1)
       .select("CustomerID", col("StockCode").alias("first_product"))
       .dropDuplicates(["CustomerID"])
)

last_product_per_customer = (
    df3.withColumn("rn", row_number().over(window_spec.orderBy(col("InvoiceDateTS").desc())))
       .filter(col("rn") == 1)
       .select("CustomerID", col("StockCode").alias("last_product"))
       .dropDuplicates(["CustomerID"])
)

# Join both
first_and_last_product_per_customer = (
    first_product_per_customer.alias("fp")
    .join(last_product_per_customer.alias("lp"), on="CustomerID", how="left")
    .select(
        col("fp.CustomerID").alias("CustomerID"),
        col("fp.first_product").alias("first_product"),
        col("lp.last_product").alias("last_product")
    )
)

final_resultB = df3.join(first_and_last_product_per_customer, on="CustomerID", how="left")

final_resultB.coalesce(1).write.csv("midterm/output/q2_partB_output", header=True, mode="overwrite")

# Close Spark
spark.stop()
sys.exit(0)

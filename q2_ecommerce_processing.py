from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys

# Initialize spark session
spark = SparkSession.builder.appName("Q2 E-commerce Analysis").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")

# Load & clean
df = spark.read.csv("midterm/data/online_retail.csv", header=True, inferSchema=True)

df = (
    df.withColumn("InvoiceDateTS", to_timestamp(trim(col("InvoiceDate")), "M/d/yyyy H:mm"))
      .withColumn("Quantity", col("Quantity").cast("int"))
      .withColumn("UnitPrice", col("UnitPrice").cast("double"))
      .withColumn("CustomerID", col("CustomerID").cast("int"))
)

# Exclude rows without CustomerID and cancelled invoices (InvoiceNo starts with 'C')
clean = df.where(col("CustomerID").isNotNull() & ~col("InvoiceNo").startswith("C"))

# Line amount per row
clean = clean.withColumn("LineAmount", round(col("Quantity") * col("UnitPrice"), 2))

# ============================================================
# Part A
#   - num_orders (distinct invoices)
#   - total_spent (sum of order totals)
#   - avg_order_value
#   - most frequently purchased product = StockCode with most DISTINCT orders
# ============================================================

# Order totals per (CustomerID, InvoiceNo)
order_totals = (
    clean.groupBy("CustomerID", "InvoiceNo")
         .agg(
             round(sum("LineAmount"), 2).alias("OrderTotal"),
             max("InvoiceDateTS").alias("OrderDate")
         )
)

# Per-customer order counts and spend
cust_orders = (
    order_totals.groupBy("CustomerID")
                .agg(
                    countDistinct("InvoiceNo").alias("num_orders"),
                    round(sum("OrderTotal"), 2).alias("total_spent")
                )
)

# AOV
cust_aov = cust_orders.select(
    "CustomerID",
    "num_orders",
    "total_spent",
    round(col("total_spent") / col("num_orders"), 2).alias("avg_order_value")
)

# Most frequently purchased product (by DISTINCT orders)
cust_prod_orders = (
    clean.select("CustomerID", "StockCode", "InvoiceNo")
         .dropna()
         .dropDuplicates(["CustomerID", "StockCode", "InvoiceNo"])
         .groupBy("CustomerID", "StockCode")
         .agg(countDistinct("InvoiceNo").alias("orders_of_product"))
)

w_top = Window.partitionBy("CustomerID") \
              .orderBy(col("orders_of_product").desc(), col("StockCode").asc())

top_product = (
    cust_prod_orders
        .withColumn("rn", row_number().over(w_top))
        .where(col("rn") == 1)
        .select(
            col("CustomerID"),
            col("StockCode").alias("top_product"),
            col("orders_of_product").alias("most_purchased_item_count")
        )
)

finalA = (
    cust_aov.alias("a")
        .join(top_product.alias("t"), on="CustomerID", how="left")
        .select("CustomerID", "num_orders", "top_product",
                "most_purchased_item_count", "total_spent", "avg_order_value")
)

finalA.coalesce(1).write.csv("midterm/output/q2_partA_output", header=True, mode="overwrite")

# ============================================================
# Part B
#   - order_number per customer (by OrderDate)
#   - days_since_last_order
#   - first_product and last_product per customer
# ============================================================

w_order = Window.partitionBy("CustomerID").orderBy("OrderDate")

ranked = (
    order_totals
        .withColumn("order_number", dense_rank().over(w_order))
        .withColumn("last_order", lag("OrderDate", 1).over(w_order))
        .withColumn(
            "days_since_last_order",
            when(col("last_order").isNull(), 0).otherwise(datediff(col("OrderDate"), col("last_order")))
        )
)

# First and last invoices per customer
first_inv = ranked.filter(col("order_number") == 1) \
                  .select("CustomerID", col("InvoiceNo").alias("FirstInv"))

last_inv = (
    ranked.withColumn("rn_desc",
                      row_number().over(Window.partitionBy("CustomerID").orderBy(col("OrderDate").desc())))
          .filter(col("rn_desc") == 1)
          .select("CustomerID", col("InvoiceNo").alias("LastInv"))
)

# Map invoices back to representative StockCode
df_at_order = clean.select("CustomerID", "InvoiceNo", "StockCode", "InvoiceDateTS")

first_prod = (
    first_inv.join(df_at_order, on=["CustomerID", first_inv.FirstInv == df_at_order.InvoiceNo], how="left")
             .select(df_at_order.CustomerID, col("StockCode").alias("first_product"))
             .dropDuplicates(["CustomerID"])
)

last_prod = (
    last_inv.join(df_at_order, on=["CustomerID", last_inv.LastInv == df_at_order.InvoiceNo], how="left")
            .select(df_at_order.CustomerID, col("StockCode").alias("last_product"))
            .dropDuplicates(["CustomerID"])
)

finalB = (ranked.join(first_prod, on="CustomerID", how="left")
                .join(last_prod, on="CustomerID", how="left"))

finalB.coalesce(1).write.csv("midterm/output/q2_partB_output", header=True, mode="overwrite")

# Close Spark session and exit
spark.stop()
sys.exit(0)

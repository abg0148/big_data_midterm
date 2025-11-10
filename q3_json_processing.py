from pyspark.sql.functions import *

df = spark.read.json("midterm/data/q3_orders.json")

# exploding products array to have one product per row
# and renaming total amount to total_amount_per_order_id for clarity
df2 = df.select("*", explode("products").alias("product")).drop("products").withColumn("total_amount_per_order_id", col("total_amount")).drop("total_amount")

# flattening the product struct to have product fields as separate columns
df3 = df2.select("product.product_id", "product.name", "product.quantity", "product.price", "*").drop("product")

df4 = df3.groupBy("product_id", "name", "price").agg(sum("quantity").alias("total_quantity"), count_distinct("order_id").alias("num_orders")).withColumn("total_revenue", round(col("total_quantity")*col("price"),2)).orderBy("product_id")

df4.write.csv("midterm/data/q3_partA.csv", header=True, mode="overwrite")

# PART B
df5 = df3.groupBy("Customer_name").pivot("product_id").agg(sum("quantity")).fillna(0)
product_cols = [col for col in df5.columns if col != "Customer_name"]

from functools import reduce
from operator import add

final_df = df5.withColumn("Total", reduce(add, [col(c) for c in product_cols]))


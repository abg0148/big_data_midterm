from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *

df=spark.read.csv("midterm/data/online_retail.csv", inferSchema=True, header=True)


schema = StructType(
    [StructField('InvoiceNo', StringType(), True), 
     StructField('StockCode', StringType(), True), 
     StructField('Description', StringType(), True), 
     StructField('Quantity', IntegerType(), True), 
     StructField('InvoiceDate', StringType(), True), 
     StructField('UnitPrice', DoubleType(), True), 
     StructField('CustomerID', IntegerType(), True), 
     StructField('Country', StringType(), True)]
    )

df2 = df.withColumn(
        "InvoiceDateTS",
        to_timestamp(trim(col("InvoiceDate")), "M/d/yyyy H:mm"))

# filtered rows with null CustomerID
customer_order_count = df2.filter(col("CustomerID").isNotNull()).groupBy("CustomerID").agg(count("InvoiceNo").alias("num_orders"))

# total amount spent per order
df2 = df2.withColumn("TotalAmount", round(col("Quantity")*col("UnitPrice"),2))

customer_item_purchase_count = df2.groupBy("CustomerID", "StockCode").agg(count("*").alias("num_purchase"))
most_purchased_item_count_per_customer = customer_item_purchase_count.groupBy("CustomerID").agg(max("num_purchase").alias("most_purchased_item_count"))



c = customer_item_purchase_count.alias("c")
m = most_purchased_item_count_per_customer.alias("m")

most_purchased_item_per_customer = (
    c.join(
        m,
        on=[
            c.CustomerID == m.CustomerID,
            c.num_purchase == m.most_purchased_item_count
        ],
        how="inner"
    )
    .select(
        c.CustomerID.alias("CustomerID"),
        c.StockCode.alias("StockCode"),
        c.num_purchase.alias("num_purchase")
    )
)

# for each customer, print total number of orders, most purchased product (StockCode) and number of times it was purchased, total amount spent across all orders, average order value
customer_total_spent = df2.groupBy("CustomerID").agg(round(sum("TotalAmount"),2).alias("total_spent"))
customer_order_count = df2.groupBy("CustomerID").agg(count("InvoiceNo").alias("num_orders"))
customer_avg_order_value = customer_total_spent.join(customer_order_count, on="CustomerID").select(
    "CustomerID",
    "total_spent",
    (col("total_spent") / col("num_orders")).alias("avg_order_value")
)

final_result = customer_order_count.alias("oc") \
    .join(most_purchased_item_per_customer.alias("mp"), on="CustomerID") \
    .join(customer_total_spent.alias("ts"), on="CustomerID") \
    .join(customer_avg_order_value.alias("aov"), on="CustomerID") \
    .select(
        col("oc.CustomerID").alias("CustomerID"),
        col("oc.num_orders").alias("num_orders"),
        col("mp.StockCode").alias("top_product"),
        col("mp.num_purchase").alias("most_purchased_item_count"),
        col("ts.total_spent").alias("total_spent"),
        col("aov.avg_order_value").alias("avg_order_value")
    )
    
final_result.write.csv("midterm/output/q2_partA_output", header=True, mode="overwrite")

# PART B

# using window to rank orders by order date per customer
from pyspark.sql.window import Window

window_spec = Window.partitionBy("CustomerID").orderBy("InvoiceDateTS")

df3 = df2.withColumn("order_number", dense_rank().over(window_spec)).where(col("CustomerID").isNotNull())

df3 = df3.withColumn("last_order", lag(col("InvoiceDateTS"), offset=1).over(window_spec)).withColumn("days_since_last_order", datediff(col("InvoiceDateTS"), col("last_order")))

# replace null with 0 for days_since_last_order
df3 = df3.withColumn("days_since_last_order", when(col("days_since_last_order").isNull(), 0).otherwise(col("days_since_last_order")))

# first and last product purchased by each customer
last_product_per_customer = df3.withColumn("rn", row_number().over(window_spec.orderBy(col("InvoiceDateTS").desc()))).filter(col("rn") == 1).select("CustomerID", col("StockCode").alias("last_product")).alias("lp").select("CustomerID", "last_product")

first_product_per_customer = df3.filter(col("order_number") == 1).select("CustomerID", col("StockCode").alias("first_product")).alias("fp").select("CustomerID", "first_product")

first_and_last_product_per_customer = first_product_per_customer.alias("fp").join(last_product_per_customer.alias("lp"), on="CustomerID").select(
    col("fp.CustomerID").alias("CustomerID"),
    col("fp.first_product").alias("first_product"),
    col("lp.last_product").alias("last_product")
)

final_resultB = df3.join(first_and_last_product_per_customer, on="CustomerID")

final_resultB.write.csv("midterm/output/q2_partB_output", header=True, mode="overwrite")

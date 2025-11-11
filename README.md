### structure of the submitted zip for midterm

```
abg0148-big_data_midterm/
├── README.md                   # this file
├── bonus.py                    # bonus question script (if attempted)
├── gradic_rubric.md            # grading rubric for the midterm
├── q1_mapper1.py               # mapper 1 script for question 1
├── q1_reducer1.py              # reducer 1 script for question 1
├── q1_mapper2.py               # mapper 2 script for question 1
├── q1_reducer2.py              # reducer 2 script for question 1
├── q1_stats.py                 # stats script to print top words and totals
├── q2_ecommerce_processing.py  # script for processing e-commerce data in question 2
├── q3_json_processing.py       # script for JSON data processing in question 3
├── q4.ipynb                    # notebook for streaming data processing in question 4
└── submission_guidelines.md    # submission and setup guidelines
```

### how to setup

```bash
unzip abg0148-big_data_midterm.zip -d abg0148-big_data_midterm && cd abg0148-big_data_midterm
pip install -r requirements.txt
```

### data setup

move data from your local fs to hdfs

```bash
hadoop fs -mkdir midterm/data
hadoop fs -mkdir midterm/output

hadoop fs -put data/* midterm/data/
```

### NOTE:

> **Run the following commands to execute the scripts for the different questions**
>
> **Commands are followed by a brief explanation of whats happening in the scripts**
>
> **Please also look at the detailed comments provided within the scripts if you need help understanding the individual code components**

---

### Q1

**Explanation:**

> The task in Q1 was to process the text file `plato.txt` using two MapReduce jobs.
> The goal was to find **how many times each word appears**, the **distribution of word lengths**, and also the **total number of words** and **number of unique words** processed.
>
> In the first step, the mapper `q1_mapper1.py` reads each line, removes punctuation, converts text to lowercase, filters out stop words like *the, a, an, is, are*, and emits key-value pairs `{word} {1}`.
>
> The reducer `q1_reducer1.py` then adds up the counts for each word and stores the result as `{word}\t{count}` inside the folder `midterm/output/word_count`.
>
> In the second step, the mapper `q1_mapper2.py` reads the word counts and emits `{len(word)}\t{count}` so that each word contributes its frequency to its corresponding length bucket.
>
> The reducer `q1_reducer2.py` adds up all these counts for each word length and saves the result in `midterm/output/word_length_distribution`.
>
> Finally, the script `q1_stats.py` is used to print the **Top 20 words**, along with the **total number of words** and **unique word count**, for quick reference.

```bash
## commands to execute code for q1

# step 1: count words
mapred streaming -input midterm/data/plato.txt -output midterm/output/word_count \
-mapper "python3 q1_mapper1.py" -reducer "python3 q1_reducer1.py" \
-combiner "python3 q1_reducer1.py" -file q1_mapper1.py -file q1_reducer1.py

# step 2: word length distribution
mapred streaming -input midterm/output/word_count/* -output midterm/output/word_length_distribution \
-mapper "python3 q1_mapper2.py" -reducer "python3 q1_reducer2.py" \
-file q1_mapper2.py -file q1_reducer2.py

# step 3: print top words and statistics
hdfs dfs -cat midterm/output/word_count/part-* | python3 q1_stats.py > output/q1_report.txt
```

---

### Q2

**Explanation:**

> The task in Q2 was to analyze the dataset `online_retail.csv` using **PySpark** and generate useful customer insights.
> The script first cleans the data by **removing rows with missing `CustomerID`** and **excluding cancelled invoices** (Invoice numbers starting with “C”). It also converts data types for easier processing.
>
> **Part A:**
>
> * Calculates the **total number of distinct orders** each customer made by counting unique `InvoiceNo` values.
> * Computes the **total amount spent** by summing all line amounts (`Quantity * UnitPrice`) per order and then per customer.
> * Derives the **average order value (AOV)** by dividing total spent by number of orders.
> * Finds the **most purchased product** for each customer based on the **total quantity** bought (not number of invoices).
>
> **Part B:**
>
> * Uses **window functions** to order each customer’s purchases by date and assigns an `order_number` to each order.
> * Calculates **days since last order** (`days_since_last_order`) by finding the date difference between consecutive orders.
> * Finds the **first** and **last product** purchased by each customer by joining the earliest and latest orders with the product data.
>
> The results for both parts are saved as CSV files inside:
> `midterm/output/q2_partA_output/` and `midterm/output/q2_partB_output/`.

```bash
## commands to execute code for q2

# run locally in client mode
spark-submit --master local --deploy-mode client q2_ecommerce_processing.py

# output files:
#   midterm/output/q2_partA_output/
#   midterm/output/q2_partB_output/
```

---

### Q3

**Explanation:**

> The task in Q3 was to process nested JSON data from `q3_orders.json` using **PySpark**.
> Each record represents an order with customer details and an array of products, where each product has `product_id`, `name`, `quantity`, and `price`.
>
> The goal was to **flatten** the nested structure and compute summaries at both product and customer levels.
>
> **Part A:**
>
> * Explodes the `products` array to have one row per product per order.
> * Calculates the **total quantity** sold, **number of distinct orders**, and **total revenue** for each product.
> * Saves the summarized product-level data in `midterm/output/q3_partA.csv`.
>
> **Part B:**
>
> * Creates a **pivot table** showing how many of each product (`product_id`) was purchased by each customer.
> * Fills missing values with zero and adds a **Total** column showing each customer’s total items purchased.
> * Saves the customer–product pivot table in `midterm/output/q3_partB.csv`.

```bash
## commands to execute code for q3

spark-submit --master local --deploy-mode client q3_json_processing.py

# output files:
#   midterm/output/q3_partA.csv
#   midterm/output/q3_partB.csv
```


### Q4
for q4 wrote a notebook instead. this can be executed by uploading in jupyterhub assigned for the course

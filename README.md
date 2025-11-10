### structure of the submitted zip for midterm

```
ag10706_midterm.zip
├── data                        # contains all data files for all the questions
├── output                      # results and screenshots for all the expected outputs from all the questions
├── q1_mapper.py                # mapper script for question 1
├── q1_reducer.py               # reducer script for question 1
├── q2_ecommerce_processing.py  # script for processing e-commerce data in question 2
├── q3_json_processing.py       # script for JSON data processing in question 3
├── q4_streaming.py             # script for streaming data processing in question 4
├── bonus.py                    # bonus question script
├── README.md                   # this file
├── requirements.txt            # dependencies
├── gradic_rubric.md            # grading rubric for the midterm
└── submission_guidelines.md    # guidelines for submitting the midterm
```


### how to setup
```bash
unzip ag10706_midterm.zip -d ag10706_midterm && cd ag10706_midterm
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

> __Run the following commands to execute the scripts for the different questions__
\
\
> __Commands are followed by a brief explanation of whats happening in the scripts__
\
\
> __Please also look at the detailed comments provided within the scripts if you need help understanding the individual code components__


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
> * Computes the **total amount spent** by summing the total order values for each customer.
> * Derives the **average order value (AOV)** by dividing total spent by number of orders.
> * Finds the **most frequently purchased product** for each customer. This is done by counting how many **distinct invoices** contained each product (`StockCode`) and picking the one that appeared in the **most orders**.
>
> **Part B:**
>
> * Uses **window functions** to order each customer’s purchases by date and assigns an `order_number` to each.
> * Calculates the **number of days since the last order** (`days_since_last_order`) using the difference between consecutive order dates.
> * Identifies the **first** and **last product** purchased by each customer by finding products from their earliest and latest invoices.
>
> The results for both parts are written as CSV files inside the output folders:
> `midterm/output/q2_partA_output/` and `midterm/output/q2_partB_output/`.

```bash
## commands to execute code for q2

# run locally in client mode
spark-submit --master local --deploy-mode client q2_ecommerce_processing.py

# output files:
#   midterm/output/q2_partA_output/
#   midterm/output/q2_partB_output/
```

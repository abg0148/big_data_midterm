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

__Explanation:__
> Given our data in `plato.txt`, the task was to write map reduce jobs to compute __word frequency count__, __word length distribution__, __total number of words processed__, __unique word count__, 
\
> the first mapper produces `{word} {1}` as key and value, which is used as the input for first reducer, which aggregates counts for all the unique keys and outputs it in part files inside the folder `midterm/output/word_count`
\
> this is then used as input for the second mapper, which produces `{len(word)} {1}` as key value, which is used by the second reducer to aggregate word length distribution and is stored in `midterm/output/word_length_distribution`


```bash
## commands to execute code for q1

# command 1
mapred streaming -input midterm/data/plato.txt -output midterm/output/word_count -mapper "python q1_mapper1.py" -reducer "python q1_reducer1.py" -file q1_mapper1.py -file q1_reducer1.py

# command 2
mapred streaming -input midterm/output/word_count/* -output midterm/output/word_length_distribution -mapper "python q1_mapper2.py" -reducer "python q1_reducer2.py" -file q1_mapper2.py -file q1_reducer2.py
```

### Q2

commands to execute for q2
```bash
# was having issues with cluster mode on dataproc, so just ran commands in client mode
spark-submit --master local --deploy-mode client q2_ecommerce_processing.py
```

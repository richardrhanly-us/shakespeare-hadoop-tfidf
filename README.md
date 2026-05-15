# Distributed Text Analytics with Hadoop MapReduce

This project demonstrates a distributed text analytics pipeline built with Hadoop MapReduce and Java.  
The system processes a corpus of Shakespeare plays stored in HDFS (Hadoop Distributed File System) and computes TF-IDF scores to identify distinctive vocabulary within each play.

Technologies: Java, Hadoop, MapReduce, HDFS

The pipeline executes across multiple MapReduce jobs to demonstrate how large-scale text data can be processed in a distributed environment.

---

## Technologies Used

- Java
- Hadoop MapReduce
- HDFS
- Distributed Data Processing
- TF-IDF Text Analysis

---

## Dataset

The dataset consists of three Shakespeare plays:

- Hamlet
- All’s Well That Ends Well
- A Midsummer Night’s Dream

Each play is uploaded into HDFS and processed as a separate document in the corpus.

---

## Processing Pipeline

The text analytics workflow is implemented using three MapReduce stages.

### 1. Term Frequency (TF)

The first MapReduce job counts how many times each word appears in each document.

Output format:

```
document@word count
```

Example:

```
Hamlet.txt@ghost 65
Hamlet.txt@king 120
```

This stage measures how important a word is within a single document.

---

### 2. Document Frequency (DF)

The second MapReduce job determines how many documents contain each word.

Output format:

```
word document_count
```

Example:

```
ghost 1
king 3
```

This stage measures how common a word is across the entire corpus.

---

### 3. TF-IDF Calculation

The final MapReduce job combines TF and DF values to compute TF-IDF scores.

TF-IDF formula:

```
TF-IDF = TF * log(N / DF)
```

Where:

- TF = term frequency within the document  
- DF = number of documents containing the term  
- N = total number of documents in the corpus  

The system outputs the top 50 most distinctive terms per play.

---

## Understanding the Output

The final TF-IDF output identifies words that are both:

- frequent within a specific play
- relatively uncommon across the other plays in the corpus

This means the system is not simply finding the most common words overall. Instead, it highlights terms that are more **distinctive** to each individual document.

Example output:

```
Hamlet.txt -> hamlet     508.66
Hamlet.txt -> horatio    172.48
Hamlet.txt -> claudius   131.83
Hamlet.txt -> ophelia     94.48
```

### What these results mean

- hamlet receives a very high TF-IDF score because it appears often in *Hamlet* and is strongly associated with that play.
- horatio, claudius, and ophelia also score highly because they are important character names that occur frequently in *Hamlet* but rarely in the other plays.
- A common word such as the or and would receive a much lower TF-IDF value because it appears in all documents and is not distinctive.

In other words, the final output helps identify the vocabulary that best characterizes each play.

---

## Hadoop Execution

### Term Frequency Job

```
hadoop jar ShakespeareTFIDF.jar TFDriver /shakespeare/input /shakespeare/tf_output
```

### Document Frequency Job

```
hadoop jar ShakespeareTFIDF.jar DFDriver /shakespeare/tf_output /shakespeare/df_output
```

### TF-IDF Job

```
hadoop jar ShakespeareTFIDF.jar TFIDFDriver /shakespeare/tf_output /shakespeare/df_output /shakespeare/final_output
```

---

## How to Run

### Prerequisites

Before running this project, ensure the following are installed and configured:

- Java 8 or compatible JDK
- Hadoop
- HDFS
- A compiled JAR file containing the project classes

Hadoop services must be running before executing the jobs.

---

### Upload Dataset to HDFS

Create an input directory:

```
hdfs dfs -mkdir -p /shakespeare/input
```

Upload the text files:

```
hdfs dfs -put Hamlet.txt /shakespeare/input
hdfs dfs -put allswell.txt /shakespeare/input
hdfs dfs -put midsummer.txt /shakespeare/input
```

Verify upload:

```
hdfs dfs -ls /shakespeare/input
```

---

### Run the MapReduce Pipeline

Run the three jobs sequentially.

Term Frequency:

```
hadoop jar ShakespeareTFIDF.jar TFDriver /shakespeare/input /shakespeare/tf_output
```

Document Frequency:

```
hadoop jar ShakespeareTFIDF.jar DFDriver /shakespeare/tf_output /shakespeare/df_output
```

TF-IDF Calculation:

```
hadoop jar ShakespeareTFIDF.jar TFIDFDriver /shakespeare/tf_output /shakespeare/df_output /shakespeare/final_output
```

View final output:

```
hdfs dfs -cat /shakespeare/final_output/part-r-00000
```

---

### Output Directories

The pipeline produces three HDFS directories:

```
/shakespeare/tf_output
/shakespeare/df_output
/shakespeare/final_output
```

If rerunning the pipeline, remove old outputs first:

```
hdfs dfs -rm -r /shakespeare/tf_output
hdfs dfs -rm -r /shakespeare/df_output
hdfs dfs -rm -r /shakespeare/final_output
```

---

## Execution Screenshots

### Dataset Uploaded to HDFS

![HDFS Dataset Upload](screenshots/hdfs_dataset_upload.png)

---

### MapReduce Job 1 – Term Frequency

![TF Job Execution](screenshots/mapreduce_tf_job_execution.png)

---

### Term Frequency Output

![TF Output](screenshots/tf_output_results.png)

---

### MapReduce Job 2 – Document Frequency

![DF Job Execution](screenshots/mapreduce_df_job_execution.png)

---

### Document Frequency Output

![DF Output](screenshots/df_output_results.png)

---

### MapReduce Job 3 – TF-IDF Calculation

![TFIDF Job Execution](screenshots/mapreduce_tfidf_job_execution.png)

---

### Final TF-IDF Results

![TFIDF Final Results](screenshots/tfidf_final_results.png)

---

## Project Structure

```
shakespeare-hadoop-tfidf
│
├── src
│   ├── TFMapper.java
│   ├── TFReducer.java
│   ├── TFDriver.java
│   ├── DFMapper.java
│   ├── DFReducer.java
│   ├── DFDriver.java
│   ├── TFIDFMapper.java
│   ├── TFIDFReducer.java
│   └── TFIDFDriver.java
│
├── screenshots
│   ├── hdfs_dataset_upload.png
│   ├── mapreduce_tf_job_execution.png
│   ├── tf_output_directory.png
│   ├── tf_output_results.png
│   ├── mapreduce_df_job_execution.png
│   ├── df_output_results.png
│   ├── mapreduce_tfidf_job_execution.png
│   └── tfidf_final_results.png
│
└── README.md
```

---

## Key Takeaways

This project demonstrates:

- Distributed text processing with Hadoop MapReduce
- Multi-stage MapReduce pipelines
- HDFS data storage and data flow between jobs
- TF-IDF feature extraction for document analysis
- How distributed systems can analyze text corpora at scale

---

## Author

Richard Hanly


# Comparative Analysis Report

## Problem Chosen

The problem I chose was text processing and classification. Specifically, I made a rule-based support ticket classifier
that reads short help-desk or technology support tickets and assigns them to a specific category based on mathing keywords.
I chose this problem because it seemed like an appropriate way to illustrate how the chosen languages, Go and Prolog, each 
process data in different ways.

## Why These Languages Were Chosen

I chose the languages Go and Prolog because they represent two very different styles of programming, and make for
a good demonstration of the differences between each language.

Go was picked because it is a modern programming style that uses a procedural control flow, and is very practicle in most use cases. Go is
statically typed, which means that when variables are used they must have clear types before the program will run.
Go works well for this assignment because the program needs to read an input file, store ticket information, loop through the tickets, 
compare words to keyword lists, and print a report. These are all tasks that fit Go’s strengths because Go makes file 
handling, loops, structs, maps, slices, and functions very straightforward.

Prolog was chosen because it looks at the same problem in a very different way than Go. Prolog uses a relational model and control 
flow that establishes facts and rules, in contrast to Go's procedural approach. The ticket classifier is a great use case for Prolog 
as we can set the ticket categories as facts, and the classifications can be presented as rules that decide what category a
ticket belongs to. The Prolog control flow leans heavily on recursive predicates and rule matching to infer the category.



## Data Representation

The two implementations represent the same support ticket classification problem, but they store and organize the data in different ways.

###Go
In the Go version, each support ticket is represented with a `Ticket` struct. The struct has fields for the ticket ID and then the ticket description. 
This makes the data organized and predictable because each ticket has the same structure.

```go
type Ticket struct {
    ID          string
    Description string
}
```
The Go version also uses a second struct called ClassificationResult to store the result after a ticket has been analyzed.

```go
type ClassificationResult struct {
    TicketID string
    Category string
    Matches  []string
}
```

This structure stores the ticket ID, selected category, and the list of keywords that matches the ticket description.
This makes it a good fit for Go because it's data is statically typed. 

The category rules in Go are stored in a map. The map connects each category name to a slice of keywords.

categoryKeywords := map[string][]string{
    "printing": {"printer", "print", "queue", "paper", "jam", "jammed"},
    "network/wifi": {"wifi", "internet", "network", "connection"},
}

This means the category name is the key, and the list of words for that category is the value.

### Prolog

In the Prolog version, the data is represented more like facts and rules. Instead of using structs and maps, the category 
keywords are written as facts using the category_keywords/2 predicate.

category_keywords(printing,
    [printer, print, queue, paper, jam, jammed, toner, release]).

category_keywords('network/wifi',
    [wifi, internet, network, connection, router, wireless, connect]).

Each fact says that a category is connected to a list of keywords. This is a natural fit for Prolog because Prolog programs 
are built around facts, predicates, and relationships.

Tickets in the Prolog version are represented as compound terms:

ticket(ID, Description)

This is similar in purpose to the Go ClassificationResult struct, but it is written in the logic based style of prolog.

The main difference is that Go represents it's data through typed structures, maps, and slices, while Prolog represents data 
through facts, rule, lists, and logical terms. Go’s representation feels a bit more like building containers for data before processing it. 
Meanwhile, Prolog’s representation feels more like describing relationships that the program has to make sense of.

## How Each Implementation Thinks

This section will compare Go's step-by-step processing with Prolog's rule-based inference.

## Typing and Correctness

This section will compare Go's static typing with Prolog's dynamic logic-based structure.

## Main Mechanism of Computation

This section will discuss loops, helper functions, recursion, predicates, and rule matching.

## Difficulty and Clarity

This section will compare which implementation was easier to write, debug, and understand.

## Strengths and Weaknesses

This section will discuss the strengths and weaknesses of Go and Prolog for this problem.

## Final Judgment

This section will explain which language was better suited to this project and why.

## AI Usage Statement

This section will explain how AI was used, where it was not good enough, and what was learned.


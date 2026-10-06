# Data Engineering Practical Case Study: Global Earthquake Analysis

This document provides a comprehensive, end-to-end explanation of the Apache Spark (PySpark) Data Engineering project we built.

---

## 1. Project Overview
This project was built to fulfill a Data Engineering practical case study that requires setting up a distributed data processing environment, loading a dataset, applying transformations, optimizing performance, and analyzing the approach. 

To take it a step further (and achieve the bonus criteria), we implemented a **real-time data pipeline** using a live API, and we built a **modern Web Frontend** to visually showcase the distributed processing results.

---

## 2. Technology Stack
* **Apache Spark (PySpark):** The core distributed data processing engine used for handling large-scale data quickly.
* **Java 17 (Adoptium Temurin):** The necessary backend runtime required for Apache Spark to function.
* **Flask (Python):** A lightweight web framework used to expose our Spark pipeline as a REST API.
* **HTML / CSS / JavaScript:** Used to build the highly-interactive, responsive, modern dashboard.

---

## 3. The Dataset
Instead of a static CSV, we are fetching real-time data from the **United States Geological Survey (USGS)**. 
* **Endpoint:** `https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv`
* **Data Context:** Contains all reported earthquakes globally over the last 30 days. It updates dynamically every minute.

---

## 4. The Backend Engine (`app.py` & `RollNo_Name_CaseStudy.py`)
The backend is where the heavy lifting occurs. When a user requests data, the following pipeline executes:

### A. Environment Initialization
PySpark spins up a `SparkSession` in `local[*]` mode. This means it utilizes all available CPU cores on your local machine to simulate a distributed cluster.

### B. Data Ingestion
The Python `urllib` library downloads the latest CSV data from the USGS API to your local machine. Spark's `.read.csv()` method is then used to load this data into a distributed DataFrame.

### C. Distributed Transformations (Data Cleaning)
Spark doesn't compute data immediately; it builds a map of operations (DAG). We apply:
1. `filter(col("mag").isNotNull())`: Drops any rows where the magnitude wasn't recorded.
2. `withColumn(...)`: Uses string manipulation (`split` and `trim`) to extract the exact State or Country region from the verbose `place` string (e.g., transforming *"15km SW of Searles Valley, CA"* into *"CA"*).

### D. Performance Optimizations
1. **Shuffle Partitions:** We lowered `spark.sql.shuffle.partitions` from the default 200 to `4` to prevent massive overhead for a small dataset.
2. **Repartitioning:** We called `.repartition(4)` to force the data to be spread evenly across our 4 simulated worker nodes.
3. **Caching:** We called `.cache()` on the DataFrame. This pins the computed data in RAM. When we run multiple analyses, Spark reads from RAM instead of re-reading the CSV and re-computing the string manipulations.

### E. Actions (Materialization)
Finally, we trigger Spark to do the work using Actions:
* We run a `groupBy("region").count().orderBy()` query to find the most seismically active regions.
* We record the execution time. Then, we use `.unpersist()` to clear the cache, run the exact same query again, and record the uncached execution time.

---

## 5. The Frontend Dashboard (`templates/index.html`)

The frontend was built to provide a "WOW" factor. It utilizes **Glassmorphism** (frosted glass effects), CSS animations, and vibrant gradient typography to make the data presentation look premium.

When you open the page (`http://localhost:5000`), the frontend makes an AJAX fetch request to our Flask backend, triggering the entire Spark pipeline. While waiting, a sleek loading spinner is displayed. Once Spark finishes processing, the data populates three main UI cards:

### Card 1: Total Global Earthquakes
* **What it shows:** The total number of valid earthquake records processed in the last 30 days.
* **How Spark got it:** Spark ran a distributed `.count()` action across all partitioned data.

### Card 2: Spark Performance Benchmarks
* **What it shows:** A side-by-side comparison of execution times (in seconds).
* **Cached Execution:** The time it took Spark to group and sort the data when the dataset was already stored in RAM.
* **Uncached Execution:** The time it took Spark to group and sort the data from scratch.
* *Note: On local SSDs, these numbers might be close, but in a real big-data cluster over a network, the cached time is exponentially faster.*

### Card 3: Top 10 Most Active Seismic Regions
* **What it shows:** A grid of custom pill-shaped badges displaying the Region Name and its respective earthquake count.
* **How Spark got it:** Spark grouped millions of bytes of data by Region, aggregated the counts, sorted them in descending order, and returned the top 10 results. Usually, California (CA) and Alaska are at the top due to their massive sensor networks!

# QuakeSpark 🌋

A PySpark data engineering pipeline that processes real-time global earthquake data from the USGS API, executes distributed RDD/DataFrame transformations, benchmarks performance optimizations (caching), and visualizes insights via an interactive Flask dashboard.

---

## 📌 Project Overview

- **Real-Time Data Extraction**: Ingests live global earthquake dataset via USGS REST API.
- **Distributed PySpark Engine**: Executes data cleaning, state/region parsing, filtering, and aggregation.
- **Performance Optimization**: Benchmarks PySpark memory caching (`.cache()`) vs uncached RDD execution.
- **Interactive Dashboard**: Modern glassmorphism web dashboard displaying key metrics, top seismic regions, and visual charts.

---

## 📁 Repository Structure

```text
├── 124A8006_Arya_CaseStudy.py  # Standalone PySpark case study & benchmark script
├── app.py                      # Flask REST API backend & Spark dashboard server
├── templates/
│   └── index.html              # Modern web frontend UI (Glassmorphism + Charts)
├── Project_Documentation.md    # Detailed architecture & implementation report
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

---

## 🚀 Setup & Execution

### 1. Prerequisites
- Python 3.8+
- Java 8 / 11 / 17 (Required for PySpark execution)

### 2. Installation
```bash
pip install -r requirements.txt
```

### 3. Run PySpark Case Study Script
To run the standalone Spark data processing and performance analysis:
```bash
python 124A8006_Arya_CaseStudy.py
```

### 4. Launch Web Dashboard
To start the Flask dashboard:
```bash
python app.py
```
Open your browser and navigate to `http://localhost:5000`.

---

## 📊 Key PySpark Operations Implemented

1. **Extraction**: `urllib` download of USGS API (`all_month.csv`) -> `spark.read.csv()`
2. **Transformations**: `filter()`, `withColumn()`, `trim()`, `split()`, `repartition(4)`
3. **Caching**: `df.cache()`, `df.unpersist()`
4. **Actions**: `count()`, `show()`, `agg()`, `orderBy()`

---

## 🏆 Project Rubric & Highlights

- ✅ **Setup Environment**: PySpark SparkSession configured with custom partition tuning.
- ✅ **Real-Time Dataset**: Automated ingestion from live USGS Earthquake endpoints.
- ✅ **Performance Benchmarking**: Direct quantitative comparison of cached vs. uncached execution latency.
- ✅ **Frontend Presentation**: Custom responsive web dashboard for real-time visualization.

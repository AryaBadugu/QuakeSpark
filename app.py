import os
import sys
import time
import urllib.request
from flask import Flask, render_template, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count, split, trim

# Ensure PySpark uses the correct python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

app = Flask(__name__)

# Initialize Spark globally
spark = None

def get_spark_session():
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("Earthquake_Dashboard") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    return spark

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/earthquake-data')
def get_earthquake_data():
    spark = get_spark_session()
    
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
    data_path = "earthquakes_realtime.csv"
    
    # Download if not exists or if we want fresh data
    urllib.request.urlretrieve(url, data_path)
        
    df = spark.read.csv(data_path, header=True, inferSchema=True)
    df_clean = df.filter(col("mag").isNotNull())
    df_clean = df_clean.withColumn("region", trim(split(col("place"), ",").getItem(1)))
    
    df_partitioned = df_clean.repartition(4)
    df_partitioned.cache()
    
    # Materialize Cache
    start_time_load = time.time()
    total_records = df_partitioned.count()
    load_time = time.time() - start_time_load
    
    # CACHED GroupBy
    start_time_cache = time.time()
    top_regions_df = df_partitioned.filter(col("region").isNotNull()) \
                                   .groupBy("region") \
                                   .agg(count("*").alias("total_quakes")) \
                                   .orderBy(desc("total_quakes")).limit(10)
    top_regions = top_regions_df.collect()
    cached_action_time = time.time() - start_time_cache
    
    # UNCACHED GroupBy
    df_partitioned.unpersist()
    start_time_nocache = time.time()
    df_partitioned.filter(col("region").isNotNull()) \
                  .groupBy("region") \
                  .agg(count("*").alias("total_quakes")) \
                  .orderBy(desc("total_quakes")).limit(10).collect()
    uncached_action_time = time.time() - start_time_nocache
    
    results = {
        "total_records": total_records,
        "top_regions": [{"region": row['region'], "count": row['total_quakes']} for row in top_regions],
        "time_cached": round(cached_action_time, 4),
        "time_uncached": round(uncached_action_time, 4),
        "load_time": round(load_time, 4)
    }
    
    return jsonify(results)

if __name__ == '__main__':
    # Run the Flask app
    app.run(debug=True, port=5000)

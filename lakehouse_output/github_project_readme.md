# Retail E-Commerce ETL Pipeline (PySpark & Medallion Architecture)

A batch data engineering mini-project built with **Apache Spark (PySpark)** in Google Colab. This pipeline ingests raw retail e-commerce logs, enforces strict data quality rules, enriches order records with product catalog metadata, and produces partitioned analytics tables using the **Medallion Architecture (Bronze → Silver → Gold)**.

---

## 🏗️ Architecture Overview

The pipeline follows a multi-hop Medallion Lakehouse pattern:

```text
Raw CSVs (Orders & Products)
        │
        ▼
┌───────────────────┐
│   BRONZE LAYER    │  Explicit schema enforcement + ingestion audit metadata
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│   SILVER LAYER    │  Deduplication, null/domain filtering, timestamp parsing, Broadcast Join
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│    GOLD LAYER     │  Partitioned daily category sales & window-ranked customer leaderboards
└───────────────────┘
```

---

## ✨ Key Engineering Features

* **Explicit Schema Enforcement:** Uses PySpark `StructType` definitions instead of error-prone schema inference (`inferSchema=True`) to safely capture malformed records in the Bronze layer.
* **Data Quality & Cleansing:**
  * Deduplicates records based on `order_id`.
  * Filters out missing customer IDs, negative quantities, and invalid prices.
  * Standardizes text casing and safely parses timestamps using `try_to_timestamp()`.
* **Join Optimization:** Applies `F.broadcast()` on the smaller product lookup table to perform an in-memory `BroadcastHashJoin`, eliminating expensive shuffle overhead.
* **Analytical Modeling & Partitioning:**
  * Generates daily category sales metrics partitioned by `order_date` in columnar **Parquet** format.
  * Computes lifetime customer spend rankings using PySpark **Window functions** (`dense_rank()`).

---

## 📁 Repository Structure

```text
├── Retail_ETL_MiniProject.ipynb    # Complete Google Colab PySpark notebook
├── lakehouse/                      # Generated Parquet Lakehouse output
│   ├── bronze/
│   │   ├── orders/                 # Raw ingested orders with audit columns
│   │   └── products/               # Raw ingested product catalog
│   ├── silver/
│   │   └── enriched_orders/        # Cleansed, deduplicated, and joined orders
│   └── gold/
│       ├── daily_category_sales/   # Partitioned by order_date (Hive-style)
│       └── customer_rankings/      # Customer lifetime spend leaderboard
└── README.md                       # Project documentation
```

---

## 📖 Detailed Line-by-Line Code Walkthrough

### 1. Environment Setup & SparkSession Initialization
```python
!pip install -q pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Retail_ETL_MiniProject")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
```
* `!pip install -q pyspark`: Installs Apache Spark's Python API quietly in the Google Colab environment.
* `from pyspark.sql import SparkSession`: Imports the main entry point class used to program Spark with the DataFrame API.
* `from pyspark.sql import functions as F`: Imports built-in SQL functions (such as `col`, `upper`, `trim`, `round`, `sum`) under the standard alias `F`.
* `from pyspark.sql.types import ...`: Imports explicit data types needed to construct strict table schemas.
* `from pyspark.sql.window import Window`: Imports window specification utilities used for analytical rankings.
* `.appName("Retail_ETL_MiniProject")`: Assigns a readable identifier to the Spark application in logs and UI.
* `.master("local[*]")`: Configures Spark to run locally on the Colab VM using all available CPU cores.
* `.config("spark.sql.shuffle.partitions", "4")`: Reduces default shuffle partitions from `200` down to `4` to eliminate scheduling overhead on small datasets.
* `.getOrCreate()`: Retrieves an existing `SparkSession` or initializes a new one.

---

### 2. Synthetic Dataset Generation
```python
import os, csv
os.makedirs("/content/raw_data", exist_ok=True)
...
with open("/content/raw_data/orders_raw.csv", "w", newline="") as f:
    csv.writer(f).writerows(orders_rows)
```
* `os.makedirs("/content/raw_data", exist_ok=True)`: Creates the local landing directory for raw CSV files without throwing an error if the directory already exists.
* `csv.writer(f).writerows(...)`: Writes synthetic transaction logs (`orders_raw.csv`) and product metadata (`products_raw.csv`) to disk. The orders dataset intentionally includes duplicate order IDs, empty customer IDs, negative quantities, inconsistent status casing, and an `"invalid_date"` string to validate pipeline resilience.

---

### 3. Bronze Layer: Explicit Schema Enforcement & Raw Ingestion
```python
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("order_ts", StringType(), True),
    StructField("status", StringType(), True)
])

orders_bronze_df = (
    spark.read
    .option("header", True)
    .schema(orders_schema)
    .csv("/content/raw_data/orders_raw.csv")
)

orders_bronze_df = (
    orders_bronze_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.input_file_name())
)

orders_bronze_df.write.mode("overwrite").parquet("/content/lakehouse/bronze/orders")
```
* `StructType([...])` & `StructField(...)`: Explicitly defines each column name, data type, and nullability (`True`). `order_ts` is kept as `StringType` in Bronze so malformed strings like `"invalid_date"` are safely ingested without dropping rows on read.
* `.option("header", True)`: Instructs Spark to treat the first line of the CSV as column names.
* `.schema(orders_schema)`: Enforces our predefined schema instead of scanning the file twice with `inferSchema=True`.
* `.withColumn("ingested_at", F.current_timestamp())`: Appends an audit column capturing the exact UTC timestamp when the batch was ingested.
* `.withColumn("source_file", F.input_file_name())`: Appends lineage metadata recording the exact source file path for each row.
* `.write.mode("overwrite").parquet(...)`: Persists the raw DataFrame into the Bronze layer in compressed columnar Parquet format, replacing any previous run outputs.

---

### 4. Silver Layer: Cleansing, Standardization & Broadcast Enrichment
```python
cleaned_orders = (
    orders_df
    .dropDuplicates(["order_id"])
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    .filter(F.col("quantity") > 0)
    .filter(F.col("unit_price") > 0)
    .withColumn("status", F.upper(F.trim(F.col("status"))))
    .withColumn("parsed_ts", F.try_to_timestamp(F.col("order_ts"), F.lit("yyyy-MM-dd HH:mm:ss")))
    .filter(F.col("parsed_ts").isNotNull())
    .drop("order_ts")
    .withColumnRenamed("parsed_ts", "order_ts")
)
```
* `.dropDuplicates(["order_id"])`: Eliminates duplicate transaction events sharing the same `order_id`.
* `.filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))`: Enforces entity integrity by discarding records with missing or empty customer IDs.
* `.filter(F.col("quantity") > 0)` & `.filter(F.col("unit_price") > 0)`: Enforces domain rules by filtering out negative or zero quantities and prices.
* `F.upper(F.trim(F.col("status")))`: Removes leading/trailing whitespace and normalizes status values to uppercase (e.g., `"completed"` $\rightarrow$ `"COMPLETED"`).
* `F.try_to_timestamp(...)`: Safely parses timestamp strings into Spark `TimestampType`. Unlike strict `to_timestamp`, `try_to_timestamp` converts unparseable text (`"invalid_date"`) into `NULL` without throwing a `DateTimeException`.
* `.filter(F.col("parsed_ts").isNotNull())`: Removes any rows whose timestamps failed parsing.
* `.drop("order_ts")` & `.withColumnRenamed("parsed_ts", "order_ts")`: Drops the original string timestamp column first to prevent `AMBIGUOUS_REFERENCE` errors, then renames the clean timestamp column to `order_ts`.

```python
cleaned_orders = (
    cleaned_orders
    .withColumn("line_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("order_date", F.to_date(F.col("order_ts")))
)

enriched_orders = (
    cleaned_orders
    .join(
        F.broadcast(products_df.select("product_id", "product_name", "category")),
        on="product_id",
        how="left"
    )
)

enriched_orders.write.mode("overwrite").parquet("/content/lakehouse/silver/enriched_orders")
```
* `.withColumn("line_total", F.round(...))`: Computes gross revenue per line item rounded to 2 decimal places.
* `.withColumn("order_date", F.to_date(...))`: Extracts the calendar date (`yyyy-MM-dd`) from `order_ts` for downstream date partitioning.
* `.join(F.broadcast(...), on="product_id", how="left")`: Performs a left join to enrich orders with product names and categories. Wrapping the small `products_df` in `F.broadcast()` distributes a copy of the lookup table to all worker nodes, enabling a fast in-memory `BroadcastHashJoin` without shuffling the larger orders table.

---

### 5. Gold Layer: Business Aggregations & Window Analytics
```python
active_orders = silver_df.filter(F.col("status") != "CANCELLED")

daily_category_sales = (
    active_orders
    .groupBy("order_date", "category")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_total").alias("gross_revenue")
    )
)

daily_category_sales.write.mode("overwrite").partitionBy("order_date").parquet("/content/lakehouse/gold/daily_category_sales")
```
* `.filter(F.col("status") != "CANCELLED")`: Excludes cancelled transactions so Gold metrics reflect actual completed business revenue.
* `.groupBy("order_date", "category").agg(...)`: Aggregates data by date and product category to compute distinct order counts, total units sold, and gross revenue.
* `.partitionBy("order_date")`: Writes the output Parquet files into Hive-style date partition folders (`order_date=2026-10-01/`, etc.), accelerating downstream analytical queries that filter by date.

```python
customer_agg = (
    active_orders
    .groupBy("customer_id")
    .agg(
        F.sum("line_total").alias("total_spend"),
        F.countDistinct("order_id").alias("order_count")
    )
)

window_spec = Window.orderBy(F.desc("total_spend"))

customer_rankings = (
    customer_agg
    .withColumn("spend_rank", F.dense_rank().over(window_spec))
)

customer_rankings.write.mode("overwrite").parquet("/content/lakehouse/gold/customer_rankings")
```
* `.groupBy("customer_id").agg(...)`: Calculates lifetime spend and total completed order count for each customer.
* `Window.orderBy(F.desc("total_spend"))`: Defines an analytical window ordering customers from highest to lowest total spend.
* `F.dense_rank().over(window_spec)`: Assigns consecutive integer ranks (`1, 2, 3...`) without gaps in the event of tied spend amounts.

---

### 6. Verification & Execution Plan Inspection
```python
enriched_orders.explain("formatted")
!ls -R /content/lakehouse/gold/daily_category_sales
spark.read.parquet("/content/lakehouse/gold/daily_category_sales").show(truncate=False)
spark.read.parquet("/content/lakehouse/gold/customer_rankings").show(truncate=False)
```
* `.explain("formatted")`: Prints Spark's physical execution plan to verify that `BroadcastHashJoin` was chosen by the Catalyst optimizer.
* `!ls -R ...`: Lists directory contents recursively to verify that Hive-style `order_date=` partition folders were created on disk.
* `.show(truncate=False)`: Displays the full contents of both Gold analytical tables without truncating column strings.

---

## 🚀 How to Run

1. **Open in Google Colab:** Upload `Retail_ETL_MiniProject.ipynb` to [Google Colab](https://colab.research.google.com/).
2. **Execute Pipeline:** Go to **Runtime > Restart session and run all** to install PySpark, generate the sample raw datasets, and execute all ETL stages from top to bottom.
3. **Verify Outputs:** Inspect the final cells to confirm the `BroadcastHashJoin` execution plan, Hive-style partition folders, and aggregated Gold tables.

---

## 🛠️ Tech Stack

* **Language:** Python 3
* **Processing Engine:** Apache Spark / PySpark
* **Storage Format:** Apache Parquet (Snappy compressed)
* **Environment:** Google Colab
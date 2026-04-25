# PySpark Billion-Row Optimization Pipeline

## Overview
This repository contains an end-to-end data engineering project focused on diagnosing and resolving performance bottlenecks in distributed systems. Using a **1.9 billion row dataset** (NYC Taxi Data), this project demonstrates how to identify cluster failures in the Spark UI and implement code-level architectures to fix them.

## 🛠️ Tech Stack
* **Compute:** Apache Spark (PySpark), Adaptive Query Execution (AQE)
* **Storage:** Parquet, Delta Lake architecture
* **Concepts:** Data Skew, Salting, Broadcast Joins, Partition Pruning, Memory Management

---

## 🚀 Optimization Phases

### 1. Storage Optimization: Partition Pruning
**The Problem:** Querying the raw dataset resulted in massive full-table scans, consuming excessive network I/O.
**The Fix:** Partitioned the Parquet data physically by Year/Month. Leveraged the Catalyst Optimizer's predicate pushdown to read only the necessary partitions.
* **Result:** Drastic reduction in `Data Read` metrics in the Spark UI.

### 2. Network Optimization: Broadcast Joins vs. Shuffle Joins
**The Problem:** Joining a 1.9 billion row fact table with a tiny dimension table triggered a massive `SortMergeJoin`, causing unnecessary network shuffles across all executors.
**The Fix:** Forced a Broadcast Join (`broadcast()`), sending the small dimension table directly to the executors' memory.
* **Result:** Eliminated the `Exchange` step in the DAG and bypassed network shuffle write/read overhead.

### 3. Memory Management: Overcoming the "Caching Tax"
**The Problem:** Spark's Lazy Evaluation caused a branched pipeline to re-read 1.4 billion rows from disk multiple times, taking ~6 minutes per branch.
**The Fix:** Implemented strategic `.persist(StorageLevel.MEMORY_AND_DISK)` on the heavily transformed intermediate DataFrame.
* **Result:** Paid the initial "Caching Tax" on the first run, but reduced all subsequent downstream report generations from **5 minutes to 13 seconds**.
* ![Caching Optimization](images/caching_optimized.png) *(Note: Add your image_37997d.png here)*

### 4. Compute Optimization: Fixing Data Skew & Disk Spill
**The Problem:** A multi-key data skew on the `VendorID` column forced 120 million rows onto a single executor. This created a severe "straggler" task (3+ minutes while others finished in seconds) and caused over **300 MB of Disk Spill**, risking cluster failure.
**The Fix:** Engineered a Salting technique:
1. Appended a random integer (0-19) to the skewed key in the fact table.
2. Exploded the dimension table using a cross-join to match the 20 new salt values.
3. Joined on the new, distributed `salted_key`.
* **Result:** Distributed the heavy partition across **505 tasks**, equalized task duration, and eliminated Disk Spill entirely (0 Bytes).
* ![Zero Disk Spill After Salting](images/salting_success.png) *(Note: Add your image_6ec98d.png here)*

---

## ⚙️ Cluster & AQE Tuning
In addition to code-level fixes, this pipeline utilizes Spark 3.x Adaptive Query Execution (AQE) to dynamically handle minor skews and coalesce shuffle partitions. 

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

#!/bin/bash

main_dir="/home/consultant/spark"

### This code runs the SPARK code to 
### ingest data into HDFS for mysql database

python3 "$main_dir"/spark_ms.py

### Code create RAW, DSL in HIVE as well 
### as and Hbase tables for mysql database

hive -f "$main_dir"/hive_hb_ms2.hql

### This code runs the SPARK code to 
### ingest data into HDFS for postgres database

python3 "$main_dir"/spark_pg.py

### Code create RAW, DSL in HIVE as well as 
### and Hbase tables for mysql database

hive -f "$main_dir"/hive_hb_pg2.hql

### This code runs the SPARK code to 
### ingest data into HDFS for sqlserver database

python3 "$main_dir"/spark_ss.py

### Code create RAW, DSL in HIVE as well as 
## and Hbase tables for mysql database

hive -f "$main_dir"/hive_hb_ss2.hql

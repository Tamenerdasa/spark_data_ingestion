hdfs dfs -rm -r /usr/hadoop/spark_ms/*

hdfs dfs -rm -r /usr/hadoop/spark_pg/*

hdfs dfs -rm -r /usr/hadoop/spark_ss/*

hive -f cleaner_hive.hql 

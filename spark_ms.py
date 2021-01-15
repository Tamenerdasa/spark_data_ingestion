from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext

spark = SparkSession \
        .builder.appName("Mysql_Spark_DF") \
        .master("local[*]")\
        .getOrCreate()

sc = spark.sparkContext
sqlc = SQLContext(sc)

table_list = ['inpatient','outpatient','beneficiary']

for i in table_list:
	rdbms_df = spark.read \
	    .format("jdbc") \
	    .option('url', 'jdbc:mysql://localhost:3306/insurance') \
	    .option('user', 'root') \
	    .option('******', '') \
	    .option('dbtable', i) \
	    .load()

	rdbms_df.write.format("csv").save("/usr/hadoop/spark_ms/{}".format(i))

from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext

spark = SparkSession \
        .builder.appName("spark_pg") \
        .master("local[*]")\
        .getOrCreate()

sc = spark.sparkContext
sqlc = SQLContext(sc)

table_list = ['inpatient','outpatient','beneficiary']

for i in table_list:
	rdbms_df = spark.read \
	    .format("jdbc") \
	    .option('url', 'jdbc:postgresql://localhost:5432/insurance') \
	    .option('user', 'postgres') \
	    .option('password', 'greatnaolat0') \
	    .option('dbtable', i) \
	    .load()

	rdbms_df.write.format("csv").save("/usr/hadoop/spark_pg/{}".format(i))

from pyspark.sql import SparkSession
from pyspark import SparkConf,SQLContext,HiveContext,SparkContext

spark = SparkSession \
        .builder.appName("spark_pg") \
        .master("local[*]")\
        .config("spark.jars.extraClassPath","/usr/local/spark-2.4.7/jars/mssql-jdbc-8.4.1.jre8.jar") \
        .getOrCreate()

sc = spark.sparkContext
sqlc = SQLContext(sc)

table_list = ['inpatient','outpatient','beneficiary']

for i in table_list:
	rdbms_df = spark.read \
	    .format("jdbc") \
	    .option('url', 'jdbc:sqlserver://localhost:1433;databaseName=insurance') \
	    .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver") \
	    .option('user', 'SA') \
	    .option('*****', '') \
	    .option('dbtable', 'dbo.'+i) \
	    .load()

	#.options('driver',"com.microsoft.sqlserver.jdbc.SQLServerDriver") \
	rdbms_df.write.format("csv").save("/usr/hadoop/spark_ss/{}".format(i))

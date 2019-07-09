from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

spark = SparkSession.builder.appName("SurveyAnalyser").getOrCreate()

df = spark.read.text("demo.txt")
# df.show()
df.registerTempTable("demo")
# # Register the UDF with our SQLContext
#
spark.udf("CTOUDF", lambda txt: (txt.split(" ")))
spark.sql("SELECT  CTOUDF(value)  FROM demo").show()
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SurveyAnalyser").getOrCreate()

    # 1. Import the dataset and create data frames directly on import
    df = spark.read.format("csv").option("header", "true").load("trail.csv")
    df.show()

    # 2. Save data to file.
    df.write.csv('surveyoutput.csv')


    # 3. Check duplicates
    df.select(df.columns).distinct().show()


    # 4.  creates a lazily evaluated "view" that you can then use like a hive table in Spark SQL
    df.createOrReplaceTempView("survey")


    AgeGroup1 = spark.sql("SELECT * FROM survey where age <= 50")
    AgeGroup1.createOrReplaceTempView("AgeGroupOne")

    AgeGroup2 = spark.sql("SELECT * FROM survey where age > 50")
    AgeGroup2.createOrReplaceTempView("AgeGroupTwo")


    JoinedDF = spark.sql("Select * from AgeGroupOne union select * from AgeGroupTwo")
    JoinedDF.show()
    #
    # # Another way to union data

    df1 = df.filter(df['Age']<=50)
    df2 = df.filter(df['Age']>50)

    CombinedDF = df1.union(df2)
    CombinedDF.show()




    # Group by query
    df.groupBy(df['Treatment']).count().show()

    #Part2

    #Max Age
    MaxAge = spark.sql("SELECT MAX(Age)  FROM survey")
    MaxAge.show()
    #
    # # MIN Age
    MinAge = spark.sql("SELECT Min(Age)  FROM survey")
    MinAge.show()
    #
    # # Row number
    df13 = spark.sql("Select * from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
                     " where rownumber = 1 ")
    df13.show()

    df3 = spark.sql(
        "Select rownumber,timestamp,age from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where age <= 50 ")
    df4 = spark.sql(
        "Select rownumber,country,state from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where age <= 50 ")

    df3.join(df4, df3["rownumber"] == df4["rownumber"]).show()

    df5 = spark.sql(
        "Select rownumber,timestamp,age from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where Country == 'Germany' ")
    df6 = spark.sql(
        "Select rownumber,country,state from (select ROW_NUMBER() OVER (ORDER BY timestamp ) AS rownumber, * from survey) a"
        " where Country == 'Germany'  ")

    df5.join(df6, df5["rownumber"] == df6["rownumber"]).show()
    #
    #
    # # Group by
    df.groupBy(df['Treatment']).count().show()

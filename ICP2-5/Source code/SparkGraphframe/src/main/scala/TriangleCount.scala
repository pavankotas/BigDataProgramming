import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame
object TriangleCount {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load(".\\datasets\\201508_trip_data.csv")


    val update = input.select(input.col("Start Station"),input.col("End Station"),
      input.col("Duration"))

    // drop duplicates
    val df = update.dropDuplicates(update.columns)

    //Output dataframes
//    df.show()


    //cReate vertices
    val vertices= df.select("Start Station","Duration").
      withColumnRenamed("Start Station","id")
      .withColumnRenamed("Duration","duration")

    //Naming columns
    val edges = df.select("Start Station","End Station", "Duration")
      .withColumnRenamed("Start Station","src")
      .withColumnRenamed("End Station","dst")



    val g = GraphFrame(vertices,edges)

   val results = g.triangleCount.run()
    results.select(results.col("*")).show()

  }
}

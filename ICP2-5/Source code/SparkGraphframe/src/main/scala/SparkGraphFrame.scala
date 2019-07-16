import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
object SparkGraphFrame {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load(".\\datasets\\201508_trip_data.csv")




    // drop duplicates
    val df = input.dropDuplicates(input.columns)

    //Output dataframes
    df.show()


    //cReate vertices
    val vertices= df.select("Start Terminal","Start Station").
      withColumnRenamed("Start Terminal","id")
      .withColumnRenamed("Start Station","station")

    //Naming columns
    val edges = df.select("Start Terminal","End Terminal", "Duration")
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")



    val g = GraphFrame(vertices,edges)

    //Show vertices
    g.vertices.show()
    //Show edges
    g.edges.show()


   //   val motifs = g.find("(50)-[e]->(70); (70)-[e2]->(50)")
     //   motifs.show()

    val inDeg = g.inDegrees
    inDeg.orderBy(desc("inDegree")).limit(5).show()


    val outDeg = g.outDegrees
    outDeg.orderBy(desc("outDegree")).limit(5).show()



    //bonus1

g.degrees.show()

 //bonus2
    val Trips = g
      .edges
      .groupBy("src", "dst")
      .count()
      .orderBy(desc("count"))
      .limit(10)

    Trips.show()

    //BOnus 3
    val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
      .drop(outDeg.col("id"))
      .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

    degreeRatio.cache()

    degreeRatio.orderBy(desc("degreeRatio")).limit(10).show()
  //bonus4

  }
}

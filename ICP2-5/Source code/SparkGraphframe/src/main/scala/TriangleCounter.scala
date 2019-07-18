import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object TriangleCounter {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("PageRanking")
      .config("spark.master", "local")
      .getOrCreate()

    val input= spark.read.format("csv").option("header","true").load(".\\datasets\\demo.csv")

    // drop duplicates
    val df = input.dropDuplicates(input.columns)

    //    Output dataframes
    //        df.show()



    //create vertices
    val vertices= df.select("Start Terminal","Start Station").
      withColumnRenamed("Start Terminal","id")
      .withColumnRenamed("Start Station","station")

    //Naming columns
    val edges = df.select("Start Terminal","End Terminal", "Duration")
      .withColumnRenamed("Start Terminal","src")
      .withColumnRenamed("End Terminal","dst")

    val g=GraphFrame(vertices,edges)

    g.vertices.write.parquet("outputvertices")
    g.edges.write.parquet("outputedges")


    ////        Show vertices
    //        g.vertices.show()
    //        //Show edges
    //        g.edges.show()
    //
//        Finding TriangularCount
        val results= g.triangleCount.run()
        results.select("id", "count").show()

    //    BFS

//    val paths : DataFrame = g.bfs.fromExpr("id = '50'").toExpr("id = '47'").run()
//    paths.show()


    //Page rank

    val results2 = g.pageRank.resetProbability(0.15).tol(0.01).run()
    results2.vertices.show()
    results2.edges.show()
//
//
    val results3 = g.pageRank.resetProbability(0.15).maxIter(10).run()
    results3.vertices.show()
    results3.edges.show()



    val results4 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("50").run()
    results4.vertices.show()
    results4.edges.show()


  }
}



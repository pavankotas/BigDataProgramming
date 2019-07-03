import org.apache.spark._

object InvertedIndex {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","F:\\winutils");
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("input")
    // Split up into words.

    val map_ii =  input.map(_.split(" "))
      .flatMap(x => x.drop(1).map(y => (y, x(0))))
      .groupBy(_._1)
      .map(p => (p._1, p._2.map(_._2).toVector))

    println(map_ii.take(6).foreach(println))
    map_ii.saveAsTextFile("output")


  }
}

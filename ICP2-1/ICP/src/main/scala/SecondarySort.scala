import org.apache.spark._

object SecondarySort {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\winutils");
    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("inputSS.txt")
    // Split up into words.
    val intermediate = input.map(_.split(",")).map { k => ((k(0), k(1)), k(2)) }

    val intermediate2 = intermediate.groupByKey(2).flatMapValues(_.toList.combinations(2))


//    intermediate2.foreach { println }

    val output = intermediate2.mapValues { case (vals: List[(String, Integer)]) => ((vals(0), vals(1))) }



//    output.foreach { println }

    output.saveAsTextFile("secondarysort")

  }
}

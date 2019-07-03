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

    val inputData =  input.map(x => (
      (x.split(",")(0) + "-" + x.split(",")(1) , x.split(",")(3)) , x.split(",")(3) ));

    val intermediate = inputData.groupByKey(2).mapValues(iter => iter.toList.sortBy(k => k))

//
val outPut = intermediate.flatMap {
  case (label, list) => {
    list.map((label, _))
  }
}

    println(outPut.foreach(println))

    //    val map_ii =  input.map(_.split(","))
//      .flatMap(x => x.drop(1).map(y => (y, x(0))))
//      .groupBy(_._1)
//      .map(p => (p._1, p._2.map(_._2).toVector))
//
//    println(map_ii.take(6).foreach(println))
//    map_ii.saveAsTextFile("outputII")


  }
}

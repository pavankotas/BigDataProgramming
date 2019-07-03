/**
 * Illustrates flatMap + countByValue for wordcount.
 */


import org.apache.spark._

object CharacterCount {
    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir","C:\\winutils" )
      //val inputFile = args(0)
      //val outputFile = args(1)
      val conf = new SparkConf().setAppName("characterCount").setMaster("local[*]")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      //val input =  sc.textFile(inputFile)
      val input = sc.textFile("input.txt")
      // Split up into words.
      val words = input.flatMap(line => line.split(""))

      val chars = words.map(word =>(word(0),1));

      // Transform into word and count.
      val counts = chars.reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("outputCharacterCount")
    }
}

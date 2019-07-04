import org.apache.spark.sql.SparkSession

object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sc = SparkSession
      .builder
      .appName("SparkWordCount")
      .master("local[*]")
      .getOrCreate().sparkContext

    val input= sc.textFile("input")
    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()
    val output=wc.reduceByKey(_+_)
    val o=output.collect()
    print("Words:Count \n")
    o.foreach{case(word,count)=>{
      print(word+" : "+count+"\n")
    }}
  }
}

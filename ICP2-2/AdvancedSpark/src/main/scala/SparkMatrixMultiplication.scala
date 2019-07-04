import org.apache.spark.{SparkConf, SparkContext}

object SparkMatrixMultiplication {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\Winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val matrix1data=sc.textFile("data/A")
    val matrix2data=sc.textFile(path = "data/B")

    val matrix1=matrix1data.zipWithIndex()
      .flatMap{
      case(line,i)=>{
        var j =0
        val linesplit=line.split(" ")
        linesplit.map(f=>{
          j = j+1
          ("A",i+1,j.toLong,f.toInt)
        })
    }}

    val matrix2=matrix2data.zipWithIndex()
      .flatMap{
        case(line,i)=>{
          var j =0
          val linesplit=line.split(" ")
          linesplit.map(f=>{
            j = j+1
            ("B",i+1,j.toLong,f.toInt)
          })
        }}

    val lastelement1=matrix1.top(1)
    val lastelement2=matrix2.top(1)

    if(lastelement1(0)._3==lastelement2(0)._2)
      {
        println("Matrix A with " + lastelement1(0)._1 +" X "+lastelement1(0)._2)
        println("Matrix B with " + lastelement2(0)._1 +" X "+lastelement2(0)._2)
        println("Are compatible for Multiplication")
        val mm=matrix1.union(matrix2)
        val ABmm=mm.reduce{case(a,b)=>{
          var str="NAN"
          var i=0.toLong
          var j=0.toLong
          var value=0
        if((a._1!=b._1)&&(a._3==b._2)){
            str= "AB"
            i=a._2
            j=b._3
            value=a._4*b._4
        }
          (str,i,j,value)
        }}
        println(ABmm)


      }
    else
      {
        println("Matrix A with " + lastelement1(0)._1 +" X "+lastelement1(0)._2)
        println("Matrix B with " + lastelement2(0)._1 +" X "+lastelement2(0)._2)
        println("Are NOT compatible for Multiplication")
      }

  }

}

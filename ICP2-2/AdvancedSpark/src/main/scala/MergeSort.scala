import org.apache.spark.{SparkConf, SparkContext}

object MergeSort {
  def merge(xs: List[Int], ys: List[Int]): List[Int] =
    (xs, ys) match {
      case(Nil, ys) => ys
      case(xs, Nil) => xs
      case(x :: xs1, y :: ys1) =>
        if (x < y) x::merge(xs1, ys)
        else y :: merge(xs, ys1)
    }
  def mergeSort(xs: List[Int]): List[Int] = {
    val n = xs.length / 2
    if (n == 0) xs
    else {
      val (left, right) = xs splitAt(n)
      merge(mergeSort(left), mergeSort(right))
    }
  }
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c:\\winutils")
    val conf = new SparkConf().setAppName("MergeSort").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val b= sc.parallelize(List(List(5,4,9,8)))
    val result = b.map(mergeSort)
    print(result.take(10).toList)

  }
}
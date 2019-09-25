import org.apache.spark.{SparkConf, SparkContext}

class TextSample {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("TextSample").setMaster("local")
    val sparkContext = new SparkContext(config)
    val textFile = sparkContext.textFile("")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)
  }
}

import java.util.Calendar

import org.apache.spark.{SparkConf, SparkContext}

object TextFileSample {

  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("TextSample").setMaster("local")
    val sparkContext = new SparkContext(config)
    //      val textFile=sparkContext.textFile("src/resources/3_pig_demo.txt")
    val textFile = sparkContext.textFile("G:/AWS/Sample.txt")
    val count = textFile.filter(l => l.contains("1HDR"))
    count.foreach(w => w.substring(length()))
    count.foreach(println)
    val counts = textFile.flatMap(line => line.split("")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.foreach(println)
    val now = Calendar.getInstance().getTimeInMillis
    counts.saveAsTextFile("src/resources/" + now)
  }
}


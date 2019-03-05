import org.apache.spark.{SparkConf, SparkContext}

object Sss {
  def main(args: Array[String]): Unit = {
    println("Shanmugam")
/*    val a=List("SHan","Tdeam","Rock","Bottom")
    for(list<-a){
      println("List of Names::"+ list)
    }
    val newList=("Shanmugami":: a)
    for(b<-newList){
      println("NewList::" + b)
    }*/
    val Conf=new SparkConf().setAppName("WordCount").setMaster("local")
val sparkContext=new SparkContext(Conf)
val fileInput = sparkContext.textFile("src/resources/3_pig_demo.txt")
val counts = fileInput.flatMap(line=>line.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
    counts.foreach(println)

  }
}

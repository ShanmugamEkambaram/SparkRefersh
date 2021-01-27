import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StringType, StructField, StructType}

object ShanTesting {
  def main(args: Array[String]): Unit = {
   val SchemaOpt=StructType(Array(StructField("Name",StringType,true),StructField("Salary",StringType,true),StructField("Country",StringType,true)))
    val spark=SparkSession.builder().appName("Testing").master("local[*]").getOrCreate()
     val Df=spark.read.option("delimiter",",").option("quote","").option("header", "false").schema(SchemaOpt).csv("C:\\Users\\shanmugam\\Downloads\\08032019\\unzipped\\Big-Data-Tools-And-Exampls-master\\PigWordCount\\tutorial\\sales.csv")
    val rdD=Df.rdd.repartition(4)
    println("Number of Partitions:: "+rdD.getNumPartitions)
    Df.printSchema()


  }
}

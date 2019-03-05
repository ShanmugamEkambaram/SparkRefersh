import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait Config {

  lazy val sparkConfiguration = new SparkConf()
    .setAppName("Sample")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")
    .set("spark.executor.memory", "2g")
  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConfiguration)
    .getOrCreate()


}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.WindowSpec
object SparkAssignment {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("SparkAssignment").master("local[*]").getOrCreate()
    val InputFilePath="C:\\Users\\shanmugam\\Desktop";
    val OutputFilePath="";

    val InputDF = spark.read.parquet(InputFilePath+"\\part-00000-8b43448f-2282-4fe6-a5cc-64cdf61b750e-c000.snappy.parquet").dropDuplicates()

    // create window to perform aggregations
    val WndSpecDF = Window.partitionBy("entity_id", "item_id")

    // To identify old and new item id using month_id
    val AggDF = InputDF.withColumn("min_date", min(("month_id")).over(WndSpecDF))
      .withColumn("max_date", max(("month_id")).over(WndSpecDF))
      .withColumn("total_signal", sum("signal_count").over(WndSpecDF))
    // AggDF.show()


    val MinDateDF = AggDF.join(InputDF, AggDF("item_id") === InputDF("item_id") && AggDF("entity_id") === InputDF("entity_id") && AggDF("min_date") === InputDF("month_id"), "leftsemi")

    val FilterOldDF = MinDateDF.withColumn("row_number", row_number().over(WndSpecDF.orderBy("signal_count"))).where(col("row_number") === 1)
    // FilterOldDF.show()

    val MaxDateDF = AggDF.join(InputDF, AggDF("item_id") === InputDF("item_id") && AggDF("entity_id") === InputDF("entity_id") && AggDF("max_date") === InputDF("month_id"), "leftsemi")

    val FilterNewDF = MaxDateDF.withColumn("row_number", row_number().over(WndSpecDF.orderBy("signal_count"))).where(col("row_number") === 1)
    // FilterNewDF.show()

    val masterDF = FilterOldDF.join(FilterNewDF, FilterOldDF("entity_id") === FilterNewDF("entity_id") && FilterOldDF("item_id") === FilterNewDF("item_id"), "leftsemi")
      .withColumn("oldest_item_id", FilterOldDF("item_id"))
      .withColumn("newest_item_id", FilterNewDF("item_id"))

    val OutputDF = masterDF.select("entity_id", "oldest_item_id", "newest_item_id", "total_signal")
    OutputDF.coalesce(1).write.format("parquet").mode("append").save(OutputFilePath+"\\outputStats.snappy.parquet")
  }
}
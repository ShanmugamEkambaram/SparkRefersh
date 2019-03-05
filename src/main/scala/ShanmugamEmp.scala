import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ShanmugamEmp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ShanmugamEmp").master("local[*]").getOrCreate()
    import spark.implicits._
    //creating schema on the fly
    val customSchema = StructType(Array(
      StructField("sno", StringType, true),
      StructField("empid", StringType, true),
      StructField("empname", StringType, true),
      StructField("mailId", StringType, true),
      StructField("dob", StringType, true),
      StructField("doj", StringType, true), StructField("sex", StringType, true), StructField("reporting", StringType, true),
      StructField("department", StringType, true), StructField("designation", StringType, true), StructField("stats", StringType, true), StructField("date", StringType, true)))
    val df = spark.read.option("header", "true").option("delimiter", ",").csv("src/resources/employ.csv").as[Employee]
    df.createOrReplaceTempView("Shan")
    val inferSchemaDF = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(customSchema).load("src/resources/employ.csv")
    inferSchemaDF.createOrReplaceTempView("Ssd")
    val ssd = spark.sql("SELECT * FROM Ssd")
    ssd.show()
    val readingSoecificCoulmnFromDf = spark.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv("src/resources/employ.csv").select("emp_id", "emp_name", "DOB", "status")
    readingSoecificCoulmnFromDf.createOrReplaceTempView("SampleSelect")
    val sample = spark.sql("Select * from SampleSelect")
    sample.show(5)
    val shan = spark.sql("Select * from shan")
    shan.show(5)
  }
}

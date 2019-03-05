import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Shanmugam {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSQL For Csv")
      .master("local[*]")
      .getOrCreate()

    val schema = new StructType()
      .add(StructField("count", StringType, true))
      .add(StructField("val1", StringType, true))
    // Reading csv file
    val df = spark.read.option("header", "false").option("delimiter", ",").csv("src/resources/employee.csv")
    val readingSoecificCoulmnFromDf = spark.read.option("header", "false").option("delimiter", ",").csv("src/resources/employ.csv").select("emp_id", "emp_name", "reportingperson")
    readingSoecificCoulmnFromDf.createOrReplaceTempView("SampleSelect")
    val SelectSample = spark.sql("SELECT * FROM SampleSelect")
    SelectSample.show()
    df.printSchema()
    df.createOrReplaceTempView("Shanmugam1")
    val Emp = spark.sql("SELECT * FROM Shanmugam1")
    Emp.show()
    //Removing the null values
    val dfs = df.na.drop()
    dfs.createOrReplaceTempView("Shanmugam")
    val Employee = spark.sql("SELECT _c0,_c1,_c3,_c6,_c7,_c8,_c9,_c10,_c11,_c12 FROM Shanmugam")
    Employee.show()
  }
}

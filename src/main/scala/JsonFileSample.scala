import org.apache.spark.sql.SparkSession

object JsonFileSample {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
    /* val schema = new StructType()
       .add(StructField("userId", StringType, true))
       .add(StructField("jobTitleName", StringType, true))
       .add(StructField("firstName", StringType, true))
       .add(StructField("lastName", StringType, true))
       .add(StructField("preferredFullName", StringType, true))
       .add(StructField("employeeCode", StringType, true))
     .add(StructField("region", StringType, true))
     .add(StructField("phoneNumber", StringType, true))
     .add(StructField("emailAddress", StringType, true))
     val JsonData=sparkSession.read.schema(schema).json("src/resources/employees.json")
     JsonData.createOrReplaceTempView("JsonSample")
     sparkSession.sql("SELECT * FROM JsonSample").show()*/
    val df = spark.read.format("json").json("src/resources/employees.json")
    df.printSchema()
    df.createOrReplaceTempView("jsonsample")
    val data = spark.sql("SELECT * FROM jsonsample")
    data.show()
  }
}

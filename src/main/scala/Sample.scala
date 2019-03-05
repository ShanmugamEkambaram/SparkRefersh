import scala.io.Source

object Sample extends App with Config {
  val SQLDIR = "src/resources/select_emp_info.sql"
  //selecting specific columns from the data
  val dFSample = sparkSession.read.option("header", "true").option("delimiter", ",").option("inferSchema", "true").csv("src/resources/employ.csv").select("emp_id", "emp_name", "reportingperson", "status", "installeddate")
  dFSample.createOrReplaceTempView("SampleView")
  //Reading Query from file
  val select_cust_info = Source.fromFile(SQLDIR).getLines.mkString + " SampleView"
  //query from file
  sparkSession.sql(s"$select_cust_info").show()
  val sampleView = sparkSession.sql("select * from SampleView")
  dFSample.show()

}

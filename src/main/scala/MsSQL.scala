import scala.io.Source

object MsSQL extends App with Config {
  /* val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
   val df = spark.read.format("jdbc").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("url", "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2014").option("user", "shanmugam").option("password", "Nokia5300").option("dbtable", "select *from Address").load()
df.printSchema()*/
  val SQLDIR = "src/resources/select_emp_info.sql"
  val stay = Song("Stay", "Inna", 4)
  val select_cust_info = Source.fromFile(SQLDIR).getLines.mkString + " SampleView"
  println("value: " + stay.title + stay.artist + stay.track)
  val prop = new java.util.Properties()
  val url = "jdbc:mysql://localhost/olaichuvadidb"
  prop.put("driver", "com.mysql.jdbc.Driver")
  prop.put("user", "root")
  prop.put("password", "root")
  val df = sparkSession.read.jdbc(url, "oc_customer", prop)

  case class Song(title: String, artist: String, track: Int)

  df.show()
  /* val jdbcDF = sparkSession.read
     .format("jdbc")
     .option("url", "jdbc:mysql://localhost/olaichuvadidb").option("driver","com.mysql.jdbc.Driver")
     .option("dbtable", "oc_customer")
     .option("user", "root")
     .option("password", "root")
     .load()*/

  // jdbcDF.show()


}
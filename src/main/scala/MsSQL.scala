

object MsSQL {
  def main(args: Array[String]) {
    /* val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
     val df = spark.read.format("jdbc").option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").option("url", "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2014").option("user", "shanmugam").option("password", "Nokia5300").option("dbtable", "select *from Address").load()
df.printSchema()*/
    case class Song(title: String, artist: String, track: Int)
    val stay = Song("Stay", "Inna", 4)
    println("value: " + stay.title + stay.artist + stay.track)


  }
}
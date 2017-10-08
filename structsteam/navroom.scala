//import spark.implicits._
import org.apache.spark.sql.streaming.ProcessingTime
//import java.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class JDBCSink(url: String, user:String, pwd:String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row]{
  val driver = "com.mysql.jdbc.Driver"
  var connection:java.sql.Connection = _
  var statement:java.sql.Statement = _

  def open(partitionId: Long, version: Long):Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }
  def process(value: org.apache.spark.sql.Row): Unit = {
    statement.executeUpdate("replace INTO metadb.heat_map(EntityId,stime,indoors,aways,visits) " +
      "VALUES (" + value(0) + ",'" + value(1)  + "'," + value(2)  +"," + value(3) +","+ value(4) +");")
  }

  def close(errorOrNull:Throwable):Unit = {
    connection.close
  }
}

object navroom {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("Nav_heat_map").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val url="jdbc:mysql://namenode:3306/metadb?useSSL=false&user=sparkdb&password=sparkdbgood&useUnicode=true&characterEncoding=utf-8"

    val df_meta=spark.read.format("jdbc").option("url", url).option("dbtable", "v_entity_ap_rel").load()
    df_meta.cache()


    val surl="jdbc:mysql://namenode:3306/metadb?useSSL=false"
    val user="xxxx"
    val pwd="xxxxxxxx"
    val writer = new JDBCSink(surl, user, pwd)


    //-----------------------------------------
    //每分钟接收到的近处远处的countDistinct mac个数
    //-----------------------------------------
    val kaf_msg = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "namenode:6667").option("subscribe", "rawdata")
      .option("fetch.message.max.bytes", "50000000").option("kafka.max.partition.fetch.bytes", "50000000").option("startingOffsets","latest").load()
      .select(explode(split($"value","\n")).alias("value"))
      .withColumn("apmac",split($"value",",").getItem(2))
      .withColumn("mac",split($"value",",").getItem(3))
      .withColumn("rrssi",split($"value",",").getItem(4).cast("int"))
      .withColumn("stime",split($"value",",").getItem(5))
      .drop("value").as("a").join(df_meta.as("b"), $"a.apmac"===$"b.apmac")
      .filter($"stime".lt(current_timestamp())).filter($"stime".gt(from_unixtime(unix_timestamp(current_timestamp()).minus(5 * 60))))
      .selectExpr("entityid","case when a.rrssi>=b.rssi then concat(a.mac,'|','1') else concat(a.mac,'|','0') end indoor",
        "case when a.rrssi<b.nearbyrssi then concat(a.mac,'|','1') else concat(a.mac,'|','0') end away" ,
        "case when a.rrssi<b.rssi and a.rrssi>=nearbyrssi then concat(a.mac,'|','1') else concat(a.mac,'|','0') end visit" ,
        //"""concat(window(a.stime,"1 minutes").start ,"||",window(a.stime,"1 minutes").end) as stime""")
        """window(a.stime,"5 minutes").start as starttime""").as("c")
      //.dropDuplicates("entityid","indoor","away","visit","starttime")
      .withWatermark("starttime", "5 minutes")
      .groupBy($"c.entityid",$"c.starttime")
      .agg(approx_count_distinct($"indoor",0.01).alias("indoors"),approx_count_distinct($"away",0.01).alias("aways"),approx_count_distinct($"visit",0.01).alias("visits"))
    //.filter($"entityid"===101132)
    //-----------------------------------------
    //每分钟接收到的近处远处的countDistinct mac个数
    //-----------------------------------------

    val query=kaf_msg.writeStream.foreach(writer).outputMode("update").trigger(ProcessingTime("5 minutes")).start()
    query.awaitTermination()

  }

}


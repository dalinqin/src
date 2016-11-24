//////////////old version but it's very useful especially the using of broadcast and jdbcRDD
import java.sql.DriverManager
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import scala.collection.JavaConversions._

object StreamNavroom {
  //从mysql 取 meta数据并转化为dataframe 并且global 之
  case class Record_meta(entityid: Int, apmac: String, rssi: Int, indoorsecondsthrehold: Int, leaveminutesthrehold: Int)
  case class Record(apmac: String, sourcemac: String, rssi: Int, ttime: String, updatingtime: String)
  def main(args: Array[String]) {
    val sconf = new SparkConf().setAppName("StreamNavroom")
    val sc = new SparkContext(sconf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "namenode.navroomhdp.com,data1.navroomhdp.com,data2.navroomhdp.com")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    admin.disableTable(TableName.valueOf("t_indoor_tmp"))
    admin.truncateTable(TableName.valueOf("t_indoor_tmp"), false)

    val sql_meta = "select * from v_entity_ap_rel limit ?,?"

    val rdd_meta = new org.apache.spark.rdd.JdbcRDD(sc, () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://121.40.48.169:8301/maindb", "dbreader", "HelloN@vr00m")
    }, sql_meta, 0, 100000, 1,
      r => r.getInt("entityid") + "," + r.getString("apmac") + "," + r.getInt("rssi") + "," + r.getInt("indoorsecondsthrehold") + "," + r.getInt("leaveminutesthrehold"))


    val sss=hc.createDataFrame(rdd_meta.map(x => x.split(",")).map(x => Record_meta(x(0).toInt, x(1).toUpperCase, x(2).toInt, x(3).toInt, x(4).toInt))).collectAsList
    val broad_meta = sc.broadcast(sss)

    val sql_join ="""select distinct raw_data.sourcemac as sourcemac,
v_entity_ap_rel.entityid,
v_entity_ap_rel.indoorsecondsthrehold,
v_entity_ap_rel.leaveminutesthrehold,
max(unix_timestamp(raw_data.updatingtime,'yyyy-mm-dd hh:mm:ss')) as updatingtime
from v_entity_ap_rel ,raw_data
where raw_data.apmac=v_entity_ap_rel.apmac
and case when raw_data.rssi>v_entity_ap_rel.rssi then 1 else 0 end=1
and raw_data.rssi<=0
group by sourcemac,entityid,indoorsecondsthrehold,leaveminutesthrehold"""

    val topics = "oggtopic"
    val ssc = new StreamingContext(sc, Seconds(30))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "namenode:6667", "fetch.message.max.bytes" -> "104857600")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)

    lines.foreachRDD(rdd => {
      import hc.implicits._
      val df_raw = hc.createDataFrame(rdd.map(x => x.toString.split(",")).map(x => Record(x(1).trim.toUpperCase, x(2).trim.toUpperCase, x(3).toInt, x(4), x(5))))
      df_raw.registerTempTable("raw_data")
      val df_meta = hc.createDataFrame(broad_meta.value.map(x => Record_meta(x(0).toString.toInt, x(1).toString, x(2).toString.toInt, x(3).toString.toInt, x(4).toString.toInt)))
      df_meta.registerTempTable("v_entity_ap_rel")
      val df_rows = hc.sql(sql_join).coalesce(1)
      df_rows.foreachPartition { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "namenode.navroomhdp.com,data1.navroomhdp.com,data2.navroomhdp.com")
        conf.set("zookeeper.znode.parent", "/hbase-unsecure")
        val conn = ConnectionFactory.createConnection(conf)
        //val admin = conn.getAdmin();
        val table = conn.getTable(TableName.valueOf("t_indoor_tmp"))
        iter.foreach { x =>
          val SourceMac = x(0).toString
          val entityid = x(1).toString
          val indoorthrehold = x(2).toString
          val leavethrehold = x(3).toString
          val updatingtime = x(4).toString
          //println("smac",SourceMac,"eid",entityid,"indoorT",indoorthrehold,"LeaveT",leavethrehold,"utime",updatingtime)
          val filterList = new FilterList()
          filterList.addFilter(new PrefixFilter(Bytes.toBytes(SourceMac + '+'.toString + entityid.toString)))
          val single_done_filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("done"), CompareFilter.CompareOp.EQUAL, new SubstringComparator("0"))
          single_done_filter.setFilterIfMissing(true)
          filterList.addFilter(single_done_filter)
          val s = new Scan()
          s.setFilter(filterList)
          val scanner = table.getScanner(s)
          val scalaList: List[Result] = scanner.iterator.toList
          var etime = ""
          var ltime = ""
          //println("scalaList.length=",scalaList.length)
          if (scalaList.length > 0) {
            //println("there is record not done in hbases ,should be only one record marked as not done")
            for (result <- scalaList) {
              val rowkey = Bytes.toString(result.getRow())
              etime = rowkey.split('+').last
              ltime = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ltime")))
              if (updatingtime.toInt - ltime.toInt >= leavethrehold.toInt) {
                //update to hbase set done=1 表示当前记录进入时间与前1记录离开时间大于阀值，前一记录完结 ，更新前1记录为done=1，并且插入新记录
                val p = new Put(Bytes.toBytes(rowkey)) //rowkey
                //val seconds_done=updatingtime.toInt -ltime.toInt
                //p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes(seconds_done.toString))
                p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("1"))
                table.put(p)

                val new_rowkey = SourceMac + '+'.toString + entityid + '+'.toString + updatingtime.toString
                val p_new = new Put(Bytes.toBytes(new_rowkey)); //rowkey
                p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("0"))
                p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
                p_new.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes("0"))
                table.put(p_new)

              } else {
                // 当前时间 更新seconds和ltime
                val p = new Put(Bytes.toBytes(rowkey)); //rowkey
                p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
                val seconds = updatingtime.toInt - etime.toInt
                p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes(seconds.toString))
                table.put(p)
              }
            }
          } else {
            //println("no record in hbase ,we need to insert") //新记录来了，直接插入时间是0 done 是0
            val new_rowkey = SourceMac + '+'.toString + entityid + '+'.toString + updatingtime.toString
            val p = new Put(Bytes.toBytes(new_rowkey)) //rowkey
            p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
            p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes("0"))
            p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("0"))
            table.put(p)
          }
        }
        //after iteration hbase should be closed
        conn.close()
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}

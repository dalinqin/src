import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

object StreamNavroom {

  def main(args: Array[String]) {
    val sconf = new SparkConf().setAppName("StreamNavroom")
    val sc = new SparkContext(sconf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "namenode.navroomhdp.com,data1.navroomhdp.com,data2.navroomhdp.com")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    val conn = ConnectionFactory.createConnection(conf)
    val admin = conn.getAdmin()
    admin.disableTable(TableName.valueOf("t_indoor_tmp"))
    admin.truncateTable(TableName.valueOf("t_indoor_tmp"), false)


    val topics = args(0).toString
    val ssc = new StreamingContext(sc, Seconds(30))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "namenode:6667", "fetch.message.max.bytes" -> "104857600")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)

    lines.foreachRDD(rdd => {
      rdd.foreachPartition { iter =>
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        conf.set("hbase.zookeeper.quorum", "namenode.navroomhdp.com,data1.navroomhdp.com,data2.navroomhdp.com")
        conf.set("zookeeper.znode.parent", "/hbase-unsecure")
        val conn = ConnectionFactory.createConnection(conf)
        //val admin = conn.getAdmin();
        val table = conn.getTable(TableName.valueOf("t_indoor_tmp"))
        iter.foreach { x =>
          val eachLine = x.split("\n")
          val maxUpdatingtime = 0
          for (z <- eachLine) {
            val colums = z.split(",")
            val entityid = colums(1)
            val SourceMac = colums(2).toUpperCase
            val updatingtime = colums(3)
            val leavethrehold = colums(5)
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
                if ((updatingtime.toInt > etime.toInt) && (updatingtime.toInt > ltime.toInt)) {
                  if (updatingtime.toInt - ltime.toInt >= leavethrehold.toInt) {
                    //update to hbase set done=1 ,the custom revist (time span greater than leavethrehold
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
                    // update time and seconds
                    val p = new Put(Bytes.toBytes(rowkey)); //rowkey
                    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
                    val seconds = updatingtime.toInt - etime.toInt
                    p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes(seconds.toString))
                    table.put(p)
                  }
                }
              }
            } else {
              //println("no record in hbase ,we need to insert") //new record comes set  done to '0'
              val new_rowkey = SourceMac + '+'.toString + entityid + '+'.toString + updatingtime.toString
              val p = new Put(Bytes.toBytes(new_rowkey)) //rowkey
              p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ltime"), Bytes.toBytes(updatingtime.toString))
              p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("seconds"), Bytes.toBytes("0"))
              p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("done"), Bytes.toBytes("0"))
              table.put(p)
            }
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

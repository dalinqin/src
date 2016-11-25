import sys
from pyspark import SparkContext
from py4j.java_gateway import java_import
from pyspark.sql import HiveContext, Row,Window
import pandas
from datetime import datetime,timedelta,date
from pyspark.sql.functions import current_date, datediff, unix_timestamp, lit
import time
import json
import pyspark.sql.functions as func
from pyspark.sql.types import *
import subprocess

#indoor : 'clientmac','entityid','etime','ltime','seconds','utoday','ufirstday'
#flow   : 'clientmac','entityid','etime','ltime','utoday','ufirstday'
if __name__ == "__main__":

    def con_DateToUnix(strDate): #'2016-03-01' 
        l_strDate=strDate.split('-')
        start = date(int(l_strDate[0]), int(l_strDate[1]), int(l_strDate[2]))
        return int(time.mktime(start.timetuple()))
   
    def gen_report_table(hc,curUnixDay):
        rows_indoor=sc.textFile("/data/indoor/*/*").map(lambda r: r.split(",")).map(lambda p: Row(clientmac=p[0], entityid=int(p[1]),etime=int(p[2]),ltime=int(p[3]),seconds=int(p[4]),utoday=int(p[5]),ufirstday=int(p[6])))
        HiveContext.createDataFrame(hc,rows_indoor).registerTempTable("df_indoor")
        #ClientMac|etime|ltime|seconds|utoday|ENTITYID|UFIRSTDAY 
        sql="select entityid,clientmac,utoday,UFIRSTDAY,seconds,"
        sql=sql+"count(1) over(partition by entityid,clientmac) as total_cnt,"
        sql=sql+"count(1) over (partition by entityid,clientmac order by utoday range  2505600 preceding) as day_30," # 2505600 is 29 days
        sql=sql+"count(1) over (partition by entityid,clientmac order by utoday range  518400 preceding)  as day_7," #518400 is 6 days
        sql=sql+"count(1) over (partition by entityid,clientmac,UFIRSTDAY order by UFIRSTDAY  range 1 preceding) as pre_mon "
        sql=sql+"from df_indoor order by entityid,clientmac,utoday" 
        df_id_stat=hc.sql(sql)
        df_id_mm=df_id_stat.withColumn("min", func.min("utoday").over(Window.partitionBy("entityid","clientmac"))).withColumn("max", func.max("utoday").over(Window.partitionBy("entityid","clientmac")))
        #df_id_mm df_min_max ,to caculate firtarrival and last arrival 
        df_id_stat_distinct=df_id_stat.drop("seconds").drop("day_30").drop("day_7").drop("utoday").drop("total_cnt").distinct()
        #distinct df is for lag function to work
        df_id_prepremon=df_id_stat_distinct.withColumn("prepre_mon",func.lag("pre_mon").over(Window.partitionBy("entityid","clientmac").orderBy("entityid","clientmac","UFIRSTDAY"))).drop("pre_mon").na.fill(0)
        
        cond_id = [df_id_mm.clientmac == df_id_prepremon.clientmac, df_id_mm.entityid == df_id_prepremon.entityid, df_id_mm.UFIRSTDAY==df_id_prepremon.UFIRSTDAY]
        df_indoor_fin_tmp=df_id_mm.join(df_id_prepremon, cond_id, 'outer').select(df_id_mm.entityid,df_id_mm.clientmac,df_id_mm.utoday,df_id_mm.UFIRSTDAY,df_id_mm.seconds,df_id_mm.day_30,df_id_mm.day_7,df_id_mm.min,df_id_mm.max,df_id_mm.total_cnt,df_id_prepremon.prepre_mon)
        df_indoor_fin_tmp=df_indoor_fin_tmp.selectExpr("entityid as entityid","clientmac as  clientmac","utoday as utoday","UFIRSTDAY as ufirstday","seconds as secondsbyday","day_30 as indoors30","day_7 as indoors7","min as FirstIndoor","max as LastIndoor","total_cnt as indoors","prepre_mon as indoorsPrevMonth")
        
        #newly added part for indoors7 and indoors30 based on current date
        df_indoor_fin_tmp1= df_indoor_fin_tmp.withColumn("r_day_7", func.when((curUnixDay- df_indoor_fin_tmp.utoday)/86400<7 , 1).otherwise(0))
        df_indoor_fin_tmp2=df_indoor_fin_tmp1.withColumn("r_day_30", func.when((curUnixDay- df_indoor_fin_tmp1.utoday)/86400<30 , 1).otherwise(0))
        df_indoor_fin_tmp3=df_indoor_fin_tmp2.withColumn("r_indoors7",func.sum("r_day_7").over(Window.partitionBy("entityid","clientmac")))
        df_indoor_fin_tmp4=df_indoor_fin_tmp3.withColumn("r_indoors30",func.sum("r_day_30").over(Window.partitionBy("entityid","clientmac")))
        df_indoor_fin=df_indoor_fin_tmp4.drop("r_day_7").drop("r_day_30")
        hc.sql("drop table if exists df_indoor_fin")
        df_indoor_fin.write.saveAsTable("df_indoor_fin")
        
        rows_flow=sc.textFile("/data/flow/*/*").map(lambda r: r.split(",")).map(lambda p: Row(clientmac=p[0], entityid=int(p[1]),etime=int(p[2]),ltime=int(p[3]),utoday=int(p[4]),ufirstday=int(p[5])))
        HiveContext.createDataFrame(hc,rows_flow).registerTempTable("df_flow")
        
        # ClientMac|ENTITYID|UFIRSTDAY|etime|ltime|utoday
        sql="select entityid,clientmac,utoday,UFIRSTDAY,"
        sql=sql+"count(1) over(partition by entityid,clientmac) as total_cnt,"
        sql=sql+"count(1) over (partition by entityid,clientmac order by utoday range  2505600 preceding) as day_30," # 2505600 is 29 days
        sql=sql+"count(1) over (partition by entityid,clientmac order by utoday range  518400 preceding)  as day_7," #518400 is 6 days
        sql=sql+"count(1) over (partition by entityid,clientmac,UFIRSTDAY order by UFIRSTDAY  range 1 preceding) as pre_mon "
        sql=sql+"from df_flow order by entityid,clientmac,utoday" 
        df_fl_stat=hc.sql(sql)
        df_fl_mm=df_fl_stat.withColumn("min", func.min("utoday").over(Window.partitionBy("entityid","clientmac"))).withColumn("max", func.max("utoday").over(Window.partitionBy("entityid","clientmac")))
        #df_fl_mm df_min_max ,to caculate firtarrival and last arrival 
        df_fl_stat_distinct=df_fl_stat.drop("day_30").drop("day_7").drop("utoday").drop("total_cnt").distinct()
        #distinct df is for lag function to work
        df_fl_prepremon=df_fl_stat_distinct.withColumn("prepre_mon",func.lag("pre_mon").over(Window.partitionBy("entityid","clientmac").orderBy("entityid","clientmac","UFIRSTDAY"))).drop("pre_mon").na.fill(0)
        
        cond_fl = [df_fl_mm.clientmac == df_fl_prepremon.clientmac, df_fl_mm.entityid == df_fl_prepremon.entityid, df_fl_mm.UFIRSTDAY==df_fl_prepremon.UFIRSTDAY]
        df_flow_fin=df_fl_mm.join(df_fl_prepremon, cond_fl, 'outer').select(df_fl_mm.entityid,df_fl_mm.clientmac,df_fl_mm.utoday,df_fl_mm.UFIRSTDAY,df_fl_mm.day_30,df_fl_mm.day_7,df_fl_mm.min,df_fl_mm.max,df_fl_mm.total_cnt,df_fl_prepremon.prepre_mon)
        df_flow_fin=df_flow_fin.selectExpr("entityid as entityid","clientmac as  clientmac","utoday as utoday","UFIRSTDAY as ufirstday","day_30 as visits30","day_7 as visits7","min as FirstVisit","max as LastVisit","total_cnt as visits","prepre_mon as visitsPrevMonth")
        hc.sql("drop table if exists df_flow_fin")
        df_flow_fin.write.saveAsTable("df_flow_fin") 
        
        #df_flow_fin=hc.sql("select * from df_flow_fin")
        #df_indoor_fin=hc.sql("select * from df_indoor_fin")
        
        #cond_fin = [df_flow_fin.clientmac == df_indoor_fin.clientmac, df_flow_fin.entityid == df_indoor_fin.entityid, df_flow_fin.utoday==df_indoor_fin.utoday]
        #df_final= df_flow_fin.join(df_indoor_fin,cond_fin,'outer').select(df_flow_fin.entityid,df_flow_fin.clientmac,df_flow_fin.utoday,df_flow_fin.ufirstday,
        #df_indoor_fin.secondsbyday,df_indoor_fin.indoors30,df_indoor_fin.indoors7,df_indoor_fin.FirstIndoor,df_indoor_fin.LastIndoor,df_indoor_fin.indoors,df_indoor_fin.indoorsPrevMonth,df_indoor_fin.r_indoors7,df_indoor_fin.r_indoors30,
        #df_flow_fin.visits30,df_flow_fin.visits7,df_flow_fin.FirstVisit,df_flow_fin.LastVisit,df_flow_fin.visits,df_flow_fin.visitsPrevMonth).na.fill(0)
        #hc.sql("drop table if exists df_report_table")
		#hc.sql("drop table if exists df_report_table")
		#df_final.write.saveAsTable("df_report_table")

        #create table df_report_table as 
        #select df_flow_fin.entityid  as entityid,
        #       df_flow_fin.clientmac as clientmac,
        #       df_flow_fin.utoday    as utoday,
        #       df_flow_fin.ufirstday as ufirstday ,
        #       coalesce(df_indoor_fin.secondsbyday     ,cast(0 as bigint)) as secondsbyday    ,
        #       coalesce(df_indoor_fin.indoors30        ,cast(0 as bigint)) as indoors30       ,
        #       coalesce(df_indoor_fin.indoors7         ,cast(0 as bigint)) as indoors7        ,
        #       coalesce(df_indoor_fin.FirstIndoor      ,cast(0 as bigint)) as FirstIndoor     ,
        #       coalesce(df_indoor_fin.LastIndoor       ,cast(0 as bigint)) as LastIndoor      ,
        #       coalesce(df_indoor_fin.indoors          ,cast(0 as bigint)) as indoors         ,
        #       coalesce(df_indoor_fin.indoorsPrevMonth ,cast(0 as bigint)) as indoorsPrevMonth,
        #       coalesce(df_indoor_fin.r_indoors7       ,cast(0 as bigint)) as r_indoors7      ,
        #       coalesce(df_indoor_fin.r_indoors30      ,cast(0 as bigint)) as r_indoors30     ,
        #       coalesce(df_flow_fin.visits30           ,cast(0 as bigint)) as visits30        ,
        #       coalesce(df_flow_fin.visits7            ,cast(0 as bigint)) as visits7         ,
        #       coalesce(df_flow_fin.FirstVisit         ,cast(0 as bigint)) as FirstVisit      ,
        #       coalesce(df_flow_fin.LastVisit          ,cast(0 as bigint)) as LastVisit       ,
        #       coalesce(df_flow_fin.visits             ,cast(0 as bigint)) as visits          ,
        #       coalesce(df_flow_fin.visitsPrevMonth    ,cast(0 as bigint)) as visitsPrevMonth 
        #  from df_flow_fin left outer join df_indoor_fin
        #    on (df_flow_fin.clientmac = df_indoor_fin.clientmac and 
        #        df_flow_fin.entityid = df_indoor_fin.entityid and
        #        df_flow_fin.utoday = df_indoor_fin.utoday);
		
    '''------------ all function definition are here end -------------'''


    sc = SparkContext(appName="Main")
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")
    hc=HiveContext(sc)
    
    curDay=str(time.strftime("%Y-%m-%d",time.localtime(time.time())))
    curUnixDay=con_DateToUnix(curDay)
    
    
    #hc.sql("drop table if exists df_indoor_fin")
    #hc.sql("drop table if exists df_flow_fin")
    gen_report_table(hc,curUnixDay)

    java_import(sc._jvm,"org.apache.hadoop.fs.FileUtil")
    java_import(sc._jvm,"org.apache.hadoop.fs.FileSystem")
    java_import(sc._jvm,"org.apache.hadoop.fs.Path")
    java_import(sc._jvm,"java.sql.DriverManager")
    con=sc._jsc.hadoopConfiguration()
    fs=sc._jvm.FileSystem.get(con)
    root_dir=str(fs.getUri())+"/data/"
    inc_dir="t_cf_inc"
    #hist_dir="t_cf"
    #indoor_dir='indoor'
    #flow_dir='flow'
    incFolders=fs.listStatus(sc._jvm.Path(root_dir+inc_dir))
    #delete the inc folder if the folder do not contain any files,it's for the next turn's inc file processing (split file group by folder name)
    for x in incFolders:
        if x.isDirectory():
            incFolders=str(x.getPath())
            if int(fs.getContentSummary(sc._jvm.Path(incFolders)).getFileCount())==0:
                fs.delete(sc._jvm.Path(incFolders) ,True)
    if fs.exists(sc._jvm.Path("/data/t_cf_inc/transfer_completed.txt")):
        fs.delete(sc._jvm.Path("/data/t_cf_inc/transfer_completed.txt"))
    #hbase offline data processing ,could be the bottleneck in the future
    conn=sc._jvm.DriverManager.getConnection("jdbc:phoenix:namenode:2181:/hbase-unsecure")
    stmt = conn.createStatement()
    stmt.execute("delete from t_indoor where exists (select 1 from t_indoor_for_delete where t_indoor_for_delete.ENTITYID=t_indoor.ENTITYID and t_indoor_for_delete.CLIENTMAC=t_indoor.CLIENTMAC and t_indoor_for_delete.ETIME=t_indoor.ETIME)")
    stmt.execute("drop table if exists t_indoor_for_delete")
    stmt.execute("create table t_indoor_for_delete(ENTITYID INTEGER not null,CLIENTMAC CHAR(12) not null,ETIME integer not null CONSTRAINT pk PRIMARY KEY (ENTITYID,CLIENTMAC,ETIME))")
    stmt.close()
    conn.close()
            
    sc.stop()
    subprocess.call(['/opt/src/shell/hive.sh'])
    

 


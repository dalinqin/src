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

if __name__ == "__main__":
    progTag=int(sys.argv[1])   # number indicate the spark program
    sc = SparkContext(appName="incData"+str(progTag))
    splitNum=6 # 6 spark program running at same time

    def processData(sc,hc,fs,con,incFileName,inThresh,outThresh,progTag):
        #incFileName="hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100033/t_cf_20161028.txt"
        
        #inThresh=10
        #outThresh=300
        
        #**************************************
        #
        #this procedure will use incfile to caculate 
        #flow(everyday one record) store as file
        #indoor(everyday one record) store as file
        #indoor_for_delete(every indoor records) store in hbase
        #indoor detail(every indoor records) store in hbase and as file
        #
        #destIndoorFile     : /data/indoor/entityid/year/id_date.json      used to generate report
        #destFlowFile       : /data/flow/entityid/year/fl_date.json        used to generate report
        #rec_destIndoorfile : /data/rec_indoor/entityid/year/id_date.json  this folder is mirror of hbase records
        #
        #
        #**************************************

        destIndoorFile=get_str_indoorFileName(incFileName)
        #hdfs://namenode.navroomhdp.com:8020/data/indoor/100033/2016/id_20161028.txt
        rec_destIndoorfile=destIndoorFile.replace("/indoor/","/rec_indoor/")
        #hdfs://namenode.navroomhdp.com:8020/data/rec_indoor/101762/2016/id_20161011.txt
        destFlowFile  =destIndoorFile.replace("/indoor/","/flow/").replace("id_","fl_")
        #hdfs://namenode.navroomhdp.com:8020/data/flow/101762/2016/fl_20161011.txt
        tmp_destIndoorFolder = "hdfs://namenode.navroomhdp.com:8020/data/tmp/indoor"+str(progTag)
        tmp_destFlowFolder   = "hdfs://namenode.navroomhdp.com:8020/data/tmp/flow"+str(progTag)
        tmp_rec_destIndoorFolder   = "hdfs://namenode.navroomhdp.com:8020/data/tmp/rec_indoor"+str(progTag)
        EntityID=int(get_str_entityID(incFileName))
        #101762
        histFileName=get_str_histFileName(incFileName) #processed file will be place here
        #hdfs://namenode.navroomhdp.com:8020/data/t_cf/101762/t_cf_20161011.txt
        if fs.exists(sc._jvm.Path(histFileName)):
            tmpFileName=get_str_tmpFileName(histFileName)
            #tmpFileName = hdfs://namenode.navroomhdp.com:8020/data/tmp/101762/t_cf_20161011.txt
            tmpFolderName=tmpFileName.rsplit('/',1)[0]+"tmp"
            #tmpFolderName=hdfs://namenode.navroomhdp.com:8020/data/tmp/101762tmp
            #copy hist file to temp folder and name it as hdfs://namenode.navroomhdp.com:8020/data/tmp/101762tmp/hist and distroy the hist file
            sc._jvm.FileUtil.copy(fs,sc._jvm.Path(histFileName),fs,sc._jvm.Path(tmpFolderName+"/hist"),True,True,con) 
            #copy inc file to temp folder and name it as hdfs://namenode.navroomhdp.com:8020/data/tmp/101762tmp/inc and  destroy the inc file
            sc._jvm.FileUtil.copy(fs,sc._jvm.Path(incFileName),fs,sc._jvm.Path(tmpFolderName+"/inc"),True,True,con)
            #copymerge the 2 files (inc and hist) into one file 
            sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(tmpFolderName),fs,sc._jvm.Path(tmpFileName),True,con,None)
            sc._jvm.FileUtil.copy(fs,sc._jvm.Path(tmpFileName),fs,sc._jvm.Path(incFileName),True,True,con)

        unixFirtDayofMonth = get_int_firstDayUnixDate(incFileName)
        # firtDayofMonth= 1475251200 it is 20161001 unixdate
        startUnixTime=get_int_fileNameUnixDate(incFileName) #1456808400 this is today's unix datetime
 
        rows_t_cf=sc.textFile(incFileName).map(lambda r: r.split(",")).map(lambda p: Row(clientmac=p[0], stime=p[1],flag=p[2]))
        HiveContext.createDataFrame(hc,rows_t_cf).registerTempTable("t_cf_inc_tmp")
        hc.sql("select distinct clientmac,stime,flag from t_cf_inc_tmp").registerTempTable("t_cf")
        
        df=hc.sql("select distinct ClientMac,stime ,lag(stime) over (partition by ClientMac order by stime) as lag_time ,lead(stime) over (partition by ClientMac order by stime) as lead_time from t_cf where flag=1")
        df1=df.withColumn("diff" , df["stime"]-df["lag_time"]).na.fill(-1)
        df1.filter((df1.diff>=outThresh)|(df1.lag_time ==-1)|( df1.lead_time==-1)).registerTempTable("df2")
        df2=hc.sql("select ClientMac,stime,lag_time,lead_time,case when (diff < "+ str(outThresh) +" and diff>0) then diff ELSE 0 end as diff from df2")
        df3=df2.withColumn("lag_time1",df2.lag_time+df2.diff).drop( "lag_time")
        df3.withColumn("lag_time2",func.lead("lag_time1").over(Window.partitionBy("clientMac"))).registerTempTable("df3")
        
        df4=hc.sql("select ClientMac,cast(stime as int) as ETime ,cast(lag_time2 as int) as LTime,cast((lag_time2- stime) as int) as Seconds from df3").na.fill(-1)
        df5=df4.filter((df4.LTime>0)&(df4.Seconds>=inThresh)&(df4.ETime>startUnixTime)&(df4.ETime<(startUnixTime+86400))).withColumn("ENTITYID",lit(EntityID)) #86400 is seonds in one day
        df5.registerTempTable("df5")
        #DF5 will be save to hbase as indoor details(rec_destIndoorfile) ,df6 and df7 will be used for stats caculation
        
        df6=hc.sql("select ClientMac,ETime, LTime, Seconds ,unix_timestamp(date_sub(from_unixtime(etime),0),'yyyy-MM-dd') as utoday from df5")
        df6.registerTempTable("df6_indoor")
        df7=hc.sql("select ClientMac,min(etime) as etime,max(ltime) as ltime,sum(Seconds) as seconds,utoday from df6_indoor group by ClientMac,utoday")
        df_current_result=df7.withColumn("ENTITYID",lit(EntityID)).withColumn('UFIRSTDAY',lit(unixFirtDayofMonth))

        flow_sql=  "select ClientMac,min(stime) as etime,max(stime) as ltime from t_cf where stime >"+str(startUnixTime) + " and stime <"+str(startUnixTime+86400)+" group by clientmac"
        hc.sql(flow_sql).registerTempTable("df_flow_tmp")
        df_flow=hc.sql("select ClientMac,etime,ltime,unix_timestamp(date_sub(from_unixtime(etime),0),'yyyy-MM-dd') as utoday from df_flow_tmp").withColumn("ENTITYID",lit(EntityID)).withColumn('UFIRSTDAY',lit(unixFirtDayofMonth))
       
        #df_flow.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "T_FLOW_TODAY") .option("zkUrl", "namenode.navroomhdp.com:2181:/hbase-unsecure").save()
        #df_flow.saveAsTable("T_FLOW")
        if len(df5.head(1))==1:  #df5 is not empty better than df5.rdd.isEmpty
            tmp_rec_destIndoorFolder   = "hdfs://namenode.navroomhdp.com:8020/data/tmp/rec_indoor"+str(progTag)
            df5.select('clientmac','entityid','etime','ltime','seconds').write.mode('overwrite').format('com.databricks.spark.csv').options(header='false').save(tmp_rec_destIndoorFolder)            
            #df5.write.mode('overwrite').json(tmp_rec_destIndoorFolder)
            df5.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "T_INDOOR") .option("zkUrl", "namenode.navroomhdp.com:2181:/hbase-unsecure").save()
            if fs.exists(sc._jvm.Path(rec_destIndoorfile)):  #the old indoor folder exists,will generate df_delete_pk for phoenix to delete invalid rows
                rows_rec_indoor=sc.textFile(rec_destIndoorfile).map(lambda r: r.split(",")).map(lambda p: Row(clientmac=str(p[0]), entityid=int(p[1]),etime=int(p[2]),ltime=int(p[3]),seconds=int(p[4])))
                HiveContext.createDataFrame(hc,rows_rec_indoor).registerTempTable("df_old_indoor")
                df_old_indoor_pk=hc.sql("select ClientMac,ENTITYID,ETime from df_old_indoor") 
                df_current_result_pk=hc.sql("select ClientMac,ENTITYID,ETime from df5") 
                df_delete_pk = df_old_indoor_pk.subtract(df_current_result_pk)
                if len(df_delete_pk.head(1))==1:
                    df_delete_pk.write.format("org.apache.phoenix.spark").mode("overwrite").option("table", "T_INDOOR_FOR_DELETE").option("zkUrl", "namenode.navroomhdp.com:2181:/hbase-unsecure").save()
        else:
            tmp_rec_destIndoorFolder="NONE"
            
        if len(df_flow.head(1))==1:
            tmp_destFlowFolder   = "hdfs://namenode.navroomhdp.com:8020/data/tmp/flow"+str(progTag)
            df_flow.select('clientmac','entityid','etime','ltime','utoday','ufirstday').write.mode('overwrite').format('com.databricks.spark.csv').options(header='false').save(tmp_destFlowFolder)
            #df_flow.write.mode('overwrite').json(tmp_destFlowFolder)
        else:
            tmp_destFlowFolder="NONE"
            
        if len(df_current_result.head(1))==1:
            tmp_destIndoorFolder = "hdfs://namenode.navroomhdp.com:8020/data/tmp/indoor"+str(progTag)
            df_current_result.select('clientmac','entityid','etime','ltime','seconds','utoday','ufirstday').write.mode('overwrite').format('com.databricks.spark.csv').options(header='false').save(tmp_destIndoorFolder)
            #df_current_result.write.mode('overwrite').json(tmp_destIndoorFolder)
        else:
            tmp_destIndoorFolder="NONE"
        
        sc._jvm.FileUtil.copy(fs,sc._jvm.Path(incFileName),fs,sc._jvm.Path(histFileName),True,True,con) 

        if fs.exists(sc._jvm.Path(destIndoorFile)):
            fs.delete(sc._jvm.Path(destIndoorFile))
        if fs.exists(sc._jvm.Path(destFlowFile)):
            fs.delete(sc._jvm.Path(destFlowFile))
        if fs.exists(sc._jvm.Path(rec_destIndoorfile)):
            fs.delete(sc._jvm.Path(rec_destIndoorfile))        
        #delete is a must if file already exists otherwise copymerge will fail  
        
        if tmp_destIndoorFolder!="NONE":
            sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(tmp_destIndoorFolder),fs,sc._jvm.Path(destIndoorFile),True,con,None)
            #destIndoorFile=get_str_indoorFileName(incFileName)
        if tmp_destFlowFolder!="NONE":
            sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(tmp_destFlowFolder),fs,sc._jvm.Path(destFlowFile),True,con,None)
        if tmp_rec_destIndoorFolder!="NONE":
            sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(tmp_rec_destIndoorFolder),fs,sc._jvm.Path(rec_destIndoorfile),True,con,None)
        #copy merge all the spark small files into one file
    def get_str_histFileName(fileName):
        #fileName = hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/t_cf/'+splitFileName[2]+'/'+splitFileName[3]
        else:
            return "error"
    def get_str_incFileName(fileName):
        #fileName = hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/t_cf_inc/'+splitFileName[2]+'/'+splitFileName[3]
        else:
            return "error"
    
           
    def get_str_indoorFileName(fileName):
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        #return hdfs://namenode.navroomhdp.com:8020/data/indoor/100141/id_20160514.json
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/indoor/'+splitFileName[2]+'/'+splitFileName[3].rsplit('_',1)[1].rsplit('.',)[0][:4]+'/id_'+splitFileName[3].rsplit('_',1)[1].rsplit('.',)[0]+'.txt'
        else:
            return "error"
            
    def get_str_shortFileName(fileName):
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        splitFileName=str(fileName).rsplit('/',1)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==2:
            return splitFileName[1]
        else:
            return "error"
    
    def get_str_tmpFileName(fileName):
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        splitFileName=fileName.rsplit('/',3)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/tmp/'+splitFileName[2]+'/'+splitFileName[3]
            #hdfs://namenode.navroomhdp.com:8020/data/tmp/100141/t_cf_20160514.txt
        else:
            return "error"
            

    def get_str_fileNameDate(fileName): #20160514
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        splitFileName=str(fileName).rsplit('_',1)[1].split(".")[0]
        return splitFileName
    
    def get_int_fileNameUnixDate(fileName): #1456808400
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        #fileName=  "hdfs://namenode.navroomhdp.com:8020/data/indoor/100141/20160514"
        if len(str(fileName).rsplit('_',1)) >1:
            splitFileName=str(fileName).rsplit('_',1)[1].split(".")[0]
        else:
            splitFileName=str(fileName).rsplit('/',1)[1]
        return int(datetime.strptime(splitFileName, '%Y%m%d').strftime('%s'))
    
    def con_unixToDate(unixTime): 
        return datetime.fromtimestamp(int(unixTime)).strftime('%Y-%m-%d')  #strftime('%Y-%m-%d %H:%M:%S')

    def con_DateToUnix(strDate): #'2016-03-01' 
        l_strDate=strDate.split('-')
        start = date(int(l_strDate[0]), int(l_strDate[1]), int(l_strDate[2]))
        return int(time.mktime(start.timetuple()))
   
    def get_int_firstDayUnixDate(fileName):
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        #return 1462075200 it is 20160501 unix time
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.navroomhdp.com:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            firstDay=splitFileName[3].rsplit('.',1)[0].rsplit('_',1)[-1][:6]+'01'
            return int(datetime.strptime(firstDay, '%Y%m%d').strftime('%s'))
        else:
            return "error"
    
    def get_str_entityID(fileName): #100142
        #fileName = "hdfs://namenode.navroomhdp.com:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        #fileName=  "hdfs://namenode.navroomhdp.com:8020/data/indoor/100141/20160514"
        splitFileName=str(fileName).rsplit('/',2)[1]
        return splitFileName
        
    def get_str_entityID_byFolder(FolderName): #101561
        #FolderName = "hdfs://namenode.localdomain:8020/data/indoor/101561"
        splitFileName=str(FolderName).rsplit('/',1)[1]
        return splitFileName
        
    #root_dir=str(fs.getUri())+"/data/"
    #con=sc._jsc.hadoopConfiguration()
    #fs=sc._jvm.FileSystem.get(con)
    #pre_process_offline_files(sc,fs,root_dir)
    #def pre_process_offline_files(sc,fs,root_dir):
    #    '''go through all the files in offline folder
    #       merge inc file ,offline file and hist file altogether to inc file
    #       after this fuction offline folder will be empty , hist file previously exists will be removed'''
    #    try:
    #        cur_dir="offline"
    #        curFolders=fs.listStatus(sc._jvm.Path(root_dir+cur_dir))
    #        for x in curFolders:
    #            if x.isDirectory():
    #                strCurFolder=str(x.getPath())
    #                foldersInCurFolders=fs.listStatus(sc._jvm.Path(strCurFolder))
    #                for Files in foldersInCurFolders:
    #                    if Files.isFile():
    #                        offlineFileName=Files.getPath()
    #                        histFileName=get_str_histFileName(offlineFileName)
    #                        #hdfs://namenode.navroomhdp.com:8020/data/t_cf/100142/t_cf_20160303.txt
    #                        #OFFLINE_FILE_LIST.append(str(offlineFileName))
    #                        incFileName=get_str_incFileName(offlineFileName)
    #                        tmpFileName=get_str_tmpFileName(offlineFileName)
    #                        #hdfs://namenode.navroomhdp.com:8020/data/tmp/t_cf_20160514.txt
    #                        if histFileName !='error' and incFileName !='error' and tmpFileName!='error' :
    #                            p_histFile    = sc._jvm.Path(str(histFileName))    #hist file path
    #                            p_incFile     = sc._jvm.Path(str(incFileName))     #inc file path
    #                            p_offlineFile = sc._jvm.Path(str(offlineFileName)) #offline file path
    #                            if fs.exists(p_histFile) or fs.exists(p_incFile):  #in this case merge file is needed
    #                                if fs.exists(p_histFile):
    #                                    sc._jvm.FileUtil.copy(fs,p_histFile,fs,sc._jvm.Path(tmpFileName+"hist"),True,True,con) 
    #                                    #if hist exists, copy it to temp file hdfs://namenode.navroomhdp.com:8020/data/tmp/t_cf_20160514.txthist
    #                                    #don't keep old and overwrite dest
    #                                if fs.exists(p_incFile):
    #                                    sc._jvm.FileUtil.copy(fs,p_incFile,fs,sc._jvm.Path(tmpFileName+"inc"),True,True,con) 
    #                                    #if inc file exists, copy it to temp file hdfs://namenode.navroomhdp.com:8020/data/tmp/t_cf_20160514.txthinc
    #                                sc._jvm.FileUtil.copy(fs,Files.getPath(),fs,sc._jvm.Path(tmpFileName+"offline"),True,True,con) 
    #                                    #copy current offline file to the tmp folder hdfs://namenode.navroomhdp.com:8020/data/tmp/t_cf_20160514.txthoffline
    #                                sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(root_dir +"tmp"),fs,p_incFile,True,con,None)
    #                            else: # no need to merge directly copy offline file to inc folder
    #                                sc._jvm.FileUtil.copy(fs,p_offlineFile,fs,p_incFile,True,True,con)
    #    except:
    #        return False 
    #    else:
    #        return True
    #        
    #def get_process_file_list(sc,fs,fileName):
    #    '''after executing pre_process_offline_files, all files need to be processed in inc folder now 
    #       this function is going to get the file list (pre current and after) and pass it to processCFData function
    #       only return non-offline files
    #    '''
    #    fileList=fileName
    #    splitFileName=fileName.rsplit('/',1)
    #    #['hdfs://namenode.localdomain:8020/data/t_cf_inc/101317', 't_cf_20160604.txt']
    #    dateFileName=splitFileName[1].rsplit('_',1)[1].split('.',1)[0]
    #    #20160518 current processing file's date
    #    p_inc_preDateFileName    = sc._jvm.Path( splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')- timedelta(days=1)).strftime('%Y%m%d') + ".txt" )
    #    #hdfs://namenode.localdomain:8020/data/t_cf_inc/101317/t_cf_20160603.txt
    #    p_inc_afterDateFileName  = sc._jvm.Path( splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')+ timedelta(days=1)).strftime('%Y%m%d') + ".txt" )
    #    #hdfs://namenode.localdomain:8020/data/t_cf_inc/101317/t_cf_20160605.txt
    #    p_hist_preDateFileName   = sc._jvm.Path(get_str_histFileName(splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')- timedelta(days=1)).strftime('%Y%m%d') + ".txt"))
    #    #hdfs://namenode.localdomain:8020/data/t_cf/101317/t_cf_20160603.txt
    #    p_hist_afterDateFileName = sc._jvm.Path(get_str_histFileName(splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')+ timedelta(days=1)).strftime('%Y%m%d') + ".txt"))       
    #    #hdfs://namenode.localdomain:8020/data/t_cf/101317/t_cf_20160605.txt
    #    
    #    #check whether inc files exists
    #    
    #    if fs.exists(p_hist_preDateFileName): #if predate history file exists,add it to filelist
    #        fileList=fileList+','+str(p_hist_preDateFileName)
    #    if fs.exists(p_inc_afterDateFileName): #if inc afterdate file exists ,add it to filelist
    #        fileList=fileList+','+str(p_inc_afterDateFileName)
    #    elif fs.exists(p_hist_afterDateFileName): #if inc file not exist, find hist file as 
    #        fileList=fileList+','+str(p_hist_afterDateFileName)
    #    return fileList
    #

    def chunks(l, n):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(l), n):
                yield l[i:i + n]    
    '''------------ all function definition are here end -------------'''


    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc._jsc.hadoopConfiguration().set("parquet.enable.summary-metadata", "false")
    
    java_import(sc._jvm,"org.apache.hadoop.fs.FileUtil")
    java_import(sc._jvm,"org.apache.hadoop.fs.FileSystem")
    java_import(sc._jvm,"org.apache.hadoop.fs.Path")
    java_import(sc._jvm,"java.sql.DriverManager")
    con=sc._jsc.hadoopConfiguration()
    fs=sc._jvm.FileSystem.get(con)
    root_dir=str(fs.getUri())+"/data/"

    hc=HiveContext(sc)
    #if pre_process_offline_files(sc,fs,root_dir)!=True:
    #    println("offline folder has error , please check and rerun") 
    #    quit()
    
    inc_dir="t_cf_inc"
    hist_dir="t_cf"
    indoor_dir='indoor'
    flow_dir='flow'
    
    #hbase offline data processing ,could be the bottleneck in the future
    #conn=sc._jvm.DriverManager.getConnection("jdbc:phoenix:namenode:2181:/hbase-unsecure")
    #stmt = conn.createStatement()
    #stmt.execute("delete from t_indoor where exists (select 1 from t_indoor_for_delete where t_indoor_for_delete.ENTITYID=t_indoor.ENTITYID and t_indoor_for_delete.CLIENTMAC=t_indoor.CLIENTMAC and t_indoor_for_delete.ETIME=t_indoor.ETIME)")
    #stmt.execute("drop table if exists t_indoor_for_delete")
    #stmt.execute("create table t_indoor_for_delete(ENTITYID INTEGER not null,CLIENTMAC CHAR(12) not null,ETIME integer not null CONSTRAINT pk PRIMARY KEY (ENTITYID,CLIENTMAC,ETIME))")
    #stmt.close()
    #conn.close()

    ######fetch in/out thresh from mysql 
    props = { "user": "dbreader", "password": "HelloN@vr00m" }
    hc=HiveContext(sc)
    df=hc.read.jdbc(url="jdbc:mysql://121.40.48.169:8301/maindb", table="tentity", properties=props)
    
    df_mysql=df.select(df.ID,df.IndoorSecondsThrehold,df.LeaveMinutesThrehold)
    df_mysql.cache()

    pd_column=['operType','processDate','fileName']
    pd_dataFrameLog=pandas.DataFrame(columns=pd_column)
    operType="0-ProcessincFile"

    #folders split evenly into 8 folders in memory and each spark job handle one list
    incFolders=fs.listStatus(sc._jvm.Path(root_dir+inc_dir))
    #splitFolders 
    splitFolders=[]
    
    for x in incFolders:
        if x.isDirectory():
            incFolders=str(x.getPath())
            splitFolders.append(incFolders)    

    numPerGroup=len(splitFolders)/splitNum+1

    folderLists=[]
    cnt=1
    for x in chunks(splitFolders,numPerGroup):
        if progTag==cnt:
            folderLists=x
        if progTag==cnt:
            folderLists=x
        if progTag==cnt:
            folderLists=x
        if progTag==cnt:
            folderLists=x
        if progTag==cnt:
            folderLists=x
        if progTag==cnt:
            folderLists=x 
        if progTag==cnt:                
            folderLists=x         
        if progTag==cnt:                
            folderLists=x         
        cnt=cnt+1
        
    for x in folderLists:
        entityid=int(get_str_entityID_byFolder(x))
        df_id=df_mysql.filter(df_mysql.ID==entityid)
        inThresh  =10  if (df_id.head(1)[0].IndoorSecondsThrehold==0) else df_id.head(1)[0].IndoorSecondsThrehold
        outThresh =300 if (df_id.head(1)[0].LeaveMinutesThrehold ==0) else df_id.head(1)[0].LeaveMinutesThrehold*60
        incFiles=fs.listStatus(sc._jvm.Path(x))
        for incFile in incFiles:
            if incFile.isFile():
                curTime=str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())))
                currrentFile=str(incFile.getPath())
                processData(sc,hc,fs,con,currrentFile,inThresh,outThresh,progTag)
                pd_rowLog = pandas.Series([operType,curTime,currrentFile],index=pd_column)
                pd_dataFrameLog=pd_dataFrameLog.append(pd_rowLog, ignore_index=True)
    
    
    curDay=str(time.strftime("%Y-%m-%d",time.localtime(time.time())))

    
    df_log=hc.createDataFrame(pd_dataFrameLog)
    df_log.sort(df_log.operType,df_log.processDate).repartition(1).write.mode('overwrite').json("/data/log/incData"+str(progTag)+"/"+curDay) 
 
        
    sc.stop()
    

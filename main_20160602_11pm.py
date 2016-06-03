
import sys
from pyspark import SparkContext
from py4j.java_gateway import java_import
from pyspark.sql import HiveContext, Row
import pandas
from datetime import datetime,timedelta
from pyspark.sql.functions import current_date, datediff, unix_timestamp
import time
import json


if __name__ == "__main__":
	
    def processCFData(sc,hc,filePath,destFolder,startUnixTime,inThresh,outThresh,turnOff):
        rows=sc.textFile(filePath).map(lambda r: r.split(",")).map(lambda p: Row(ClientMac=p[0], stime=p[1],flag=p[2]))
        t_cf_inc=HiveContext.createDataFrame(hc,rows)
        t_cf_inc.registerTempTable("t_cf_inc_tmp2")
        df_tmp=hc.sql("select distinct ClientMac,cast(stime as bigint) as stime ,cast(flag as int) as flag from t_cf_inc_tmp2").registerTempTable("t_cf")
        df=hc.sql("select distinct ClientMac,stime ,lag(stime) over (partition by ClientMac order by stime) as lag_time ,lead(stime) over (partition by ClientMac order by stime) as lead_time from t_cf where flag=1")
        df1=df.withColumn("diff" , df["stime"]-df["lag_time"]).na.fill(-1)
        df2=df1.filter((df1.diff>=outThresh)|(df1.lag_time ==-1)|( df1.lead_time==-1))
        dd=df2.toPandas()
        dd["diff"]=dd["diff"].map(lambda x : x if  x<outThresh and x>0 else 0 )
        dd["lag_time"]=dd["lag_time"]+dd["diff"]
        dd.lag_time=dd.lag_time.shift(-1)
        df3=hc.createDataFrame(dd)
        df3.registerTempTable("df3")
        df4=hc.sql("select ClientMac,stime as ETime ,cast(lag_time as bigint) as LTime,cast((lag_time- stime) as bigint) as Seconds from df3")
        if turnOff:
            df5=df4.filter((df4.LTime>0)&(df4.LTime!='NaN')&(df4.Seconds>=inThresh)&(df4.ETime>startUnixTime)&(df4.ETime<(startUnixTime+86400))) #86400 is seonds in one day
        else:
            df5=df4.filter((df4.LTime>0)&(df4.LTime!='NaN')&(df4.Seconds>=inThresh))  
        df5.repartition(1).write.mode('overwrite').json(destFolder)
        #host = 'namenode.localdomain'
        #table = 'indoor'
        #keyConv = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
        #valueConv = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"
        #conf = {"hbase.zookeeper.quorum": host,
        #    "hbase.mapred.outputtable": table,
        #    "hbase.zookeeper.property.clientPort": "2181",
        #    "zookeeper.znode.parent": "/hbase-unsecure",
        #    "mapreduce.outputformat.class": "org.apache.hadoop.hbase.mapreduce.TableOutputFormat",
        #    "mapreduce.job.output.key.class": "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        #    "mapreduce.job.output.value.class": "org.apache.hadoop.io.Writable"}
        #df6=hc.sql("select clientmac,cast(stime as string) as stime,cast(diff as string) as diff from df5")
        #df7=df6.rdd.map(lambda x: (x.clientmac+str(x.stime),[str(x.clientmac+str(x.stime)),"cf","seconds",str(x.diff)]))
        #df7.saveAsNewAPIHadoopDataset(conf=conf,keyConverter=keyConv,valueConverter=valueConv)
        
    def get_str_histFileName(fileName):
        #fileName = hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160514.txt
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.localdomain:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/t_cf/'+splitFileName[2]+'/'+splitFileName[3]
        else:
            return "error"
    def get_str_incFileName(fileName):
        #fileName = hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160514.txt
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.localdomain:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/t_cf_inc/'+splitFileName[2]+'/'+splitFileName[3]
        else:
            return "error"
            
    def get_str_indoorFileName(fileName):
        #fileName = hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160514.txt
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.localdomain:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/indoor/'+splitFileName[2]+'/'+splitFileName[3]
        else:
            return "error"
            
    def get_str_shortFileName(fileName):
        #fileName = "hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        splitFileName=str(fileName).rsplit('/',1)
        #['hdfs://namenode.localdomain:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==2:
            return splitFileName[1]
        else:
            return "error"
    
    def get_str_tmpFileName(fileName):
        #fileName = "hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160514.txt"
        splitFileName=str(fileName).rsplit('/',3)
        #['hdfs://namenode.localdomain:8020/data','t_cf_inc', '100141', 't_cf_20160514.txt']
        if len(splitFileName)==4:
            return splitFileName[0]+'/tmp/'+'/'+splitFileName[3]
        else:
            return "error"


    
    def pre_process_offline_files(sc,fs,root_dir):
        try:
            cur_dir="offline"
            curFolders=fs.listStatus(sc._jvm.Path(root_dir+cur_dir))
            for x in curFolders:
                if x.isDirectory():
                    strCurFolder=str(x.getPath())
                    foldersInCurFolders=fs.listStatus(sc._jvm.Path(strCurFolder))
                    for Files in foldersInCurFolders:
                        if Files.isFile():
                            offlineFileName=Files.getPath()
                            histFileName=get_str_histFileName(offlineFileName)
                            #hdfs://hdp.localdomain:8020/data/t_cf/100142/t_cf_20160303.txt
                            incFileName=get_str_incFileName(offlineFileName)
                            tmpFileName=get_str_tmpFileName(offlineFileName)
                            if histFileName !='error' and incFileName !='error' and tmpFileName!='error' :
                                p_histFile    = sc._jvm.Path(str(histFileName))    #hist file path
                                p_incFile     = sc._jvm.Path(str(incFileName))     #inc file path
                                p_offlineFile = sc._jvm.Path(str(offlineFileName)) #offline file path
                                if fs.exists(p_histFile) or fs.exists(p_incFile):  #in this case merge file is needed
                                    if fs.exists(p_histFile):
                                        sc._jvm.FileUtil.copy(fs,p_histFile,fs,sc._jvm.Path(tmpFileName+"hist"),True,True,con) 
                                        #don't keep old and overwrite dest
                                    if fs.exists(p_incFile):
                                        sc._jvm.FileUtil.copy(fs,p_incFile,fs,sc._jvm.Path(tmpFileName+"inc"),True,True,con) 
                                    sc._jvm.FileUtil.copy(fs,Files.getPath(),fs,sc._jvm.Path(tmpFileName+"offline"),True,True,con) 
                                    sc._jvm.FileUtil.copyMerge(fs, sc._jvm.Path(root_dir +"tmp"),fs,p_incFile,True,con,None)
                                else: # no need to merge directly copy offline file to inc folder
                                    sc._jvm.FileUtil.copy(fs,p_offlineFile,fs,p_incFile,True,True,con)
        except:
            return False 
        else:
            return True

    def get_process_file_list(sc,fs,fileName):
        #fileName = "hdfs://namenode.localdomain:8020/data/t_cf_inc/100142/t_cf_20150513.txt"
        #inc file as the parameter 
        fileList=fileName
        splitFileName=fileName.rsplit('/',1)
        #['hdfs://namenode.localdomain:8020/data/t_cf_inc/100141', 't_cf_20160514.txt']
        dateFileName=splitFileName[1].rsplit('_',1)[1].split('.',1)[0]
        #20160518 current processing file's date
        p_inc_preDateFileName    = sc._jvm.Path( splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')- timedelta(days=1)).strftime('%Y%m%d') + ".txt" )
        #20160517  hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160517.txt 
        p_inc_afterDateFileName  = sc._jvm.Path( splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')+ timedelta(days=1)).strftime('%Y%m%d') + ".txt" )
        #20160519  hdfs://namenode.localdomain:8020/data/t_cf_inc/100141/t_cf_20160519.txt 
        p_hist_preDateFileName   = sc._jvm.Path(get_str_histFileName(splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')- timedelta(days=1)).strftime('%Y%m%d') + ".txt"))
        p_hist_afterDateFileName = sc._jvm.Path(get_str_histFileName(splitFileName[0] + "/t_cf_"+ (datetime.strptime(dateFileName, '%Y%m%d')+ timedelta(days=1)).strftime('%Y%m%d') + ".txt"))       
        
        if fs.exists(p_inc_preDateFileName): #if predate file exists no need to check hist file 
            fileList=fileList+','+str(p_inc_preDateFileName)
        elif fs.exists(p_hist_preDateFileName): #if perdte file not exists,check hist file 
            fileList=fileList+','+str(p_hist_preDateFileName)
            
        if fs.exists(p_inc_afterDateFileName):
            fileList=fileList+','+str(p_inc_afterDateFileName)
        elif fs.exists(p_hist_afterDateFileName):
            fileList=fileList+','+str(p_hist_afterDateFileName)
        return fileList


    sc = SparkContext(appName="test")
    java_import(sc._jvm,"org.apache.hadoop.fs.FileUtil")
    java_import(sc._jvm,"org.apache.hadoop.fs.FileSystem")
    java_import(sc._jvm,"org.apache.hadoop.fs.Path")
    con=sc._jsc.hadoopConfiguration()
    fs=sc._jvm.FileSystem.get(con)
    root_dir=str(fs.getUri())+"/data/"
    
    if pre_process_offline_files(sc,fs,root_dir):
    
    
    inc_dir="t_cf_inc"
    hist_dir="t_cf"
    indoor_dir='indoor'
    incFolders=fs.listStatus(sc._jvm.Path(root_dir+inc_dir))
    hc=HiveContext(sc)
    pd_column=['processDate','fileName']
    pd_dataFrameLog=pandas.DataFrame(columns=pd_column)
    for x in incFolders:
        if x.isDirectory():
            incFolder_iteration=str(x.getPath())
            foldersInIncFolders=fs.listStatus(sc._jvm.Path(incFolder_iteration))
            for incFile in foldersInIncFolders:
                if incFile.isFile():
                    FILE_LIST=get_process_file_list(sc,fs,incFile.getPath())
                    dateFileNam_unix=datetime.strptime(dateFileName, '%Y%m%d').strftime('%s')
                    #1463500800 translate date to unix timestamp
                    destIndoorFolder=root_dir+indoor_dir+'/'+parsedPathForIndoor[1]+'/'+ dateFileName  #current final indoor path folder
                    # root_dir=hdfs://namenode.localdomain:8020/data/ indoor_dir=indoor parsedPathForIndoor[1]=100141 dateFileName=20160518
                    #hdfs://namenode.localdomain:8020/data/indoor/100141/20160518
                    histFileName= get_str_histFileName(incFile.getPath())
                    #current hist File name, caculation will based on this file and one day's before this file and oneday's after this file if exists
                    #hdfs://namenode.localdomain:8020/data/t_cf/100141/t_cf_20160514.txt
                    p_histFile=sc._jvm.Path(str(histFile))
                    p_incFile=incFile.getPath()
                    if fs.exists(histFile): 
                        #rename the hist_file_name to hist_file_name_tmp
                        #copy the inc file to hist folder and rename to hist
                        #concat hist_file_name_tmp and hist_file_name_tmp_inc to new file hist_file_name
                        #copy the hist_file_name back to inc folder  --no need
                        p_histFile_tmp=sc._jvm.Path(str(histFile)+"_tmp")
                        fs.rename(p_histFile,p_histFile_tmp)
                        p_incFileCopyDest=sc._jvm.Path(str(p_histFile_tmp)+"_inc")
                        sc._jvm.FileUtil.copy(fs,p_incFile,fs,p_incFileCopyDest,True,False,con)
                        fs.createNewFile(p_histFile)
                        #create path array to do the concat operation
                        path_class=sc._gateway.jvm.Path
                        path_array = sc._gateway.new_array(path_class,2)
                        path_array[0] = p_incFileCopyDest
                        path_array[1] = p_histFile_tmp
                        #fs.createNewFile(p_histFile)
                        #fs.closeAll()
                        #con=sc._jsc.hadoopConfiguration()
                        #fs=sc._jvm.FileSystem.get(con)
                        fs.concat(p_histFile,path_array)
                        #sc._jvm.FileUtil.copy(fs,p_histFile,fs,p_incFile,False,True,con) #False keep the orginal file True overwrtie dest file
                    else: #if not exists means pure incremental data, copy the file to hist folder and start to process data
                        #sc._jvm.FileUtil.replaceFile(p_incFile,p_histFile)
                        sc._jvm.FileUtil.copy(fs,p_incFile,fs,p_histFile,True,True,con)
                    #so far Histfile has been merged with the inc file if offlinefile exists and we are ready to process today and yesterday's data all together
                    preHistFile=str(histFile).replace(dateFileName,datePreFileName)
                    #hdfs://namenode.localdomain:8020/data/t_cf/100141/t_cf_20160513.txt
                    filesToBeProcessed=str(histFile) #current histfile should be processed
                    ##filesToBeProcessed.append(histFile)
                    curTime=str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())))
                    pd_rowLog = pandas.Series([curTime,filesToBeProcessed],index=pd_column)
                    pd_dataFrameLog=pd_dataFrameLog.append(pd_rowLog, ignore_index=True)
                    if fs.exists(sc._jvm.Path(preHistFile)):
                         filesToBeProcessed=filesToBeProcessed + ',' + preHistFile    #if previous day's data exists , should process it either
                         pd_rowLog = pandas.Series([curTime,str(preHistFile)],index=pd_column)
                         pd_dataFrameLog=pd_dataFrameLog.append(pd_rowLog, ignore_index=True)
                    df3=hc.createDataFrame(pd_dataFrameLog)
                    df3.repartition(1).write.mode('overwrite').json('/data_tmp/log/')
                    processCFData(sc,hc, filesToBeProcessed,destIndoorFolder,int(dateFileNam_unix),10,300,True)
    sc.stop()


/usr/hdp/2.4.0.0-169/spark/examples/src/main/python


import json
host = '172.23.18.139'
table = 'test_hbase_table'
conf = {"hbase.zookeeper.quorum": host, "zookeeper.znode.parent": "/hbase-unsecure", "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = sc.newAPIHadoopRDD(
        "org.apache.hadoop.hbase.mapreduce.TableInputFormat",
        "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
        "org.apache.hadoop.hbase.client.Result",
        keyConverter=keyConv,
        valueConverter=valueConv,
        conf=conf)
hbase_rdd1 = hbase_rdd.flatMapValues(lambda v: v.split("\n"))

tt=sqlContext.jsonRDD(hbase_rdd1.values())



'''
spark.driver.extraClassPath
spark.executor.extraClassPath

export CLASSPATH=/usr/hdp/2.4.0.0-169/spark/lib/spark-examples-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop-common-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-annotations.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-client.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-common.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop-compat.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop2-compat.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-it.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-prefix-tree.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-procedure.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-protocol.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-resource-bundle.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-rest.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-server.jar:/usr/hdp/2.4.0.0-169/hbase/lib/guava-12.0.1.jar:/usr/hdp/2.4.0.0-169/hadoop/hadoop-common-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop-hdfs/hadoop-hdfs-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop/mysql-connector-java-5.1.38-bin.jar:.

/usr/hdp/2.4.0.0-169/spark/lib/spark-assembly-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop-common-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-annotations.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-client.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-common.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop-compat.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop2-compat.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-it.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-prefix-tree.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-procedure.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-protocol.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-resource-bundle.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-rest.jar:/usr/hdp/2.4.0.0-169/hbase/lib/hbase-server.jar:/usr/hdp/2.4.0.0-169/hbase/lib/guava-12.0.1.jar:/usr/hdp/2.4.0.0-169/hadoop/hadoop-common-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop-hdfs/hadoop-hdfs-2.7.1.2.4.0.0-169.jar:/usr/hdp/2.4.0.0-169/hadoop/mysql-connector-java-5.1.38-bin.jar

cp  /usr/hdp/2.4.0.0-169/spark/lib/spark-assembly-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar /root/jars
cp  /usr/hdp/2.4.0.0-169/hadoop-common-2.7.1.2.4.0.0-169.jar              /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-annotations.jar                  /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-client.jar                       /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-common.jar                       /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop-compat.jar                /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-hadoop2-compat.jar               /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-it.jar                           /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-prefix-tree.jar                  /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-procedure.jar                    /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-protocol.jar                     /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-resource-bundle.jar              /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-rest.jar                         /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/hbase-server.jar                       /root/jars
cp  /usr/hdp/2.4.0.0-169/hbase/lib/guava-12.0.1.jar                       /root/jars
cp  /usr/hdp/2.4.0.0-169/hadoop/hadoop-common-2.7.1.2.4.0.0-169.jar       /root/jars
cp  /usr/hdp/2.4.0.0-169/hadoop-hdfs/hadoop-hdfs-2.7.1.2.4.0.0-169.jar    /root/jars
cp  /usr/hdp/2.4.0.0-169/hadoop/mysql-connector-java-5.1.38-bin.jar       /root/jars
cp  /usr/hdp/2.4.0.0-169/spark/lib/spark-examples-1.6.0.2.4.0.0-169-hadoop2.7.1.2.4.0.0-169.jar /root/jars


import datetime
datetime.datetime.fromtimestamp(1463419577).strftime('%Y-%m-%d')  %H:%M:%S


props = { "user": "test", "password": "test"  }
hc=HiveContext(sc)
df=hc.read.json("/data_test/aa")
df.write.jdbc(url="jdbc:mysql://10.161.212.92:3306/test", table="indoor", mode="append", properties=props)

org.apache.hadoop.hbase.client.Admin
org.apache.hadoop.hbase.client.Operation
java_import(sc._jvm,"org.apache.hadoop.hbase.client.*")

java_import(sc._jvm,"org.apache.hadoop.hbase.HBaseConfiguration")
                     org.apache.hadoop.hbase.client.
org,apache,hadoop,hbase,HRegionInfo
java_import(sc._jvm,"org.apache.hadoop.hbase.HRegionInfo")    


from py4j.java_gateway import java_import, JavaGateway, JavaObject
from py4j.protocol import DEFAULT_JVM_ID

def get_imports(gateway):
    java_jvm_view = JavaObject(DEFAULT_JVM_ID[1:], gateway._gateway_client)
    imports = list(java_jvm_view.getSingleImportsMap().values()) + list(java_jvm_view.getStarImports())
    return imports
    
'''    



test:

indoor_df=processCFData(hc,"/data/t_cf_inc/100141/t_cf_20151229.txt",0,10,300,False)
def processCFData(hc,filePath,startUnixTime,inThresh,outThresh,turnOff):
    rows=sc.textFile(filePath).map(lambda r: r.split(",")).map(lambda p: Row(ClientMac=p[0], stime=p[1],flag=p[2]))
    t_cf_inc=HiveContext.createDataFrame(hc,rows)
    t_cf_inc.registerTempTable("t_cf_inc_tmp2")
    df_tmp=hc.sql("select distinct ClientMac,cast(stime as bigint) as stime ,cast(flag as int) as flag from t_cf_inc_tmp2").registerTempTable("t_cf")
    df=hc.sql("select distinct ClientMac,stime ,lag(stime) over (partition by ClientMac order by stime) as lag_time ,lead(stime) over (partition by ClientMac order by stime) as lead_time from t_cf where flag=1")
    df1=df.withColumn("diff" , df["stime"]-df["lag_time"]).na.fill(-1)
    df2=df1.filter((df1.diff>=outThresh)|(df1.lag_time ==-1)|( df1.lead_time==-1))
    dd=df2.toPandas()
    dd["diff"]=dd["diff"].map(lambda x : x if  x<outThresh and x>0 else 0 )
    dd["lag_time"]=dd["lag_time"]+dd["diff"]
    dd.lag_time=dd.lag_time.shift(-1)
    df3=hc.createDataFrame(dd)
    df3.registerTempTable("df3")
    df4=hc.sql("select ClientMac,stime as ETime ,cast(lag_time as bigint) as LTime,cast((lag_time- stime) as bigint) as Seconds from df3")
    if turnOff:
        df5=df4.filter((df4.LTime>0)&(df4.LTime!='NaN')&(df4.Seconds>=inThresh)&(df4.ETime>startUnixTime)&(df4.ETime<(startUnixTime+86400))) #86400 is seonds in one day
    else:
        df5=df4.filter((df4.LTime>0)&(df4.LTime!='NaN')&(df4.Seconds>=inThresh))  
    return df5
    
indoor_df.registerTempTable("indoor_tab")


In [61]: df[df.ClientMac.isin("000822804AFE")].collect()
Out[61]:
[Row(ClientMac=u'000822804AFE', ETime=1451460067, LTime=1451463244, Seconds=3177),
 Row(ClientMac=u'000822804AFE', ETime=1451473244, LTime=1451473264, Seconds=20),
 Row(ClientMac=u'000822804AFE', ETime=1451473564, LTime=1451473599, Seconds=35)]

In [62]: type(isin)
Out[62]: list

In [63]: isin[0]
Out[63]: Row(ClientMac=u'000822804AFE', ETime=1451460067, LTime=1451463244, Seconds=3177)

In [64]: tt=isin[0]

In [65]: tt.
tt.asDict  tt.count   tt.index

In [65]: tt.ClientMac
Out[65]: u'000822804AFE'

In [66]:  isin=df[df.ClientMac.isin("000822804AFd")].collect()

In [67]: isin
Out[67]: []

In [68]: if isin ==[]:
   ....:     print "true"
   ....:
true

yum.conf 	
proxy=http://proxyprd.scotia-capital.com:8080
.bashrc
export http_proxy=http://proxyprd.scotia-capital.com:8080
export https_proxy=http://proxyprd.scotia-capital.com:8080
vi /etc/ambari-agent/conf/ambari-agent.ini
http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.4.2.0
http://public-repo-1.hortonworks.com/HDP/centos6/2.x/updates/2.4.2.0
http://public-repo-1.hortonworks.com/HDP-UTILS-1.1.0.20/repos/centos6
yum install wget unzip 
yum install java-1.8.0-openjdk.x86_64
yum install java-1.8.0-openjdk-devel.x86_64
yum install openssl
yum install ntsysv
yum -y install openssh-clients.x86_64
yum -y install  ntp.x86_64 ntpdate.x86_64 
chkconfig ntp on 
vi /etc/rc.local
if test -f /sys/kernel/mm/transparent_hugepage/enabled; then
   echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi
if test -f /sys/kernel/mm/transparent_hugepage/defrag; then
   echo never > /sys/kernel/mm/transparent_hugepage/defrag
fi

 http://test.localdomain:50111/templeton/v1/status?user.name=ambari-qa Traceback (most recent call last): File "/var/lib/ambari-agent/cache/common-services/HIVE/0.12.0.2.0/package/alerts/alert_webhcat_server.py", line 190, in execute url_response = urllib2.urlopen(query_url, timeout=connection_timeout) File "/usr/lib64/python2.6/urllib2.py", line 126, in urlopen return _opener.open(url, data, ti	
 	
 	/usr/bin/python
 	
 	hc=HiveContext(sc)
hc.sql("select * from test").show()
./sqlline.py localhost:2181:/hbase-unsecure
	
	
hc=HiveContext(sc)
df = hc.read.format("org.apache.phoenix.spark").option("table", "TABLE1").option("zkUrl", "localhost:2181:/hbase-unsecure").load()	

/usr/hdp/2.4.0.0-169/hive/lib/mysql-connector-java.jar:/usr/hdp/current/spark-client/conf/hive-site.xml:/root/tmp/phoenix-4.6.0-HBase-1.1-bin/phoenix-spark-4.6.0-HBase-1.1.jar
	
/usr/hdp/2.4.0.0-169/hive/lib/mysql-connector-java.jar:/root/phoenix/phoenix-4.7.0-HBase-1.1-bin/phoenix-spark-4.7.0-HBase-1.1.jar:/root/phoenix/phoenix-4.7.0-HBase-1.1-bin/phoenix-4.7.0-HBase-1.1-client-spark.jar:/usr/hdp/current/hbase-client/lib/*.jar:/usr/hdp/current/spark-client/conf/hive-site.xml
/usr/hdp/2.4.0.0-169/hive/lib/mysql-connector-java.jar:/usr/hdp/current/spark-client/conf/hive-site.xml	
	
	
http://172.23.18.232/HDP/centos6/2.x/updates/2.4.2.0/	
http://172.23.18.232/HDP-UTILS-1.1.0.20/repos/centos6	
http://172.23.18.232/html/HDP/centos6/2.x/updates/2.4.2.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins	
	
http://172.23.18.232/AMBARI-2.2.2.0/centos6/2.2.2.0-460	
http://172.23.18.232/AMBARI-2.2.2.0/centos6/2.2.2.0-460/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
	
	
#VERSION_NUMBER=2.4.2.0-258
[HDP-2.4.2.0]
name=HDP Version - HDP-2.4.2.0
baseurl=http://172.23.18.224/HDP/centos6/2.x/updates/2.4.2.0/
gpgcheck=1
gpgkey=http://172.23.18.224/html/HDP/centos6/2.x/updates/2.4.2.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
enabled=1
priority=1


[HDP-UTILS-1.1.0.20]
name=HDP Utils Version - HDP-UTILS-1.1.0.20
baseurl=http://172.23.18.224/HDP-UTILS-1.1.0.20/repos/centos6
gpgcheck=1
gpgkey=http://172.23.18.224/html/HDP/centos6/2.x/updates/2.4.2.0/RPM-GPG-KEY/RPM-GPG-KEY-Jenkins
enabled=1
priority=1
	


import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._
import java.sql.DriverManager



import sys
from pyspark import SparkContext
from py4j.java_gateway import java_import
from pyspark.sql import HiveContext, Row
import pandas
from datetime import datetime,timedelta
from pyspark.sql.functions import current_date, datediff, unix_timestamp
import time
import json
java_import(sc._jvm,"java.sql.DriverManager")
conn=sc._jvm.DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-unsecure")
stmt = conn.createStatement()
stmt.execute("drop table if exists t")
df = sqlContext.read.format("org.apache.phoenix.spark").option("table", "TABLE1").option("predicate", "ID=1").option("zkUrl", "localhost:2181:/hbase-unsecure").load()

java_import(sc._jvm,"org.apache.spark.sql.SQLContext")

df=sqlContext.phoenixTableAsDataFrame("TABLE1",["ID","COL1"],{"hbase.zookeeper.quorum":"localhost:2181:/hbase-unsecure"})

#############scala predicate example

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.phoenix.spark._

val configuration = new Configuration()
configuration.set("hbase.zookeeper.quorum","localhost:2181:/hbase-unsecure")
configuration.set("predicate","ID=1")
// Can set Phoenix-specific settings, requires 'hbase.zookeeper.quorum'

val sqlContext = new SQLContext(sc)

// Load the columns 'ID' and 'COL1' from TABLE1 as a DataFrame
val df = sqlContext.phoenixTableAsDataFrame( "TABLE1", Array("ID", "COL1"),"ID=1" ,conf = configuration)
val df = sqlContext.phoenixTableAsDataFrame( "TABLE1", Array("ID", "COL1"),Option("ID in TT") ,conf = configuration)


  def phoenixTableAsDataFrame(table: String, columns: Seq[String],
                               predicate: Option[String] = None, zkUrl: Option[String] = None,
                               conf: Configuration = new Configuration): DataFrame = {

    // Create the PhoenixRDD and convert it to a DataFrame
    new PhoenixRDD(sqlContext.sparkContext, table, columns, predicate, zkUrl, conf)
      .toDataFrame(sqlContext)
  }

df.show


#############JDBC connection example
import sys
from pyspark import SparkContext
from py4j.java_gateway import java_import
from pyspark.sql import HiveContext, Row
import pandas
from datetime import datetime,timedelta
from pyspark.sql.functions import current_date, datediff, unix_timestamp
import time
import json
java_import(sc._jvm,"java.sql.DriverManager")
conn=sc._jvm.DriverManager.getConnection("jdbc:phoenix:localhost:2181:/hbase-unsecure")
stmt = conn.createStatement()
stmt.execute("drop table if exists t")
df = sqlContext.read.format("org.apache.phoenix.spark").option("table", "select * from table1 where id in (select id from table2)").option("zkUrl", "localhost:2181:/hbase-unsecure").load()


package com.github.gbraccialli.spark;

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.sql.{Connection, DriverManager, DatabaseMetaData, ResultSet}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.JdbcRDD

object PhoenixSparkSample{

  def main(args: Array[String]) {

   val sparkConf = new SparkConf()
   val sc = new SparkContext(sparkConf)
   val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

   //option 1, read table
   val jdbcDF = sqlContext.read.format("jdbc").options( 
     Map(
     "driver" -> "org.apache.phoenix.jdbc.PhoenixDriver",
     "url" -> "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure",
     "dbtable" -> "TABLE1")).load()
  
   jdbcDF.show
 
 
   //option 2, read custom query
   def getConn(driverClass: => String, connStr: => String, user: => String, pass: => String): Connection = {
     var conn:Connection = null
     try{
       Class.forName(driverClass)
        conn = DriverManager.getConnection(connStr, user, pass)
     }catch{ case e: Exception => e.printStackTrace }
     conn
   }
 
   val myRDD = new JdbcRDD( sc, () => getConn("org.apache.phoenix.jdbc.PhoenixDriver", "jdbc:phoenix:localhost:2181:/hbase-unsecure", "", "") ,
     "select sum(10) total from TABLE1 where ? <= id and id <= ?",
     1, 10, 2,
     r => r.getString("total")
   )
   
   myRDD.collect().foreach(line => println(line))
 

   //option 3, using phoenix-spark package
   val df = sqlContext.load(
     "org.apache.phoenix.spark",
     Map("table" -> "TABLE1", "zkUrl" -> "localhost:2181:/hbase-unsecure")
   )

   df.show

   sc.stop()
  }

}

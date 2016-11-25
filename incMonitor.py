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

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
            yield l[i:i + n]    


java_import(sc._jvm,"org.apache.hadoop.fs.FileUtil")
java_import(sc._jvm,"org.apache.hadoop.fs.FileSystem")
java_import(sc._jvm,"org.apache.hadoop.fs.Path")
java_import(sc._jvm,"java.sql.DriverManager")
con=sc._jsc.hadoopConfiguration()
fs=sc._jvm.FileSystem.get(con)
root_dir=str(fs.getUri())+"/data/"
inc_dir="t_cf_inc"
hist_dir="t_cf"
indoor_dir='indoor'
flow_dir='flow'
incFolders=fs.listStatus(sc._jvm.Path(root_dir+inc_dir))
splitFolders=[]
splitNum=6

for x in incFolders:
    if x.isDirectory():
        incFolders=str(x.getPath())
        splitFolders.append(incFolders)    

numPerGroup=len(splitFolders)/splitNum+1

folderLists1=[]
folderLists2=[]
folderLists3=[]
folderLists4=[]
folderLists5=[]
folderLists6=[]
folderLists7=[]
folderLists8=[]

cnt=1
for x in chunks(splitFolders,numPerGroup):
    if cnt==1:
        folderLists1=x
    if cnt==2:
        folderLists2=x
    if cnt==3:
        folderLists3=x
    if cnt==4:
        folderLists4=x
    if cnt==5:
        folderLists5=x
    if cnt==6:
        folderLists6=x
    if cnt==7:
        folderLists7=x
    if cnt==8:
        folderLists8=x
    cnt=cnt+1
    
filsNum1=0
filsNum2=0
filsNum3=0
filsNum4=0
filsNum5=0
filsNum6=0
filsNum7=0
filsNum8=0
emptyFolderNum1=0
emptyFolderNum2=0
emptyFolderNum3=0
emptyFolderNum4=0
emptyFolderNum5=0
emptyFolderNum6=0
emptyFolderNum7=0
emptyFolderNum8=0

unEmptyFolderNum1=0
unEmptyFolderNum2=0
unEmptyFolderNum3=0
unEmptyFolderNum4=0
unEmptyFolderNum5=0
unEmptyFolderNum6=0
unEmptyFolderNum7=0
unEmptyFolderNum8=0

spaceUsed1=0
spaceUsed2=0
spaceUsed3=0
spaceUsed4=0
spaceUsed5=0
spaceUsed6=0
spaceUsed7=0
spaceUsed8=0

for x in folderLists1: 
    filsNum1=filsNum1+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum1=emptyFolderNum1+1
    else:
        unEmptyFolderNum1=unEmptyFolderNum1+1
        spaceUsed1+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists2:        
    filsNum2=filsNum2+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum2=emptyFolderNum2+1
    else:
        unEmptyFolderNum2=unEmptyFolderNum2+1
        spaceUsed2+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists3:
    filsNum3=filsNum3+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum3=emptyFolderNum3+1
    else:
        unEmptyFolderNum3=unEmptyFolderNum3+1
        spaceUsed3+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists4:
    filsNum4=filsNum4+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum4=emptyFolderNum4+1
    else:
        unEmptyFolderNum4=unEmptyFolderNum4+1
        spaceUsed4+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists5:
    filsNum5=filsNum5+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum5=emptyFolderNum5+1
    else:
        unEmptyFolderNum5=unEmptyFolderNum5+1
        spaceUsed5+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists6:
    filsNum6=filsNum6+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum6=emptyFolderNum6+1
    else:
        unEmptyFolderNum6=unEmptyFolderNum6+1
        spaceUsed6+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists7:
    filsNum7=filsNum7+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum7=emptyFolderNum7+1
    else:
        unEmptyFolderNum7=unEmptyFolderNum7+1
        spaceUsed7+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
for x in folderLists8:
    filsNum8=filsNum8+fs.getContentSummary(sc._jvm.Path(x)).getFileCount()
    if fs.getContentSummary(sc._jvm.Path(x)).getFileCount()==0:
        emptyFolderNum8=emptyFolderNum8+1
    else:
        unEmptyFolderNum8=unEmptyFolderNum8+1
        spaceUsed8+=fs.getContentSummary(sc._jvm.Path(x)).getSpaceConsumed()
        
print "remaining files          count: " ,filsNum1,filsNum2,filsNum3,filsNum4,filsNum5,filsNum6, filsNum1+filsNum2+filsNum3+filsNum4+filsNum5+filsNum6
print "remaining folder         count: " ,unEmptyFolderNum1,unEmptyFolderNum2,unEmptyFolderNum3,unEmptyFolderNum4,unEmptyFolderNum5,unEmptyFolderNum6,unEmptyFolderNum7,unEmptyFolderNum8
print "Processed folder         count: " ,emptyFolderNum1,emptyFolderNum2,emptyFolderNum3,emptyFolderNum4,emptyFolderNum5,emptyFolderNum6,emptyFolderNum7,emptyFolderNum8
print "space  in folder remaining(MB): " ,spaceUsed1/1048576/3,spaceUsed2/1048576/3,spaceUsed3/1048576/3,spaceUsed4/1048576/3,spaceUsed5/1048576/3,spaceUsed6/1048576/3,spaceUsed7/1048576/3,spaceUsed8/1048576/3
print "current time : ", str(time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())))


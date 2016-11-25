#!/bin/bash

streamProcessNum=$(yarn application -list | grep "application_" | grep "StreamNavroom" | wc -l)
if [ "$streamProcessNum" -ge 1 ];  then
    echo "StreamNavroom  yarn applications is running,killing it now "
    yarn application -list | grep "application_" | grep "StreamNavroom" |   awk '{print $1}' | xargs yarn application -kill
fi

done=0
while : ; do
  if [ "$done" -ne 0 ]; then
      break
  fi
  incProcessFlag=$(hdfs dfs -ls /data/t_cf_inc/transfer_completed.txt | wc -l)

  if [ "$incProcessFlag" -ne 1 ];  then
      echo "file transfer not complete"
      sleep 300
      done=0
  else
      echo "good to run"
      nohup ssh namenode spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 5 & 
      nohup ssh namenode spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 6 &
      nohup ssh data1 spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 1 &
      nohup ssh data2 spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 2 &
      nohup ssh data3 spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 3  &
      nohup ssh data4 spark-submit --master yarn-client --num-executors 3 --driver-memory 2g --executor-memory 3g --executor-cores 3 --conf spark.yarn.am.memory=1024m /opt/src/main_inc.py 4  &
      done=1
  fi    
done

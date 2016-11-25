#!/bin/bash

done=0
while : ; do
  if [ "$done" -ne 0 ]; then
      break
  fi
  incProcessNum=$(yarn application -list | grep "application_" | grep "incData" | wc -l)
  if [ "$incProcessNum" -ge 1 ];  then
      echo "there is "$incProcessNum" yarn applications is running, wait 5 minutes try again" 
      sleep 300
      done=0
  else
      echo "good to run"
      spark-submit --master yarn-client --num-executors 16 --driver-memory 2g --executor-memory 6g --executor-cores 1 --conf spark.yarn.am.memory=1024m /opt/src/main_report.py
      done=1
  fi    
done

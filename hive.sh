#!/bin/sh
hive  << EOF
drop table if exists df_report_table;
create table df_report_table as 
select df_flow_fin.entityid  as entityid,
       df_flow_fin.clientmac as clientmac,
       df_flow_fin.utoday    as utoday,
       df_flow_fin.ufirstday as ufirstday ,
       coalesce(df_indoor_fin.secondsbyday     ,cast(0 as bigint)) as secondsbyday    ,
       coalesce(df_indoor_fin.indoors30        ,cast(0 as bigint)) as indoors30       ,
       coalesce(df_indoor_fin.indoors7         ,cast(0 as bigint)) as indoors7        ,
       coalesce(df_indoor_fin.FirstIndoor      ,cast(0 as bigint)) as FirstIndoor     ,
       coalesce(df_indoor_fin.LastIndoor       ,cast(0 as bigint)) as LastIndoor      ,
       coalesce(df_indoor_fin.indoors          ,cast(0 as bigint)) as indoors         ,
       coalesce(df_indoor_fin.indoorsPrevMonth ,cast(0 as bigint)) as indoorsPrevMonth,
       coalesce(df_indoor_fin.r_indoors7       ,cast(0 as bigint)) as r_indoors7      ,
       coalesce(df_indoor_fin.r_indoors30      ,cast(0 as bigint)) as r_indoors30     ,
       coalesce(df_flow_fin.visits30           ,cast(0 as bigint)) as visits30        ,
       coalesce(df_flow_fin.visits7            ,cast(0 as bigint)) as visits7         ,
       coalesce(df_flow_fin.FirstVisit         ,cast(0 as bigint)) as FirstVisit      ,
       coalesce(df_flow_fin.LastVisit          ,cast(0 as bigint)) as LastVisit       ,
       coalesce(df_flow_fin.visits             ,cast(0 as bigint)) as visits          ,
       coalesce(df_flow_fin.visitsPrevMonth    ,cast(0 as bigint)) as visitsPrevMonth 
  from df_flow_fin left outer join df_indoor_fin
    on (df_flow_fin.clientmac = df_indoor_fin.clientmac and 
        df_flow_fin.entityid = df_indoor_fin.entityid and
        df_flow_fin.utoday = df_indoor_fin.utoday);
EOF

nohup spark-submit --class "StreamNavroom" --master yarn-client  --num-executors 5 --driver-memory 3g  --executor-memory 3g  --executor-cores 4 /opt/src/stream.jar rawdata &

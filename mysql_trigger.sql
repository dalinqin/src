
GRANT REPLICATION SLAVE ON *.* TO 'oracle'@'%' IDENTIFIED BY 'rawdbdata';


GRANT SELECT, SHOW VIEW, PROCESS, REPLICATION CLIENT ON *.* TO 'fereader'@'%' identified by 'oracle123456';

CREATE TABLE v_entity_ap_rel_remote (  
  entityid int(11) not null,
  rssi smallint(6) ,
  apmac varchar(12),
  IndoorSecondsThrehold bigint(11),
  LeaveMinutesThrehold bigint(11) 
) ENGINE=FEDERATED CONNECTION='mysql://fereader:oracle123456@ipaddress:8301/maindb/v_entity_ap_rel' ;

create table v_entity_ap_rel(
  entityid int(11) not null,
  rssi smallint(6) ,
  apmac varchar(12),
  IndoorSecondsThrehold bigint(11),
  LeaveMinutesThrehold bigint(11) ,
  primary key(apmac,entityid)
) ENGINE=InnoDB;





create table raw_data_rep(
id bigint(20) unsigned NOT NULL AUTO_INCREMENT,
entityid int,
sourcemac varchar(12),
time bigint,
IndoorSecondsThrehold int,
LeaveMinutesThrehold int,
PRIMARY KEY (`id`))
ENGINE=InnoDB  DEFAULT CHARSET=utf8 ;



DROP TRIGGER IF EXISTS trig_raw_data;
DELIMITER //
CREATE TRIGGER trig_raw_data AFTER INSERT ON raw_data
FOR EACH ROW
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE ids INT;
    DECLARE indoors INT;
    DECLARE leaves INT;
    DECLARE cur CURSOR FOR select entityid,IndoorSecondsThrehold,LeaveMinutesThrehold from v_entity_ap_rel where apmac=NEW.ApMacAddress AND case when NEW.rssi>rssi then 1 else 0 end=1;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN cur;
        ins_loop: LOOP
            FETCH cur INTO ids,indoors,leaves;
            IF done THEN
                LEAVE ins_loop;
            END IF;
            IF  NEW.Time +INTERVAL 5 minute > NEW.UpdatingTime Then
                INSERT INTO raw_data_rep(entityid,sourcemac,time,IndoorSecondsThrehold,LeaveMinutesThrehold) VALUES (ids, NEW.SourceMacAddress,unix_timestamp(NEW.time),indoors,leaves);
            END IF;
        END LOOP;
    CLOSE cur;
END; //
DELIMITER ;



./kafka-topics.sh --zookeeper namenode:2181 --delete --topic ogg
./kafka-topics.sh --zookeeper namenode:2181 --create --topic ogg --partitions 3 --replication-factor 1
./kafka-console-consumer.sh --zookeeper namenode:2181 --topic rawdata --from-beginning | jq '.'	

SELECT DISTINCT tentity.ID as entityid,
                tvbox.indoorrssi as rssi,
                upper(tvbox_ap_rel.apmac) as apmac,
                CASE WHEN tentity.indoorsecondsthrehold = 0 THEN 10 ELSE tentity.indoorsecondsthrehold end AS IndoorSecondsThrehold,
                CASE WHEN tentity.leaveminutesthrehold = 0 THEN 300 ELSE tentity.leaveminutesthrehold * 60 end AS LeaveMinutesThrehold
  FROM tvbox, tvbox_ap_rel, tentity, tentity_vbox_rel
 WHERE tvbox_ap_rel.flag = 1
   AND tvbox_ap_rel.vboxid = tvbox.id
   AND tentity_vbox_rel.vboxid = tvbox.id
   AND tentity_vbox_rel.entityid = tentity.id
   AND tentity.flags < 90
   and tvbox.Flag < 90

--complicated mysql trigger 

drop table  newdb.`raw_data_rep_new`;
CREATE TABLE newdb.`raw_data_rep_new` (
  `sourcemac` varchar(12) NOT NULL DEFAULT '',
  `entityid` int(11) NOT NULL DEFAULT '0',
  `etime` bigint(20) NOT NULL DEFAULT '0',
  `ltime` bigint(20) DEFAULT NULL,
  `seconds` int(11) DEFAULT NULL
) ENGINE=InnoDB CHARSET=utf8 ;

drop table rawdb.`raw_data_rep_new`;
CREATE TABLE rawdb.`raw_data_rep_new` (
	id  bigint(10) unsigned NOT NULL AUTO_INCREMENT,
  `sourcemac` varchar(12) NOT NULL DEFAULT '',
  `entityid` int(11) NOT NULL DEFAULT '0',
  `etime` bigint(20) NOT NULL DEFAULT '0',
  `ltime` bigint(20) DEFAULT NULL,
  `seconds` int(11) DEFAULT NULL,
	done char(1)  default '0',
  PRIMARY KEY (id,`sourcemac`,`entityid`,`etime`,done)
)  ENGINE=InnoDB CHARSET=utf8 PARTITION BY LIST COLUMNS(done) ( PARTITION done values in ('0'),PARTITION go values in ('1'))
 ;

drop trigger trig_raw_data;
DELIMITER //
CREATE TRIGGER trig_raw_data AFTER INSERT ON raw_data
FOR EACH ROW
BEGIN
    DECLARE done BOOLEAN DEFAULT FALSE;  
    DECLARE ids INT;
    DECLARE leaves INT;
    DECLARE t_etime INT;
    DECLARE t_ltime INT;
    declare new_time int;
    DECLARE cur CURSOR FOR select entityid,LeaveMinutesThrehold from v_entity_ap_rel where apmac=NEW.ApMacAddress AND  NEW.rssi>=rssi ;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    OPEN cur;
        ins_loop: LOOP
            FETCH cur INTO ids,leaves;
            IF done THEN
                close cur;
                LEAVE ins_loop;
            END IF;
            set new_time=unix_timestamp(NEW.time);
            IF  NEW.Time +INTERVAL 5 minute > NEW.UpdatingTime Then
                BLOCK2: BEGIN
                DECLARE cnt ,new_done INT DEFAULT 0;
                DECLARE cur_new CURSOR FOR select etime,ltime from raw_data_rep_new where sourcemac=NEW.SourceMacAddress AND entityid=ids and done='0';
                DECLARE CONTINUE HANDLER FOR NOT FOUND SET new_done = TRUE;
                open cur_new;
                cur_new_loop: LOOP
                    fetch cur_new into t_etime,t_ltime;
                    IF new_done THEN
                        IF cnt=0 THEN
                            insert into raw_data_rep_new(sourcemac,entityid,etime,ltime,seconds) VALUES (NEW.SourceMacAddress, ids,new_time,new_time,0);
                        END IF;    
                        close cur_new;
                        LEAVE cur_new_loop;
                    END IF;
                    set cnt=1;
                    IF  new_time> t_ltime Then
                        if (new_time -t_ltime) >leaves  then 
                            if t_etime=t_ltime then
                             	 insert into newdb.raw_data_rep_new(sourcemac,entityid,etime,ltime,seconds) VALUES (NEW.SourceMacAddress, ids,t_etime,t_ltime,5);
                            else
                               insert into newdb.raw_data_rep_new(sourcemac,entityid,etime,ltime,seconds) VALUES (NEW.SourceMacAddress, ids,t_etime,t_ltime,t_ltime-t_etime);
                            end if; 	
                            update raw_data_rep_new set done='1' where  sourcemac=NEW.SourceMacAddress AND entityid=ids and done=0;
                            INSERT INTO raw_data_rep_new(sourcemac,entityid,etime,ltime,seconds) VALUES (NEW.SourceMacAddress, ids,new_time,new_time,0);
                        else 
                            UPDATE rawdb.raw_data_rep_new set ltime=new_time,seconds= new_time-t_etime where sourcemac=NEW.SourceMacAddress AND entityid=ids ;
                        end if;
                    END IF;   
                    END LOOP cur_new_loop;                 
                END BLOCK2;
            END IF;
        END LOOP;
END; //
DELIMITER ;

drop table if exists t_mine_visit;

create table t_mine_visit(
EntityId INTEGER not null ,
ClientMac CHAR(12) not null ,
Brand VARCHAR(256) ,
FirstVisit date ,
LastVisit date ,
visits INTEGER ,
visitsPrevMonth INTEGER ,
visits30 INTEGER ,
visits7 INTEGER ,
FirstIndoor date ,
LastIndoor date ,
indoors INTEGER ,
indoorsPrevMonth INTEGER ,
indoors30 INTEGER ,
indoors7 INTEGER ,
secondsByDay INTEGER 
CONSTRAINT pk PRIMARY KEY (EntityId, ClientMac)); 

drop table if exists t_indoor;

create table t_indoor(
EntityId INTEGER not null ,
ClientMac CHAR(12) not null ,
etime date not null,
ltime date,
seconds integer 
CONSTRAINT pk PRIMARY KEY (EntityId,ClientMac,etime));

drop table if exists t_flow;

create table t_flow(
EntityId INTEGER not null ,
ClientMac CHAR(12) not null ,
etime date not null,
ltime date,
seconds integer 
CONSTRAINT pk PRIMARY KEY (EntityId,ClientMac,etime));

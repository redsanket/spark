register /tmp/integration_test_files/lib/FETLProjector.jar;
register /tmp/integration_test_files/lib/BaseFeed.jar;

in1 = load 'hdfs://${NAMENODE_NAME}:8020//data/daqdev/abf/data/${DATASET_NAME}/20130309/PAGE/Valid/News/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in1 = filter in1 by connection_speed == 'CS_broadband';
out1 = foreach in1 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
grp = GROUP out1 ALL;
totalRecords = foreach grp generate  COUNT(out1);
dump totalRecords;
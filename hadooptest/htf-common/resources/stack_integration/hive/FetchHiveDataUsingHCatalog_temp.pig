register '/home/y/libexec/hive/lib/hive-exec.jar';
register '/home/y/libexec/hive/lib/hive-common.jar'
input1 = load 'gdm.user1' USING org.apache.hive.hcatalog.pig.HCatLoader() as (calendar_day:chararray,connection_speed:chararray,region:chararray,valid:chararray,content_type:chararray,event_type:chararray,hostname:chararray,ip:chararray);
input1 = filter input1 by connection_speed == 'CS_broadband';
out1 = foreach input1 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
grp = GROUP out1 ALL;
totalRecords = foreach grp generate  COUNT(out1);
dump totalRecords;

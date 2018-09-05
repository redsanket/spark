set mapred.child.java.opts ' -Djavax.net.ssl.trustStore=/home/gs/conf/current/kms.jks ';

inputLoad = load 'hbase://integration_test_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('info:calendar_day info:connection_speed info:region info:valid info:content_type info:event_type info:hostname info:ip');
grp = GROUP inputLoad ALL;
totalRecords = foreach grp generate  COUNT(inputLoad);
dump totalRecords;

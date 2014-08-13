-- use webhdfs://$namenode for input

register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- pull in 20130309/PAGE/Valid/Mail/
in = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/Mail/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in = filter in by connection_speed == 'CS_mobile';
out = foreach in generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out into '/tmp/HTF/output/test_basic_webhdfs/out';

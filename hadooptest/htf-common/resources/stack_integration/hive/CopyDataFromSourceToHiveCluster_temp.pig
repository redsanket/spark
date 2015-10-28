register SCRIPT_PATH/FETLProjector.jar;
register SCRIPT_PATH/BaseFeed.jar;

in1 = load 'NAME_NODE_NAME/FILEPATH/20130309/PAGE/Valid/News/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in1 = filter in1 by connection_speed == 'CS_broadband';
out1 = foreach in1 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out1 into 'FILEPATH/';
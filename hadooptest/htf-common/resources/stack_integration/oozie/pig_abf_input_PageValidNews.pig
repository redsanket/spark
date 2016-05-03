-- stack int test, pig job meant to be run from oozie
-- process ABF data /data/daqdev/abf/data/Integration_Testing_DS_1430145075334/20130309/PAGE/Valid/News

register  $NAME_NODE/tmp/integration_test_files/lib/FETLProjector.jar;
register  $NAME_NODE/tmp/integration_test_files/lib/BaseFeed.jar;

-- in1 = load '/data/daqdev/abf/data/Integration_Testing_DS_1430145075334/20130309/PAGE/Valid/News/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in1 = load '$INPUTFILE' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');

in1 = filter in1 by connection_speed == 'CS_broadband';

out1 = foreach in1 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out1 into '$OUTPUTDIR';
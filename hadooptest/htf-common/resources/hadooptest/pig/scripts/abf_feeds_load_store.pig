register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- load a bunch of feed data
in1 = load '$protocol://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in2 = load '$protocol://$namenode/HTF/testdata/pig/20130309/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

in3 = load '$protocol://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in4 = load '$protocol://$namenode/HTF/testdata/pig/20130310/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

-- in5 = load '$protocol://$namenode/HTF/testdata/pig/20130311/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
-- in6 = load '$protocol://$namenode/HTF/testdata/pig/20130311/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

store in1 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in1';
store in2 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in2';

store in3 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in3';
store in4 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in4';

-- store in5 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in5';
-- store in6 into '$protocol://$namenode/HTF/output/abf_feeds_load_store_$protocol/in6';

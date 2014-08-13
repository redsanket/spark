-- use webhdfs://$namenode for input

register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- load a bunch of feed data
in1 = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in2 = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

in3 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in4 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

-- in5 = load 'webhdfs://$namenode/HTF/testdata/pig/20130311/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
-- in6 = load 'webhdfs://$namenode/HTF/testdata/pig/20130311/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

store in1 into '/tmp/HTF/output/abf_feeds_load_store_out/in1';
store in2 into '/tmp/HTF/output/abf_feeds_load_store_out/in2';

store in3 into '/tmp/HTF/output/abf_feeds_load_store_out/in3';
store in4 into '/tmp/HTF/output/abf_feeds_load_store_out/in4';

-- store in5 into '/tmp/HTF/output/abf_feeds_load_store_out/in5';
-- store in6 into '/tmp/HTF/output/abf_feeds_load_store_out/in6';

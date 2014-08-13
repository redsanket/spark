-- use webhdfs://$namenode for input

register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- load a bunch of feed data
-- and do basic foreach on it
in1 = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in2 = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

in3 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in4 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

-- in5 = load 'webhdfs://$namenode/HTF/testdata/pig/20130311/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
-- in6 = load 'webhdfs://$namenode/HTF/testdata/pig/20130311/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

out1 = foreach in1 generate *;
out2 = foreach in2 generate *;

out3 = foreach in3 generate *;
out4 = foreach in4 generate *;

-- out5 = foreach in5 generate *;
-- out6 = foreach in6 generate *;

store out1 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out1';
store out2 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out2';

store out3 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out3';
store out4 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out4';

-- store out5 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out5';
-- store out6 into '/tmp/HTF/output/abf_feeds_load_foreach_store_out/out6';

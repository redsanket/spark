register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- load a bunch of feed data
-- and do basic foreach on it
in1 = load '$protocol://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in2 = load '$protocol://$namenode/HTF/testdata/pig/20130309/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

in3 = load '$protocol://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
in4 = load '$protocol://$namenode/HTF/testdata/pig/20130310/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

-- in5 = load '$protocol://$namenode/HTF/testdata/pig/20130311/PAGE/Valid/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');
-- in6 = load '$protocol://$namenode/HTF/testdata/pig/20130311/NONPAGE/*/*' using com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,region:string,event_type:string,hostname:string');

out1 = foreach in1 generate *;
out2 = foreach in2 generate *;

out3 = foreach in3 generate *;
out4 = foreach in4 generate *;

-- out5 = foreach in5 generate *;
-- out6 = foreach in6 generate *;

store out1 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out1';
store out2 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out2';

store out3 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out3';
store out4 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out4';

-- store out5 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out5';
-- store out6 into '$protocol://$namenode/HTF/output/abf_feeds_load_foreach_store_$protocol/out6';

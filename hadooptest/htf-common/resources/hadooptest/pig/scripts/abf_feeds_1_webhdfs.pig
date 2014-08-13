-- use webhdfs://$namenode for input

register /home/y/lib/jars/FETLProjector.jar;
register /home/y/lib/jars/BaseFeed.jar;

-- pull in 20130309/PAGE/Valid/Mail/
in = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/Mail/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in = filter in by connection_speed == 'CS_mobile';
out = foreach in generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out into '/tmp/HTF/output/test_abf_feeds_1_webhdfs';

-- pull in 20130309/PAGE/Valid/News/
in2 = load 'webhdfs://$namenode/HTF/testdata/pig/20130309/PAGE/Valid/News/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in2 = filter in2 by connection_speed == 'CS_broadband';
out2 = foreach in2 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out2 into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2/';

-- pull in 20130310/PAGE/Valid/Mail
in3 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/Mail/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in3 = filter in3 by connection_speed == 'CS_mobile';
out3 = foreach in3 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out3 into '/tmp/HTF/output/test_abf_feeds_1_webhdfs3';

-- pull in 20130310/PAGE/Valid/News
in4 = load 'webhdfs://$namenode/HTF/testdata/pig/20130310/PAGE/Valid/News/*' USING com.yahoo.ccdi.fetl.sequence.pig.Projector('calendar_day:string,connection_speed:string,region:string,valid:string,content_type:string,event_type:string,hostname:string,ip:string');
in4 = filter in4 by connection_speed == 'CS_mobile';
out4 = foreach in4 generate calendar_day,connection_speed,region,valid,content_type,event_type,hostname,ip;
store out4 into '/tmp/HTF/output/test_abf_feeds_1_webhdfs4';

-- First Group
-- group first input on region
outRegion = group out by region;
store outRegion into '/tmp/HTF/output/test_abf_feeds_1_webhdfsRegion';
-- split by region
split out into outasia if region == 'asia',
                outeurope if region == 'europe',
                outyahoo if region == 'yahoo';
store outasia into '/tmp/HTF/output/test_abf_feeds_1_webhdfsasia';
store outeurope into '/tmp/HTF/output/test_abf_feeds_1_webhdfseurope';
store outyahoo into '/tmp/HTF/output/test_abf_feeds_1_webhdfsyahoo';


-- group second input on region
out2Region = group out2 by region;
store out2Region into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2Region';
-- split by region
split out2 into out2asia if region == 'asia',
                out2europe if region == 'europe',
                out2yahoo if region == 'yahoo';
store out2asia into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2asia';
store out2europe into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2europe';
store out2yahoo into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2yahoo';


-- group third input on region
out3Region = group out3 by region;
store out3Region into '/tmp/HTF/output/test_abf_feeds_1_webhdfs3Region';
-- split by region
split out3 into out3asia if region == 'asia',
                out3europe if region == 'europe',
                out3yahoo if region == 'yahoo';
store out3asia into '/tmp/HTF/output/test_abf_feeds_1_webhdfs3asia';
store out3europe into '/tmp/HTF/output/test_abf_feeds_1_webhdfs3europe';
store out3yahoo into '/tmp/HTF/output/test_abf_feeds_1_webhdfs3yahoo';


-- group fourth input on region
out4Region = group out4 by region;
store out4Region into '/tmp/HTF/output/test_abf_feeds_1_webhdfs4Region';
-- split by region
split out4 into out4asia if region == 'asia',
                out4europe if region == 'europe',
                out4yahoo if region == 'yahoo';
store out4asia into '/tmp/HTF/output/test_abf_feeds_1_webhdfs4asia';
store out4europe into '/tmp/HTF/output/test_abf_feeds_1_webhdfs4europe';
store out4yahoo into '/tmp/HTF/output/test_abf_feeds_1_webhdfs4yahoo';

-- Second Group
-- group first input on event_type
outEvent = group out by event_type;
store outEvent into '/tmp/HTF/output/test_abf_feeds_1_webhdfsEvent';
-- split by event_type
split out into outpage if event_type == 'page',
                outclick if event_type == 'click';
store outpage into '/tmp/HTF/output/test_abf_feeds_1_webhdfspage';
store outclick into '/tmp/HTF/output/test_abf_feeds_1_webhdfsclick';

-- group second input on event_type
out2Event = group out2 by event_type;
store out2Event into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2Event';
-- split by event_type
split out2 into out2page if event_type == 'page',
                out2click if event_type == 'click';
store out2page into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2page';
store out2click into '/tmp/HTF/output/test_abf_feeds_1_webhdfs2click';

-- First Cogroup and Join
-- cogroup Mail and join the two Mail feed instances
MailGroup = cogroup out by hostname, out3 by hostname;
store MailGroup into '/tmp/HTF/output/test_abf_feeds_1_webhdfsMailGroup';
CountMailGroup = foreach MailGroup generate flatten(out), flatten(out3);
store CountMailGroup into '/tmp/HTF/output/test_abf_feeds_1_webhdfsCountMailGroup';

MailGroupJoin = join out by hostname, out3 by hostname;
store MailGroupJoin into '/tmp/HTF/output/test_abf_feeds_1_webhdfsMailGroupJoin';

-- cogroup News join the two News instances
NewsGroup = cogroup out2 by hostname, out4 by hostname;
store NewsGroup into '/tmp/HTF/output/test_abf_feeds_1_webhdfsNewsGroup';
CountNewsGroup = foreach NewsGroup generate flatten(out2), flatten (out4);
store CountNewsGroup into '/tmp/HTF/output/test_abf_feeds_1_webhdfsCountNewsGroup';

NewsGroupJoin = join out2 by hostname, out4 by hostname;
store NewsGroupJoin into '/tmp/HTF/output/test_abf_feeds_1_webhdfsNewsGroupJoin';

-- big honking Join across feeds
PageNonpageJoinFirst = join out by calendar_day, out2 by calendar_day;
store PageNonpageJoinFirst into '/tmp/HTF/output/test_abf_feeds_1_webhdfs_out/PageNonpageJoinFirst';

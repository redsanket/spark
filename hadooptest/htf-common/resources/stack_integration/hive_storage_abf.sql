use ${IN_DB};
drop table ${IN_TABLE};
CREATE EXTERNAL TABLE ${IN_TABLE} (calendar_day string, connection_speed string, region string, valid string, content_type string, event_type string, hostname string, ip string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION "/tmp/test_stackint/Tables";
LOAD DATA INPATH   '${my_srcFile}'   OVERWRITE INTO TABLE ${IN_TABLE} ;
SELECT *  FROM ${IN_TABLE}  WHERE connection_speed='CS_broadband' AND hostname='fe224.global.media.bf1.yahoo.com';


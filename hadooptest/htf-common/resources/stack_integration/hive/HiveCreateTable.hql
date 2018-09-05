CREATE DATABASE IF NOT EXISTS gdm;
CREATE EXTERNAL TABLE gdm.user1 (calendar_day string, connection_speed string, region string, valid string, content_type string, event_type string, hostname string, ip string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

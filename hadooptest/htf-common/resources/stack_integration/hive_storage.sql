use ${IN_DB};
drop table ${IN_TABLE};
create table ${IN_TABLE} (name string, age int) row format delimited fields terminated by '\t' stored as textfile;
load data inpath  '${my_srcFile}'   overwrite into table ${IN_TABLE};
select * from ${IN_TABLE};


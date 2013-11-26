

#############################################################
## Section is date/time related
#############################################################
# First convert PDT To UTC it is not alreadyh in UTC
s/ PDT 2010/ UTC 2010/

## Normalize the Unix 'date' output 
##      Tue                 Oct                26           18:        17:       22       UTC 2010
s/[MTWFS][ouehra][neduit] [JFMASOND][a-z][a-z] [ 123][0-9] [ 012][0-9]:[ 0-5][0-9]:[ 0-5][0-9] UTC 2010/ddd mmm 00 00:00:00 UTC 2010/


## Normalize the date output from Hadoop ls 
#   2010-10-20                         20:31 
s/ 201[0-9]-[01][0-9]-[0123][0-9] [ 012][0-9]:[0-6][0-9] / 2010-00-00 00:00 /

## another type is sort of log entries
## 10/10/26 19:01:54 WARN conf.Configuration: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
s#10/[01][0-9]/[0123][0-9] [ 012][0-9]:[0-6][0-9]:[0-6][0-9]#10/00/00 00:00:00 #

#  Stat output in the form of: 2010-11-29 22:10:43
s/^201[0-5]-[01][0-9]-[0123][0-9] [ 012][0-9]:[0-6][0-9]:[0-6][0-9]$/2010-00-00 00:00:00/

# Yet another form in the log : "11/02/23 22:10:43 "  ==> "10/00/00 00:00:00 "
s#^1[0-9]/[01][0-9]/[0123][0-9] [ 012][0-9]:[0-6][0-9]:[0-6][0-9] #10/00/00 00:00:00 #


######################################################
### Non date/time relatd
#####################################################
# Now take care of the time-varying quantities
s/ in [0-9]* milliseconds/ in 000000  milliseconds/

######################################################
###  IP address and host names
#####################################################
# gsbl90772.blue.ygrid ==> gsbl00000.blue.ygrid
s#gsbl[0-9][0-9][0-9][0-9][0-9]\.blue\.ygrid#gsbl00000.blue.ygrid#g
s#dense[a-z]*[0-9][0-9]*\.blue\.ygrid#gsbl00000.blue.ygrid#g

# ip address i89.89.89.128 ==> 000.00.00.000 
s#[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*#000.00.00.000#g

######################################################
#### remove the deprated warning:   (the last one on dus, lsr, rmr
#####################################################
/mapred.task.id is deprecated/d
/org.apache.hadoop.log.EventCounter is deprecated/d
/: DEPRECATED: Please use /d

######################################################
#### Mapping of user,group hdfs,hdfs ==> hdfsqa, hdfs. This is for those cluster deployed with hdfs rather than hdfsqa
#####################################################
/2010-00-00/s/ hdfs hdfs / hdfsqa hdfs /
/2010-00-00/s/ hdfs users / hdfsqa users /

# /user/hdfs/.Trash ==> /user/hdfsqa/.Trash
/2010-00-00/s# /user/hdfs/# /user/hdfsqa/#
/Cannot access/s# /user/hdfs/.Trash# /user/hdfsqa/.Trash#

# exception: ==> Exception:
s#\. exception: #. Exception: #

# FS. command aborted. 
s# FS. command aborted. # FS. Command aborted. #

# Remove any directory listing  that has /.svn/ or ^.svn/ or /.svn$ 
/\/.svn\//d
/\/.svn$/d
/^.svn\//d

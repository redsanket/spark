##
# mgr.sh
#
# stack integration testing, feed processing manager script, this uses/takes a feed name,
# a date/time stamp, build an oozie workflow and properties, and submit the workflow
# to oozie for processing the instance of the feed, the actions used are:
#   cleanup_output      - rm output if exists, needed for reprocessing
#   check_input         - verify input exists
#   pig_raw_processor   - pig script to do basic processing and hdfs move
#   hive_storage        - hive storage into db.table=testdb.test_table1
#   hive_verify         - hql check on feed sanity
##


# kinit as dfsload
kinit -k -t  /homes/dfsload/dfsload.dev.headless.keytab dfsload@DEV.YGRID.YAHOO.COM

FEED=bidded_clicks

CURRENTDATE=`date "+%Y%m%d"`
echo "DATE: $CURRENTDATE"

# get the next hour
HOUR=`date "+%H"`
echo "HOUR: $HOUR"
let "HOURPLUSONE = HOUR + 1"
echo "HOURPLUSONE: $HOURPLUSONE"

# set the feed instance we want to process
# this is CURRENTDATE+HOURPLUSONE+00
# TODO: this will fail on day boundries, fix it
TIMESTAMP="$HOURPLUSONE"00
echo "TIMESTAMP: $TIMESTAMP"

DATESTAMP=$CURRENTDATE
echo "Datestamp is: $DATESTAMP"

# setup the feed instance area in hdfs
FEEDBASE="/data/daqdev/abf/data/Integration_Testing_DS_"$DATESTAMP$TIMESTAMP""
FEEDINPUT="/data/daqdev/abf/data/Integration_Testing_DS_"$DATESTAMP$TIMESTAMP"/20130309/PAGE/Valid/News"
##FEEDINPUT="/data/daqdev/abf/data/Integration_Testing_DS_1430145075334/20130309/PAGE/Valid/News"
PIPELINEINSTANCE="/tmp/test_stackint/Pipeline/$FEED/"$DATESTAMP$TIMESTAMP""
FEEDRESULT=$FEED"__"$DATESTAMP$TIMESTAMP"_out"
echo $FEEDRESULT
hadoop fs -rm -r -f -skipTrash  $PIPELINEINSTANCE
hadoop fs -mkdir -p $PIPELINEINSTANCE 
hadoop fs -mkdir $PIPELINEINSTANCE/$FEEDRESULT

# upload the hive-site
hadoop fs -put /home/y/libexec/hive/conf/hive-site.xml  $PIPELINEINSTANCE/.

# pre process job.properties params
SCRATCHPATH=/tmp/test_stackint/pipeline_scratch/$FEED/$DATESTAMP$TIMESTAMP
rm -rf $SCRATCHPATH 
mkdir -p $SCRATCHPATH 
cp -r  * $SCRATCHPATH/. 
sed "s:ADD_INSTANCE_PATH:$PIPELINEINSTANCE:g" $SCRATCHPATH/job.properties > $SCRATCHPATH/job.properties.tmp
mv $SCRATCHPATH/job.properties.tmp $SCRATCHPATH/job.properties
sed "s:ADD_INSTANCE_DATE_TIME:$DATESTAMP$TIMESTAMP:g" $SCRATCHPATH/job.properties > $SCRATCHPATH/job.properties.tmp
mv $SCRATCHPATH/job.properties.tmp $SCRATCHPATH/job.properties
sed "s:ADD_INSTANCE_OUTPUT_PATH:$PIPELINEINSTANCE/$FEEDRESULT:g" $SCRATCHPATH/job.properties > $SCRATCHPATH/job.properties.tmp
mv $SCRATCHPATH/job.properties.tmp $SCRATCHPATH/job.properties
sed "s:ADD_INSTANCE_INPUT_PATH:$FEEDINPUT/part*:g" $SCRATCHPATH/job.properties > $SCRATCHPATH/job.properties.tmp
mv $SCRATCHPATH/job.properties.tmp $SCRATCHPATH/job.properties

# setup the pipeline's files to process that instance
hadoop fs -put   $SCRATCHPATH/*  $PIPELINEINSTANCE/.


# poll for data to be ready by checking for DONE file
# check every 15 secs for 20minutes, then give up and error out
POLLCOUNTER=0
while [ $POLLCOUNTER -lt 80 ]; do
  echo "Polling for feed data to be ready, pollcounter is $POLLCOUNTER"
  # hadoop fs -ls  /data/daqdev/abf/data/Integration_Testing_DS_1430145075334/DONE
  hadoop fs -ls  "$FEEDBASE"/DONE
  if [ $? -eq "0" ]; then
    echo "Feed data is ready"
    break
  fi
  echo "Feed data not ready yet..."

  let POLLCOUNTER=POLLCOUNTER+1
  if [ $POLLCOUNTER -eq  79 ]; then
    echo "Error: feed data not ready after 20 minutes"
    exit 11
  fi
  sleep 15
done


# run the oozie workflow for the raw input etl processing
OOZIERESULT=`/home/y/var/yoozieclient/bin/oozie job -run -config $SCRATCHPATH/job.properties -oozie http://dense37.blue.ygrid.yahoo.com:4080/oozie -auth kerberos`
# if 0 return, cleanup scratch and exit
if [ $? -ne "0" ]
then
  echo "Error: oozie workflow submission returned non-zero, exit without cleaning up scratch space at:"
  echo "  $SCRATCHPATH"
  exit 2
fi
echo "OOZIE $OOZIERESULT"

# get the job's ID 
OOZIEJOBID=`echo $OOZIERESULT|cut -d ' ' -f2`
echo "OOZIE job identifier is: $OOZIEJOBID"

# wait for oozie job to complete
OOZIECOUNTER=0
while [  $OOZIECOUNTER -lt 10 ]; do
  echo "Waiting for oozie job to complete, counter is: $OOZIECOUNTER" 
  CHECKRESULT=`/home/y/var/yoozieclient/bin/oozie jobs -len 10 -oozie http://dense37.blue.ygrid.yahoo.com:4080/oozie -auth kerberos | grep $OOZIEJOBID`
  echo "CHECKRESULT is:  $CHECKRESULT"

  if [[ ! "$CHECKRESULT" =~ "RUNNING" ]]; then
    echo "Oozie job no longer reports as RUNNING, checking result..."
    break
  fi

  let OOZIECOUNTER=OOZIECOUNTER+1 
  if [ $OOZIECOUNTER -eq 9 ]; then
    echo "Error: oozie job never reported completed after 5 minutes"
    exit 11
  fi
  sleep 30
done

# check oozie job's result
if [ -z "$CHECKRESULT" ]; then
  echo "Error: Oozie job id not found when checking result"
  exit 12
elif [[ "$CHECKRESULT" =~ "KILLED" ]]; then
  echo "Error: Oozie job reports as KILLED"
  exit 13
elif [[ "$CHECKRESULT" =~ "FAILED" ]]; then
  echo "Error: Oozie job reports as FAILED"
  exit 14
elif [[ "$CHECKRESULT" =~ "SUCCEEDED" ]]; then
  echo "Oozie job reports SUCCEEDED"
  # cleanup
  rm -rf $SCRATCHPATH
fi
 

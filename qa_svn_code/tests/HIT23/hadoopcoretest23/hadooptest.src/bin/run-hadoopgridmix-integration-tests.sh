#!/bin/sh

# Set some defaults
HIT_PASS=${HIT_PASS:-0}
HIT_FAIL=${HIT_FAIL:-1}

##################
## Script Usage ##
##################
function parseCmd() {

  usage="Usage: $0 -results-dir=<results dir> -logs-dir=<logs dir>";
  if [ $# -ne 2 ]
  then
     echo ${usage}
     exit $HIT_FAIL
  fi
  
  PARAM1=`echo $1 | cut -d"=" -f1`
  VALUE1=`echo $1 | cut -d"=" -f2`
  secondParam=`echo $2 | cut -d"=" -f1`
  PARAM2=${secondParam:-'-logs-dir'}
  VALUE2=`echo $2 | cut -d"=" -f2`

  if [ ${PARAM1} = "-results-dir" ]
  then
     if [ "X${VALUE1}" = "X" ]
     then
       RESULTS_DIR=`pwd`
     else
       RESULTS_DIR=${VALUE1}
     fi
  else
     echo "${PARAM1} is invalid parameter..."
     echo ${usage}
     exit $HIT_FAIL
  fi
  
  # Log directory is optional
  if [ ${PARAM2} = "-logs-dir" ]
  then
    if [ "X${VALUE2}" != "X" ]; then
      LOGS_DIR=${VALUE2}
    else
      echo ${usage}
      exit $HIT_FAIL
    fi
  else
    echo "${PARAM2} is invalid paramter..."
    echo ${usage}
    exit $HIT_FAIL
  fi

  if [ ! -d "${RESULTS_DIR}" ]
  then
    echo "ERROR: Results directory (${RESULTS_DIR}) is not found."
    exit $HIT_FAIL
  fi

  if [ ! -d "${LOGS_DIR}" ]
  then
    echo "ERROR: Logs directory (${LOGS_DIR}) is not found."
    exit $HIT_FAIL
  fi

}

# XML elements and formats
header='<?xml version="1.0" encoding="UTF-8" ?>
<testsuite errors="%s" failures="%s" hostname="%s" name="%s" tests="%s" time="%s" timestamp="%s">'
fmt='  <testcase name="%s" time="%s"/>'
footer='</testsuite>'
# End of XML elements and formats

#####################
## Log the results ##
#####################
function logResults () {
    fmt='Tests run: "%s", Failures: "%s", Errors: "%s"'
    MESSAGE=$1
    TESTS=$2
    FAIL=$3
    ERRORS=$4
    echo $MESSAGE >> ${LOG_FILE}
    printf "$fmt\n" $TESTS $FAIL $ERRORS >&1
    printf "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: " >> ${RESULTS_FILE}
    printf "$fmt\n" $TESTS $FAIL $ERRORS >>  ${RESULTS_FILE}
}

##############################
## Validate the environment ##
##############################
function validate_env() {

  if [ -z "${HADOOP_COMMON_HOME}" ]
  then
      logResults "ERROR: Hadoop Home is not found..." 1 0 1;
      exit $HIT_FAIL
  fi
  
  if [ -z "${HADOOP_CONF_DIR}" ]
  then
      logResults "ERROR: Hadoop Configuration directory is not found..." 1 0 1
      exit $HIT_FAIL
  fi
  
  if [ -z "${USER}" ]
  then
    logResults "ERROR: User is not found..." 1 0 1
    exit $HIT_FAIL
  fi
  
  if [ -z "${JAVA_HOME}" ]
  then
    logResults "ERROR: Java home is not found..." 1 0 1
    exit $HIT_FAIL
  fi
  
  if [ -z "${HIT_HDFS_STORAGE_DIR}" ]
  then
    logResults "ERROR: HDFS storage directory not found..." 1 0 1 
    exit $HIT_FAIL
  fi
}

###########################################
## Run the mapreduce job for job history ##
###########################################
function run_mapreduce_job() {  
  
  #Create input data in local and copy it into DFS.
  input_data="Hadoop is framework for data intensive distributed application.\
  Hadoop enables applications to work with thousands of nodes."
  touch /tmp/data.txt
  count=1
  while [ $count -lt 50 ]
  do
    echo $input_data >> /tmp/data.txt
    count=`expr $count + 1`
  done

  echo "Copying the input data into DFS..." >> ${LOG_FILE} 2>&1
  ${HADOOP_COMMON_HOME}/bin/hadoop fs -ls $HIT_HDFS_STORAGE_DIR/input >> ${LOG_FILE} 2>&1
  if [ $? -ne 0 ]
  then
    ${HADOOP_COMMON_HOME}/bin/hadoop fs -mkdir $HIT_HDFS_STORAGE_DIR/input >>  ${LOG_FILE} 2>&1
    ${HADOOP_COMMON_HOME}/bin/hadoop fs -copyFromLocal /tmp/data.txt $HIT_HDFS_STORAGE_DIR/input >> ${LOG_FILE} 2>&1
  else
    ${HADOOP_COMMON_HOME}/bin/hadoop fs -rmr $HIT_HDFS_STORAGE_DIR/input/data.txt >> ${LOG_FILE} 2>&1
    ${HADOOP_COMMON_HOME}/bin/hadoop fs -copyFromLocal /tmp/data.txt $HIT_HDFS_STORAGE_DIR/input >> ${LOG_FILE} 2>&1
  fi

  exitcode=$?
  if [ ${exitcode} -eq 0 ]
  then
    echo "Data is copied successfully into the DFS..." >> ${LOG_FILE} 2>&1
  else
    logResults "ERROR: Data has not been copied successfully into the DFS.." 1 1 0
    exit $HIT_FAIL
  fi

  #Get the exaxmples jar.
  for examjar in `ls ${HADOOP_COMMON_HOME}/*.jar | grep -e examples`
  do
    if [ ! -z "${examjar}" ]
    then
      HADOOP_EXAMPLES_JAR=${examjar}
      break
    fi
  done

  if [ -z "${HADOOP_EXAMPLES_JAR}" ]
  then
      logResults "ERROR: Hadoop examples jar is not found.." 1 0 1
      exit $HIT_FAIL
  fi    
  
  #Remove the existing output directory.
  ${HADOOP_COMMON_HOME}/bin/hadoop fs -rmr $HIT_HDFS_STORAGE_DIR/output >> ${LOG_FILE} 2>&1 

  #Invoking the word count mapredue job.
  echo "Invoking the mapreduce job..." >> ${LOG_FILE} 2>&1
  ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} jar \
  ${HADOOP_EXAMPLES_JAR} wordcount -D mapred.map.tasks=2 -D mapred.reduce.tasks=2 \
    $HIT_HDFS_STORAGE_DIR/input $HIT_HDFS_STORAGE_DIR/output >> ${LOG_FILE} 2>&1
  exitcode=$?
  if [ $exitcode -eq 0 ]
  then
      echo "Mapreduce job has been completed successfully." >> ${LOG_FILE} 2>&1
      echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: MAPREDUCE JOB STATUS: PASSED." >> ${RESULTS_FILE} 2>&1
  else
      logResults "Mapreduce job has been failed. MAPREDUCE JOB STATUS: FAILED" 1 1 0
      exit $HIT_FAIL
  fi  
}

##########################################
## Get the job history path from config ##
##########################################
function get_job_history_path() {
  MAPRED_SITE_XML=${HADOOP_CONF_DIR}/mapred-site.xml
  HISTORY_COMP_LOC=`grep -n mapred.job.tracker.history.completed.location ${MAPRED_SITE_XML}`
  KEY_POSITION=`echo ${HISTORY_COMP_LOC} | cut -d':' -f1`
  VALUE_POSITION=`expr ${KEY_POSITION} + 1`
  JOB_HISTORY_COMP_PATH=`grep -n \<* ${MAPRED_SITE_XML} | grep "${VALUE_POSITION}:" | cut -d":" -f2`  
  JOB_HISTORY_COMP_PATH=`echo ${JOB_HISTORY_COMP_PATH} | sed +'s/<\/value>//g' | sed +'s/<value>//g'`
}


#########################################
## Copy the job history files to local ##
#########################################
function copy_job_history_to_local() {
  echo "Copying job histroy information to local from DFS..." >> ${LOG_FILE} 2>&1
  JOB_HISTORY_PATH=$HIT_HDFS_STORAGE_DIR/output/_logs/history
  JOB_HISTORY_LOCAL_PATH=/tmp/gridmix_mapreduce/output/_logs/history
  if [ -d ${JOB_HISTORY_LOCAL_PATH} ]
  then
    rm -rf /tmp/gridmix_mapreduce
  fi 
  mkdir -p /tmp/gridmix_mapreduce/output/_logs/history
  ${HADOOP_COMMON_HOME}/bin/hadoop fs -ls ${JOB_HISTORY_PATH} >> ${LOG_FILE} 2>&1
  if [ $? -eq 0 ]
  then
     ${HADOOP_COMMON_HOME}/bin/hadoop fs -copyToLocal ${JOB_HISTORY_PATH}/* ${JOB_HISTORY_LOCAL_PATH} >> ${LOG_FILE} 2>&1
     echo "Job history files has been copied successfully." >> ${LOG_FILE} 2>&1
  else
     JobID="job`cat ${LOG_FILE} | grep -e 'Job complete:' | sed s/': job'/^/g | cut -d'^' -f2`"
     get_job_history_path
     JOB_HISTORY_PATH="${JOB_HISTORY_COMP_PATH}/*/*/*/*/*/*/*${JobID}*"
     ${HADOOP_COMMON_HOME}/bin/hadoop fs -ls "${JOB_HISTORY_PATH}.xml" >> ${LOG_FILE} 2>&1
     if [ $? -ne 0 ]
     then
       logResults "Job history path is not available in DFS" 1 1 0
       exit $HIT_FAIL
     fi
     
     ${HADOOP_COMMON_HOME}/bin/hadoop fs -copyToLocal ${JOB_HISTORY_PATH} ${JOB_HISTORY_LOCAL_PATH} >> ${LOG_FILE} 2>&1
     if [ $? -eq 0 ]
     then
        echo "Job history files has been copied successfully." >> ${LOG_FILE} 2>&1
     else   
        logResults "Failed to copy the files." 1 1 0
        exit $HIT_FAIL
     fi   
  fi

}

#################################
## Run the Rumen for json file ##
#################################
function run_rumen_for_json() {

 copy_job_history_to_local

 JOB_TRACE_JASON_FILE=/tmp/wordcount.json
 TOPOLOGY_FILE=/tmp/wordcount_topology.json
 JOB_LOG_HISTORY="${JOB_HISTORY_LOCAL_PATH}" 
 JDK_TOOLS_JAR=${JAVA_HOME}/lib/tools.jar
 PATH_SEP=":"
 
 # Get the core and tools jars.
 JARS=`ls ${HADOOP_COMMON_HOME}/*.jar | grep -e core -e tools`
 for jarfile in ${JARS}
 do
   HADOOP_JARS=${HADOOP_JARS}${jarfile}${PATH_SEP}
 done
 
 if [ -z "${HADOOP_JARS}" ]
 then
     logResults "ERROR: Hadoop core and tools jars are not found." 1 0 1
     exit $HIT_FAIL;
 fi
 
 # Get the libraries jars.
 JARS=`ls ${HADOOP_COMMON_HOME}/lib/*.jar`
 for libjarfile in ${JARS} 
 do
   HADOOP_LIB_JARS=${HADOOP_LIB_JARS}${libjarfile}${PATH_SEP}
 done
 
 if [ -z "${HADOOP_LIB_JARS}" ]
 then
    logResults "ERROR: Hadoop library jars are not found." 1 0 1
    exit $HIT_FAIL
 fi

 # Execute the Rumen by passing the job history file.
 ${JAVA_HOME}/bin/java -classpath ${HADOOP_COMMON_HOME}${PATH_SEP}\
${HADOOP_CONF_DIR}${PATH_SEP}\
${JDK_TOOLS_JAR}${PATH_SEP}\
${HADOOP_JARS}\
${HADOOP_LIB_JARS} \
 -Djava.library.path=${HADOOP_COMMON_HOME}/lib/native/Linux-i386-32 \
 org.apache.hadoop.tools.rumen.HadoopLogsAnalyzer \
 -write-job-trace ${JOB_TRACE_JASON_FILE} \
 -write-topology  ${TOPOLOGY_FILE} \
 -v1 "file:///${JOB_LOG_HISTORY}" >> ${LOG_FILE} 2>&1
 if [ $? -eq 0 ]
 then
   echo "Successfully generated the json file..." >> ${LOG_FILE} 2>&1
   echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: JSON file generation status : PASSED." >> ${RESULTS_FILE} 2>&1
 else
   logResults "Failed to generate the json file. JSON file generation status : FAILED" 1 1 0
   exit $HIT_FAIL
 fi

 # Copy the JSON file to local file system from DFS.
 LOCAL_PATH=/tmp/json
 if [ -d ${LOCAL_PATH} ]
 then
    rm -rf ${LOCAL_PATH}
 fi
 mkdir ${LOCAL_PATH}
 
 echo "Copying the json files to local file systems.." >> ${LOG_FILE} 2>&1
 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} \
 fs -copyToLocal /tmp/*.json ${LOCAL_PATH} >> ${LOG_FILE} 2>&1
 if [ $? = 0 ]
 then
    echo "File has beeen copied." >>  ${LOG_FILE} 2>&1
 else
    logResults "File has not been copied." 1 1 0
    exit $HIT_FAIL
 fi

 echo "Cleaning the json files in DFS..." >> ${LOG_FILE} 2>&1
 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} \
 fs -rmr /tmp/*.json >> ${LOG_FILE} 2>&1 
 if [ $? = 0 ]
 then
    echo "Files has been cleaned up successfully." >>  ${LOG_FILE} 2>&1
 else
    echo "Failed to cleaned up." >>  ${LOG_FILE} 2>&1
 fi

}

#########################
## Common usage script ##
#########################
function common_usage() {
  JARS=`ls ${HADOOP_COMMON_HOME}/*.jar | grep -e tools`
  for tooljar in ${JARS} 
  do 
      if [ ! -z "${tooljar}" ]
      then
         HADOOP_TOOLS_JAR=${tooljar}
         break
      fi 
  done

  if [ -z "${HADOOP_TOOLS_JAR}" ]
  then
     logResults "ERROR: Hadoop tools jar is not found." 1 0 1
     exit $HIT_FAIL
  fi

  JARS=`ls ${HADOOP_COMMON_HOME}/*.jar | grep -e gridmix` 
  for gridmixjar in ${JARS} 
  do
      if [ ! -z "${gridmixjar}" ]
      then
         HADOOP_GRIDMIX_JAR=${gridmixjar}
         break
      fi
  done
 
  
  if [ -z "${HADOOP_GRIDMIX_JAR}" ]
  then
       logResults "ERROR: Hadoop gridmix jar is not found." 1 0 1
       exit $HIT_FAIL
  fi
  
  HADOOP_CLASSPATH=${HADOOP_TOOLS_JAR}${PATH_SEP}${HADOOP_GRIDMIX_JAR}
  export HADOOP_CLASSPATH
}

#################################################
## Check and build the folder structure in DFS ##
#################################################
function check_and_build_folder_struct () {

 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls /mapred >> ${LOG_FILE} 2>&1
 if [ $? -ne 0 ]
 then
    ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir  /mapred >> ${LOG_FILE} 2>&1   
 fi
 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls /mapred/history >> ${LOG_FILE} 2>&1
 if [ $? -ne 0 ]
 then
   ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir  /mapred/history >> ${LOG_FILE} 2>&1
   ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chmod -R 755 /mapred >> ${LOG_FILE} 2>&1
 fi

 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls /mapredsystem >> ${LOG_FILE} 2>&1   
 if [ $? -ne 0 ]
 then
   ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir /mapredsystem >> ${LOG_FILE} 2>&1
 fi
 ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls /mapredsystem/hadoop >> ${LOG_FILE} 2>&1
 if [ $? -ne 0 ]
 then
   ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir  /mapredsystem/hadoop >> ${LOG_FILE} 2>&1
   ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -chmod -R 755 /mapredsystem >>  ${LOG_FILE} 2>&1
 fi
}


###################################
## Generate the 2 GB data in DFS ##
###################################
function generate_data() {

  common_usage
  check_and_build_folder_struct 
  ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls \
                    ${HIT_HDFS_STORAGE_DIR}/gridmix >> ${LOG_FILE} 2>&1
  if [ $? -eq 0 ]
  then
    echo "Deleting input folder under gridmix in HDFS..." >> ${LOG_FILE} 2>&1
    ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -rmr \
                       ${HIT_HDFS_STORAGE_DIR}/gridmix/input >> ${LOG_FILE} 2>&1
    if [ $? -eq 0 ]
    then
      echo "input directory has been deleted." >>  ${LOG_FILE} 2>&1
    else
      echo "input directory not found." >>  ${LOG_FILE} 2>&1
    fi

    echo "Deleting output folder under gridmix in HDFS..." >> ${LOG_FILE} 2>&1
    ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -rmr \
                       ${HIT_HDFS_STORAGE_DIR}/gridmix/output >> ${LOG_FILE} 2>&1
    if [ $? -eq 0 ]
    then
      echo "output directory has been deleted." >>  ${LOG_FILE} 2>&1
    else
      echo "output directory not found." >>  ${LOG_FILE} 2>&1
    fi
  else
    ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -mkdir \
                       ${HIT_HDFS_STORAGE_DIR}/gridmix >> ${LOG_FILE} 2>&1
  fi

  echo "Invoking the data generation script..." >> ${LOG_FILE} 2>&1
  ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} \
  org.apache.hadoop.mapred.gridmix.Gridmix \
  -D log4j.logger.org.apache.hadoop.mapred.gridmix=DEBUG \
  -D gridmix.output.directory=$HIT_HDFS_STORAGE_DIR/gridmix/output \
  -D gridmix.job-submission.use-queue-in-trace=true \
  -D gridmix.job.type=LOADJOB \
  -generate 2048m \
   $HIT_HDFS_STORAGE_DIR/gridmix/input 'file:///dev/null' >> ${LOG_FILE} 2>&1
   
  if [ $? -eq 0 ]
  then
    echo "2 GB input data has been generated successfully." >>  ${LOG_FILE} 2>&1
    echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: GRIDMIX INPUT GENERATION STATUS: PASSED" >> ${RESULTS_FILE}
  else
    logResults "Failed to generate the input data. GRIDMIX INPUT GENERATION STATUS: FAILED" 1 1 0
    exit $HIT_FAIL
  fi
}

######################################
## Run GridMix with given json file ##
######################################
function run_gridmix() {

  common_usage
  ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} fs -ls \
                     $HIT_HDFS_STORAGE_DIR/gridmix/input >> ${LOG_FILE} 2>&1
  if [ $? -ne 0 ]
  then
    logResults "GridMix input directory not found. GRIDMIX JOB STATUS: FAILED" 1 1 0
    exit $HIT_FAIL
  fi

  ${HADOOP_COMMON_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} \
  org.apache.hadoop.mapred.gridmix.Gridmix \
  -D log4j.logger.org.apache.hadoop.mapred.gridmix=DEBUG \
  -D gridmix.output.directory=$HIT_HDFS_STORAGE_DIR/gridmix/output \
  -D gridmix.client.pending.queue.depth=10 \
  -D gridmix.job-submission.policy=STRESS \
  -D gridmix.job.type=LOADJOB \
   $HIT_HDFS_STORAGE_DIR/gridmix/input 'file:///tmp/json/wordcount.json' >> ${LOG_FILE} 2>&1

  if [ $? -eq 0 ]
  then
    logResults "Info GridMix3: GRIDMIX JOB STATUS: PASSED" 1 0 0
  else
    logResults "Info GridMix3: GRIDMIX JOB STATUS: FAILED" 1 1 0
    exit $HIT_FAIL
  fi
}

########################
## Directory clean up ##
########################
function cleanup() {
 echo "Cleaning up the data in both local and HDFS...." >> ${LOG_FILE}
 rm -rf /tmp/json
 rm -rf /tmp/gridmix_mapreduce
 rm -rf /tmp/data.txt
 ${HADOOP_COMMON_HOME}/bin/hadoop fs -rmr $HIT_HDFS_STORAGE_DIR
}

##########
## Main ##
##########
date_and_timestamp=`date +'%m-%d-%Y-%H%M%S'`

# The following variables are expected to be set by HIT framework.
# However, some reasonable defaults have to be present
if [ "x$HIT_EXEC_ID" = "x" ]; then
  export HIT_EXEC_ID=hadoop-gridmix-${date_and_timestamp}
else
  export HIT_EXEC_ID=hadoop-gridmix-${HIT_EXEC_ID}
fi

if [ "x$HIT_HDFS_STORAGE_DIR" = "x" ]; then
  export HIT_HDFS_STORAGE_DIR=/user/$USER/${HIT_EXEC_ID}/
fi

kinit -kt ~/hadoopqa.dev.headless.keytab hadoopqa 2> /dev/null
parseCmd $*
LOG_FILE="${LOGS_DIR}/${HIT_EXEC_ID}.log"
RESULTS_FILE="${RESULTS_DIR}/hadoop-gridmix-result-${date_and_timestamp}.txt"

echo "Running Hadoop GirdMix tests..."
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Start validating the environment..." >> ${RESULTS_FILE}
validate_env
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Done." >> ${RESULTS_FILE}

echo "Running the mapreduce job"
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Start running the mapreduce job..." >> ${RESULTS_FILE}
run_mapreduce_job
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Done." >> ${RESULTS_FILE}

echo "Generating the json file"
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Start the Rumen for generating the json..." >> ${RESULTS_FILE}
run_rumen_for_json
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Done." >> ${RESULTS_FILE}

echo "Generating the input data for GridMix3"
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Start generating the 1GB input data for gridmix..." >> ${RESULTS_FILE}
generate_data
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Done." >> ${RESULTS_FILE}

echo "Running the GridMix3 job"
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Start running the gridmix..." >> ${RESULTS_FILE}
run_gridmix
echo "`date +'%m/%d/%Y %H:%M:%S'` Info GridMix3: Done." >> ${RESULTS_FILE}
cleanup
echo "Done."

exit $HIT_PASS

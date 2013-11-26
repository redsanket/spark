#!/bin/sh

# CLUSTER and GATEWAY from driver.sh 

. $WORKSPACE/lib/library.sh

# Setting Owner of this TestSuite
OWNER="Hadoop QE"
SCANMUS_CMD=/home/y/bin/scanmus
GATEWAY=`hostname`

# TEMPLATE=/home//y//var/yinst/packages/scanmus-1.5.3_2/templates/AlexandriaNNTemplate.txt
TEMPLATE=${WORKSPACE}/tests/Scanmus/HDFS/NNScanmus/${CLUSTER}NNTemplate.txt
TEMPLATE=`ssh $GATEWAY echo $TEMPLATE`

echo "NN SCANMUS TEMPLATE"
ssh $GATEWAY "cat $TEMPLATE"

echo " "
echo "SCANMUS COMMAND: $SCANMUS_CMD -d 7 -l 7--socks socks.yahoo.com:1080 template:$TEMPLATE"
ssh $GATEWAY "$SCANMUS_CMD -d 7 -l 7--socks socks.yahoo.com:1080 template:$TEMPLATE" | tee $ARTIFACTS_FILE

echo " "
echo "OUTPUT OF SCANMUS LOGGED IN THE LOG FILE"
LOG_FILE_NAME=`cat $ARTIFACTS_FILE | grep "log saved to" | cut -f 7 -d ' ' `
ssh $GATEWAY "cat $LOG_FILE_NAME"

ssh $GATEWAY "cat $LOG_FILE_NAME" | grep "Scanning completed at" > /dev/null
COMMAND_EXIT_CODE=$?
TESTCASENAME="NNScanmus"
displayTestCaseResult

(( SCRIPT_EXIT_CODE = $SCRIPT_EXIT_CODE + $COMMAND_EXIT_CODE ))

echo " "
echo "SCRIPT_EXIT_CODE=$SCRIPT_EXIT_CODE"
exit ${SCRIPT_EXIT_CODE}


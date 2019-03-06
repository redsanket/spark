#!/bin/sh
export JAVA_HOME=$GSHOME/java/jdk64/current

[ -z "$HADOOP_CONF_DIR" ] && export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
[ -z "$HDFSUSER" ] && export HDFSUSER=hdfs

echo "Running $0 -- HDFSUSER=$HDFSUSER"

if [ `whoami` != $HDFSUSER ]
then
	echo "failure: need to run $0 as $HDFSUSER." 1>&2
	exit 2
fi

# secondary namenode doesnt require erasing nor janitorial services.
case $1 in
    start+erase | startonly)
       export ERASEENABLED=false
       CMD=start
       # echo "starting and not erasing"
       ;;
   stop)
       # echo "stopping, only."
       CMD=stop
       ;;
   *)
       echo "unknown option to $0."
       exit 1
       ;;
esac

# echo "Part 3: beginning."
if [ $CMD == "start" ]; then
    secondarynamenode=`hostname`
    shortname=`expr  $secondarynamenode : '(' '\([^\.]*\)\..*$' ')'`
    # echo name=$secondarynamenode shortname=$shortname
    # echo '***** NEED TO RUN' kinit to deal with keytab on ${secondarynamenode}
    ktabfile1=/etc/grid-keytabs/${shortname}.dev.service.keytab
    ktabfile2=/etc/grid-keytabs/hdfs.dev.service.keytab
    if [ -f "$ktabfile1" ]; then
        ktabfile=$ktabfile1
        princ=hdfs/${secondarynamenode}@DEV.YGRID.YAHOO.COM
    elif [ -f "$ktabfile2" ]; then
        ktabfile=$ktabfile2
        princ=hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
    else
        echo "ERROR: no valid keytab file found!!!"
        echo "$ktabfile1"
        echo "$ktabfile2"
        exit 1;
    fi
    export PATH=/usr/kerberos/bin:$PATH
    case $HDFSUSER in
      hdfsqa|hadoop[0123456789]|hdfs)
	    echo "kinit -kt $ktabfile $princ"
	    kinit -kt $ktabfile $princ
            ;;
        *)
	    echo "Do not recognize HDFSUSER $HDFSUSER -- cannot run kinit!!!"
            exit 1;
	;;
    esac
    klist
elif [ $CMD == "stop" ]; then 
    echo "Part 3: the stop is part of part 2; there is no part 3 for stop.."
else
    echo "Usage: namenodescript.sh [startonly|stop|start+erase]"
fi
# echo "Part 3: done."

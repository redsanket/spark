hostname=gwbl2003

verbose=1	
cluster=
readonly=true

while getopts N:c:g:P:u:vw o
do	case "$o" in
	g|grid|cluster|c)	cluster="$OPTARG";;
	N)	NAMENODELIST=`echo $OPTARG | tr ','  ' ' ` ;;
	w)	readonly=false;;
	v)	verbose=1;;
        P)	processorType=$OPTARG;;
        u)      MAPREDUSER=$OPTARG;;

	[?])	print >&2 "Usage: $0 [-v] [-c cluster] file ..."
		exit 1;;
	esac
done
  
	echo "processor type $processorType  (readonly=$readonly)"

shift `expr $OPTIND - 1`

set -x
echo namenodelist = $NAMENODELIST

export HADOOP_HDFS_HOME=${yroothome}/share/hadoop
export HADOOP_CONF_DIR=${yroothome}/conf/hadoop
export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop
export JAVA_HOME=$GSHOME/java/jdk32/current
export YARN_HOME=${yroothome}/share/hadoop
export yarnparms="-Dmapreduce.job.user.name=$MAPREDUSER -Dmapreduce.framework.name=yarn "

cd ${yroothome}
if [ -e share/hadoop-current ]
then
	export HADOOP_COMMON_HOME=${yroothome}/share/hadoop-current
        export HADOOP_PREFIX=${yroothome}/share/hadoop
else
	export HADOOP_COMMON_HOME=${yroothome}/share/hadoop
        export HADOOP_PREFIX=${yroothome}/share/hadoop
fi
export PATH=/usr/kerberos/bin:$PATH

kinit -k -t $HOMEDIR/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa

if [ "$readonly" = true ]
then
    echo "==== check if namenode is in safe mode."
    checkstatus=`$HADOOP_COMMON_HOME/bin/hdfs dfsadmin -safemode get`
    if [ "$checkstatus" = "Safe mode is OFF" ]
    then
        export readonly=false
    else
        echo "Wait another 10 mins for namenode turning off safe mode"
        sleep 600
        checkstatus=`$HADOOP_COMMON_HOME/bin/hdfs dfsadmin -safemode get`
        if [ "$checkstatus" = "Safe mode is OFF" ]
        then
            export readonly=false
        fi
    fi
fi

$HADOOP_COMMON_HOME/bin/hadoop version  | sed -n 1p
finalstatus=`$HADOOP_COMMON_HOME/bin/hadoop version  | sed -n 1p`
finalstatus="${finalstatus} with $processorType on cluster $cluster"

echo "==== bin/hadoop fs -ls -R /: test that name node is up."
$HADOOP_COMMON_HOME/bin/hadoop fs -ls -R /
export written=0
export read=0
tmpfile=tmp-`date +%y%m%d%H%M`
for n in $NAMENODELIST
do
   if [ "$readonly" = false ]
   then
        fname=hdfs://${n}:8020/user/hadoopqa/rw.$n.$tmpfile.1mb

        echo "==== running randomwriter to ${n}:8020"
        $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    randomwriter -Dmapreduce.job.queuename=grideng  -D mapreduce.randomwriter.totalbytes=$((128*1024)) $yarnparms  $fname
       [ $? -eq 0 ] && export written=`expr $written + 1`
   fi
done
for n in $NAMENODELIST
do

   if [ "$readonly" = false ]
   then
       fname=hdfs://${n}:8020/user/hadoopqa/rw.$n.$tmpfile.1mb

       echo "==== testing JT run to ${n}:8020"
       $debug $HADOOP_COMMON_HOME/bin/hadoop fs -ls -R $fname
       [ $? -eq 0 ] && export read=`expr $read + 1`
   fi
done
if [ "$written" -gt 0  -a "$read" = "$written" ]
then
   finalstatus="${finalstatus} $read namenode(s) up, data nodes up"
fi

set $NAMENODELIST
inNN=$1
shift
outNN=$1
if [ -z "$outNN" ]
then
    outNN=$inNN
fi

st=0
if [ "$readonly" = false ]
then
    datadir=hdfs://${inNN}:8020/user/hadoopqa/indir-`date +%y%m%d%H%M`
    outdir=hdfs://${outNN}:8020/user/hadoopqa/outdir-`date +%y%m%d%H%M`
    tmpfile=tmp-`date +%y%m%d%H%M`

    echo ==== running teragen to $datadir
    echo " $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    teragen $yarnparms  -Dmapreduce.job.queuename=grideng  100000 $datadir"
    $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    teragen $yarnparms  -Dmapreduce.job.queuename=grideng  100000 $datadir

    if [ $? -eq 0 ]
    then
        finalstatus="${finalstatus}, teragen run"
    else
        finalstatus="${finalstatus}, teragen failed"
        st=1
    fi

    echo === hadoop fs -ls -R $datadir
    $HADOOP_COMMON_HOME/bin/hadoop fs -ls $datadir

    echo ==== running terasort from $datadir to $outdir

    echo "$HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    terasort -Dmapreduce.job.queuename=grideng -Dmapred.reduce.child.java.opts="-Xmx1536m -Djava.net.preferIPv4Stack=true -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/grid/0/tmp/" $yarnparms  -Dmapreduce.jobtracker.split.metainfo.maxsize=50000 -Dmapred.compress.map.output=true $datadir  $outdir "
    $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    terasort -Dmapreduce.job.queuename=grideng -Dmapred.reduce.child.java.opts="-Xmx1536m -Djava.net.preferIPv4Stack=true -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/grid/0/tmp/" $yarnparms  -Dmapreduce.jobtracker.split.metainfo.maxsize=50000 -Dmapred.compress.map.output=true $datadir  $outdir

    if [ $? -eq 0 ]
    then
        finalstatus="${finalstatus}, terasort run"
    else
        finalstatus="${finalstatus}, terasort failed"
        st=1
    fi

    echo === hadoop fs -ls -R $outdir
    $HADOOP_COMMON_HOME/bin/hadoop fs -ls $outdir

# note that a basic MR-to-each-namenode is run in 260-installsteps-testJT.sh

    echo === bin/hadoop jar hadoop-mapreduce-examples-*.jar   randomwriter ....
    echo "$HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    randomwriter -Dmapreduce.job.queuename=grideng  $yarnparms  -D mapreduce.randomwriter.totalbytes=$((1024*1024)) rw1-$tmpfile.1mb"
    $HADOOP_COMMON_HOME/bin/hadoop jar $HADOOP_MAPRED_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar    randomwriter -Dmapreduce.job.queuename=grideng  $yarnparms  -D mapreduce.randomwriter.totalbytes=$((1024*1024)) rw1-$tmpfile.1mb

    if [ $? -eq 0 ]
    then
        finalstatus="${finalstatus}, randomwriter run"
    else
        finalstatus="${finalstatus}, randomwriter failed"
        st=1
    fi

    $HADOOP_COMMON_HOME/bin/hadoop fs -ls /user/hadoopqa/rw1-$tmpfile.1mb
fi
echo $finalstatus
exit $st

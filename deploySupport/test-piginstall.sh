hostname=gwbl2003

verbose=1	
cluster=
readonly=true

while getopts c:g:n:vw o
do	case "$o" in
	cluster|c)	cluster="$OPTARG";;
	namenode|n)	namenode="$OPTARG";;
	w)	readonly=false;;
	v)	verbose=1;;
	[?])	print >&2 "Usage: $0 [-v] [-c cluster] file ..."
		exit 1;;
	esac
done
shift `expr $OPTIND - 1`

#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#----------------------------
#----------------------------
#----------------------------
#----------------------------
#----------------------------
#
# this is stolen shamelessly from the main3.sh invocation
# of pig, to run a word-count.
#
# In fact, we steal the input data also.
#
runPigWordCount() {
	if [ -L ${yroothome}/tmp/pigversions/current ]
	then
	   export PIG_HOME=${yroothome}/tmp/pigversions/current
	   export PATH=$PIG_HOME/bin:$PATH
	fi
	
	if [ -d /grid/0/tmp/validate.$cluster ]
	then
		rm -rf /grid/0/tmp/validate.$cluster 
	fi
	
	mkdir /tmp/$cluster.$$.validation
	trap "rm -rf /tmp/$cluster.$$.validation" 0
	cd /tmp/$cluster.$$.validation
	yinst fetch  -branch test hadoopvalidation
	tar xzf *.tgz
	
	kinit -k -t $HOMEDIR/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa
	cd share/hadoopvalidation/
	
	export VALIDATION_DIR=`ls -d /tmp/$cluster.$$.validation*/share/hadoopvalidation`
	export DFS_VALIDATION_DIR=/user/`whoami`/`hostname`/validation.`date +%y%m%d%H%M`
	
	### Generate pig word count script.
	echo "
/* Register a custom tokenizer function that will parse tokens */
/* just like the mapreduce framework does.  The function       */
/* enables this grunt script to produce the exact same         */
/* word count output as the mapreduce word count example.      */

register $VALIDATION_DIR/bin/customPigFunction2.jar
a = load '$DFS_VALIDATION_DIR/data/wordCountInput/' using TextLoader();
b = foreach a generate flatten(CustomTOKENIZE2(\$0));
c = group b by \$0;
d = foreach c generate group, COUNT(\$1);
e = order d by \$0 parallel 10;
store e into '$DFS_VALIDATION_DIR/data/pigWordCountOutput/';
" > bin/wordcount-pig

$HADOOP_HOME/bin/hadoop dfs -copyFromLocal $VALIDATION_DIR $DFS_VALIDATION_DIR

if [ -f bin/wordcount-simple ]
then
	if [ -f bin/wordcount-simple-20.102 ]
	then
		rm  -f bin/wordcount-simple
	else
		mv  -f bin/wordcount-simple bin/wordcount-simple-20.102
	fi
	ln bin/wordcount-simple-20.103 bin/wordcount-simple
fi
chmod +x bin/*


echo PIG_HOME=$PIG_HOME
$PIG_HOME/bin/pig --version
export pigversion=`$PIG_HOME/bin/pig --version`
$PIG_HOME/bin/pig -Dmapreduce.job.queuename=grideng -f $VALIDATION_DIR/bin/wordcount-pig 

mkdir -p $VALIDATION_DIR/output/pigWordCountOutput
$HADOOP_HOME/bin/hadoop  dfs -copyToLocal $DFS_VALIDATION_DIR/data/pigWordCountOutput/part-* $VALIDATION_DIR/output/pigWordCountOutput/ 
cat $VALIDATION_DIR/output/pigWordCountOutput/part-* > $VALIDATION_DIR/output/pigWordCountOutput.txt
LINES=`wc -l $VALIDATION_DIR/output/pigWordCountOutput.txt | cut -f1 -d " "`
[[ $LINES -eq 5315 ]]		# hard-coded from the data
  }

#
#
#----------------------------
#----------------------------
#----------------------------
#----------------------------
#----------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------
#------------------------------------------------------

cd ${yroothome}
if [ -e share/hadoop-current ]
then
	cd share/hadoop-current/ && export HADOOP_HOME=`pwd`
	cd ../../conf/hadoop && export HADOOP_CONF_DIR=`pwd`
else
	cd share/hadoop-0.*/ && export HADOOP_HOME=`pwd`
	cd ../../conf/hadoop && export HADOOP_CONF_DIR=`pwd`
fi
if [ -e share/pig ]
then
	export PIG_HOME=${yroothome}/share/pig
fi
kinit -k -t $HOMEDIR/hadoopqa/hadoopqa.dev.headless.keytab hadoopqa

$HADOOP_HOME/bin/hadoop version  | sed -n 1p
finalstatus=`$HADOOP_HOME/bin/hadoop version  | sed -n 1p`
finalstatus="${finalstatus} cluster $cluster"

st=0
if [ "$readonly" = false ]
then
    if [ -e ${yroothome}/share/pig ]
    then
        runPigWordCount
        if [ $? -eq 0 ]
        then
            finalstatus="${finalstatus}, pig-wordcount run ($pigversion)"
        else
            finalstatus="${finalstatus}, pig-wordcount failed ($pigversion)"
	    st=1
        fi
    else
        echo "Pig not installed, so not running pig-wordcount."
    fi
fi
echo $finalstatus
exit $st

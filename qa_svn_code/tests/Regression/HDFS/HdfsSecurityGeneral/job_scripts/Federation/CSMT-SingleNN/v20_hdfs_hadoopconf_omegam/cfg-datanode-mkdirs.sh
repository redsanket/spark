#!/bin/sh

#
umask 077

TARGET=/grid

set -x
export mapred=mapred
export hdfs=hdfs
env|grep -i MAPREDUSER
[ !  -z "$YINST_VAR_TODO_MAPREDUSER" ] &&  export mapred=$YINST_VAR_TODO_MAPREDUSER
[ ! -z "$YINST_VAR_TODO_HDFSUSER" ] &&  export hdfs=$YINST_VAR_TODO_HDFSUSER



if [ -z "$YINST_VAR_TODO_RUNMKDIRS" -o "$YINST_VAR_TODO_RUNMKDIRS" != true ]
then
    (
    echo "Not running $0 to make directories."
    echo "If you want to run mkdir/chown/chmod across your /grid,"
    echo "set \$YINST_VAR_TODO_RUNMKDIRS to 'true' in your environment and rerun this script,"
    echo "or the flag, '-set $YINST_PKGNAME.TODO_RUNMKDIRS=true', when you run 'yinst install'"
    echo "on this configuration package."
    ) | fmt 1>&2
    if [ ! -z "$YINST_ROOT" ]
    then
        echo "(This is not a failure, but it still is not running the script.)" \
    1>&2
    fi

    exit 0
else
    echo "Running $0 to make directories."
fi
if [ -z "$YROOT" ]
then
	if [ -z "$YINST_ROOT" ]
	then
            (
            echo "This script is meant to be run by yinst, as root."
	    echo "If you care to run this yourself, set \$YROOT to the"
	    echo "directory where 'yinst' installs things."
	    [ -d /home/y/. ] && echo "Most likely, that will be YROOT=/home/y."
            ) | fmt 1>&2
            exit 1
        else
	      export YROOT=$YINST_ROOT
        fi
fi

#
#
#
#
if [ -z "$YROOT" ]
then
    echo "Are you sure you didn't run this a post-processed <yroot> directory?
There is a variable in this script that was not expanded." >&2
    exit 2
fi

if [ ! -d "$YROOT/share" ]
then
    echo "\$YROOT/share is a necessary directory. It is missing." >&2
    echo "   (Examined $YROOT/share.)" >&2
    exit 2
fi

hadoopInfo=`yinst list -root $YROOT | egrep 'hadoop-|hadoopinstrumented-'`
case $hadoopInfo in
    hadoopinstrumented-*)
	hadoopName=hadoopinstrumented
	;;
    hadoop-*)
	hadoopName=hadoop
	;;
    *)
        echo "Name of hadoop package not found. Exiting."
	exit 2
	;;
esac
hadoopDateStamp=`expr '(' "$hadoopInfo" : "${hadoopName}\-.*\.\(.*\)" ')'`
hadoopVersion=`expr '(' "$hadoopInfo" : "${hadoopName}\-\(.*\)\..*" ')'`

echo HadoopVersion = $hadoopVersion
echo HadoopDateStamp = $hadoopDateStamp
cd $YROOT/share

if [ -z "$hadoopVersion" ]
then
    echo "It does look like there is a hadoop- product installed here." >&2
    echo "output of 'yinst list hadoop' follows:" >&2
    yinst list -root $YROOT | grep  hadoop >&2
    exit 2
fi

if [ ! -d "$YROOT/share/$hadoopInfo" ]
then
    echo "\$YROOT/share/$hadoopInfo should be there, and it is not." >&2
    echo "   (Examined $YROOT/share.)" >&2
    exit 2
fi

top=${TARGET}/0/gs/${hadoopName}/${hadoopName}-${hadoopVersion}

mkIfNotThere() {
	for d in $*
	do
		if [ !  -d $d ]
		then
			echo Making ${d}.
			(umask 022 && mkdir -p $d)
			ls -ald $d
		fi
	done
}
rmIfThere() {
	for d in $*
	do
		if [ -d $d ]
		then
			echo Removing ${d}.
			rm -rf $d
		fi
	done
}
lnCurrent() {
   (
   umask 022 ; [ -d $1 ] || mkdir -p $1
   cd $1
   rm -f $1/current
   echo ln -s $2 $1/current
   ln -s $2 $1/current
   )
}

mkIfNotThere  ${TARGET}

for i in 0 1 2 3
do
	mkIfNotThere  ${TARGET}/${i}  ${TARGET}/${i}/gs   ${TARGET}/${i}/gs/${hadoopName}
	chmod 755   ${TARGET}/${i}  ${TARGET}/${i}/gs   ${TARGET}/${i}/gs/${hadoopName}  
done

rmIfThere   $top/src/contrib/thriftfs

ln  -s  $YROOT/share/${hadoopInfo}    $top

echo "Making symbolic links"

   
lnCurrent    $TARGET/0/gs/conf  $YROOT/conf/hadoop
jdk64loc=`ls -d $YROOT/share/gridjdk64-* | egrep 'gridjdk64-1.6.[0-9_]+$'`
jdkloc=`ls -d $YROOT/share/gridjdk-* | egrep 'gridjdk-1.6.[0-9_]+$'`

lnCurrent    $TARGET/0/gs/java/jdk32  $jdkloc
lnCurrent    $TARGET/0/gs/java/jdk64  $jdk64loc
ln -f -s $TARGET/0/gs/java/jdk32/current $TARGET/0/gs/java/jdk
if [ -e "$YROOT/share/hadoop-current" ]
then
     lnCurrent    $TARGET/0/gs/hadoop  $YROOT/share/hadoop-current/
else
     lnCurrent    $TARGET/0/gs/hadoop  $YROOT/share/hadoop-*/
fi

# no longer necessary
#chmod 6150  $TARGET/0/gs/hadoop/bin/task-controller


mkdir -p  $TARGET/2/hadoop/var/hdfs/name

chown -R $hdfs  $TARGET/2/hadoop/var/hdfs/name


chmod 755 /grid/0/gs
chmod 755 /grid/0/gs
chmod 755 /grid/0/gs/conf
chmod 755 /grid/0/gs/hadoop


if [ -d /grid/0/hadoop/var/hdfs/data ] 
then
	echo "**** NOT removing  /grid/0/hadoop/var/hdfs/data. (Leaving it alone for now.)"
fi

for i in 0 1 2 3
do
	mkIfNotThere /grid/$i/hadoop/var/hdfs/data
	mkIfNotThere /grid/${i}/tmp
        rm -rf /grid/${i}/tmp/mapred-local
	mkIfNotThere /grid/${i}/tmp/mapred-local
	mkIfNotThere /grid/${i}/hadoop
	mkIfNotThere /grid/${i}/hadoop/var
	mkIfNotThere /grid/${i}/hadoop/var/hdfs
	mkIfNotThere /grid/${i}/hadoop/var/hdfs/name
	mkIfNotThere  /grid/${i}/hadoop/var/hdfs/data
	mkIfNotThere /grid/${i}/hadoop/var/jobstatus
	mkIfNotThere /grid/${i}/hadoop/var/log
	mkIfNotThere  /grid/${i}/hadoop/var/log/hdfs
	mkIfNotThere /grid/${i}/hadoop/var/log/mapred
	mkIfNotThere /grid/${i}/hadoop/var/log/$mapred
	mkIfNotThere /grid/${i}/hadoop/var/log/$hdfs
	mkIfNotThere /grid/${i}/hadoop/var/run
	mkIfNotThere  /grid/${i}/hadoop/var/run/hdfs
	mkIfNotThere /grid/${i}/hadoop/var/run/mapred

	chmod 755 /grid/${i}/hadoop/var/jobstatus
	chmod 755 /grid/${i}/hadoop/var/run
	chmod 755 /grid/${i}/hadoop/var/log
	chmod 755 /grid/${i}/hadoop/var/log/mapred
	chmod 755 /grid/${i}/hadoop/var/log/$mapred
	chmod 755 /grid/${i}/hadoop/var/log/hdfs
	chmod 755 /grid/${i}/hadoop/var/log/$hdfs
	chmod 755 /grid/$i/hadoop/var/hdfs/data
	chmod 755 /grid/$i/hadoop/var/hdfs
	chmod 755 /grid/$i/hadoop/var/hdfs/name
	chmod 755 /grid/$i/hadoop
	chmod 755 /grid/$i/hadoop/var
	chmod 777 /grid/${i}/tmp
	chmod 755 /grid/${i}/tmp/mapred-local
	chmod 700 /grid/$i/hadoop/var/hdfs/data

	chown -R $hdfs /grid/$i/hadoop/var/hdfs/data
	chown $mapred /grid/${i}/hadoop/var/jobstatus
	chown root /grid/${i}/hadoop/var/log
	# chown -R $mapred /grid/${i}/hadoop/var/log/mapred
	chown -R $mapred /grid/${i}/hadoop/var/log/$mapred
	chown -R $mapred /grid/${i}/hadoop/var/log/mapred
	chown  $mapred /grid/${i}/hadoop/var
	chown -R $hdfs /grid/${i}/hadoop/var/log/$hdfs
	chown -R $hdfs /grid/${i}/hadoop/var/log/hdfs
	chown -R $mapred /grid/${i}/hadoop/var/run/$mapred
	chown -R $mapred /grid/${i}/hadoop/var/run/mapred
	chown -R $hdfs /grid/${i}/hadoop/var/run/hdfs
	chown -R $hdfs /grid/${i}/hadoop/var/run/$hdfs
	chown -R $mapred /grid/${i}/tmp/mapred-local
	chgrp -R hadoop /grid/${i}/tmp/mapred-local /grid/${i}/hadoop/var/log/$mapred /grid/${i}/hadoop/var/log/mapred

done

mkIfNotThere /grid/0/gs/conf/local/
chmod 755 /grid/0/gs/conf/local/

chmod 755 /grid/0/gs/java
chmod 755 /grid/0/gs/java/jdk32
chmod 755 /grid/0/gs/java/jdk64

exit 0

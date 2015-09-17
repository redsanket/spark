#!/bin/sh

export mapred=$MAPREDUSER
[ -z "$mapred" ] && export mapred=mapred

find /grid/0/hadoop -user hdfsqa -type d -exec chown hdfs '{}' ';' 
find /grid/0/hadoop -user mapredqa -type d -exec chown mapred '{}' ';' 

if [ -d /grid/0/tmp/mapred-local/taskTracker ]; then
    rm -rf /grid/0/tmp/mapred-local/taskTracker
    mkdir -p /grid/0/tmp/mapred-local/taskTracker
    chmod 777 /grid/0/tmp/mapred-local/taskTracker
fi

if [ -d /grid/0/tmp/yarn-local/taskTracker ]; then
    rm -rf /grid/0/tmp/yarn-local/taskTracker
    mkdir -p /grid/0/tmp/yarn-local/taskTracker
    chmod 777 /grid/0/tmp/yarn-local/taskTracker
fi

# Remove prior history files. Deploy will remake directories with correct
# owner and permissions
if [ -d /home/gs/var/run ]; then
    rm -rf /home/gs/var/run
fi

for i in 0 1 2 3 4 5 6 7 8 9 10 11
do
    if [ -d /grid/${i}/hadoop/var/mapred-local ]; then
        # make sure one data file exists
        touch /grid/${i}/hadoop/var/mapred-local/emptyfile
        find /grid/${i}/hadoop/var/mapred-local/* -prune -exec rm -rf {} \;  > /dev/null 2>&1
    fi
    if [ -d /grid/${i}/tmp/mapred-local ]; then
        # make sure one data file exists
        touch /grid/${i}/tmp/mapred-local/emptyfile
        find /grid/${i}/tmp/mapred-local/* -prune -exec rm -rf {} \;  > /dev/null 2>&1
    fi

    if [ -d /grid/${i}/hadoop/var/yarn-local ]; then
        # make sure one data file exists
        touch /grid/${i}/hadoop/var/yarn-local/emptyfile
        find /grid/${i}/hadoop/var/yarn-local/* -prune -exec rm -rf {} \;  > /dev/null 2>&1
    fi
    if [ -d /grid/${i}/tmp/yarn-local ]; then
        # make sure one data file exists
        touch /grid/${i}/tmp/yarn-local/emptyfile
        find /grid/${i}/tmp/yarn-local/* -prune -exec rm -rf {} \;  > /dev/null 2>&1
    fi
    if [ -d /grid/${i}/tmp/yarn-logs ]; then
        # make sure one data file exists
        touch /grid/${i}/tmp/yarn-logs/emptyfile
        find /grid/${i}/tmp/yarn-logs/* -prune -exec rm -rf {} \;  > /dev/null 2>&1
    fi

    if [ -d /grid/${i}/hadoop/var/log/$mapred/userlogs ]; then
        # make sure one data file exists
        a=/grid/${i}/hadoop/var/log/$mapred/userlogs
	[ -d "$a.previous" ] && rm -rf $a.previous
	mv $a $a.previous
	mkdir -p $a  && chown $mapred $a
    fi
    if [ -d /grid/${i}/hadoop/var/hdfs/name ]; then
        # make sure one data file exists
        mv /grid/${i}/hadoop/var/hdfs/name /grid/${i}/hadoop/var/hdfs/name.dead
        find /grid/${i}/hadoop/var/hdfs/name.dead -prune -exec rm -rf {} \;  > /dev/null 2>&1 &
    fi
    if [ -d /grid/${i}/hadoop/var/hdfs/oiv_images ]; then
        mv /grid/${i}/hadoop/var/hdfs/oiv_images /grid/${i}/hadoop/var/hdfs/oiv_images.dead
        find /grid/${i}/hadoop/var/hdfs/oiv_images.dead -prune -exec rm -rf {} \;  > /dev/null 2>&1 &
    fi
    mkdir -p /grid/${i}/hadoop/var/hdfs/oiv_images
    chmod 700 /grid/${i}/hadoop/var/hdfs/oiv_images
    chown ${HDFSUSER} /grid/${i}/hadoop/var/hdfs/oiv_images
    if [ -d /grid/${i}/hadoop/var/hdfs/data ]; then
        # make sure one data file exists
        mv /grid/${i}/hadoop/var/hdfs/data /grid/${i}/hadoop/var/hdfs/data.dead
        find /grid/${i}/hadoop/var/hdfs/data.dead -prune -exec rm -rf {} \;  > /dev/null 2>&1 &
    fi
    mkdir -p /grid/${i}/hadoop/var/hdfs/data
    chmod 700 /grid/${i}/hadoop/var/hdfs/data
    chown ${HDFSUSER} /grid/${i}/hadoop/var/hdfs/data
done

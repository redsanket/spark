set +x
if [ "$RUNSIMPLETEST" = false -o -z "$RUNSIMPLETEST" ]; then
	echo "Running no tests for ${cluster}."
else
    echo "Running tests for ${cluster}."
    if [ "$RUNSIMPLETEST" = true ] ; then
	testfile="$base/shorttest-rw.sh"
	testname=shorttest-rw.sh
    elif [ -f "$RUNSIMPLETEST" ] ; then
	testfile=${RUNSIMPLETEST}
	testname=${RUNSIMPLETEST}
    elif [ -f "$base/$RUNSIMPLETEST" ] ; then
	testfile="$base/$RUNSIMPLETEST"
	testname=${RUNSIMPLETEST}
    fi

    export yarnProcessor=yarn
    # [ -n  "$PREFERREDJOBPROCESSOR" ] && export yarnProcessor=$PREFERREDJOBPROCESSOR
    if [ "$REMOVEEXISTINGDATA" = true ]; then
        writeenabledflag=-w
    else
        writeenabledflag=
        sleep 180
    fi

    # Run $cluster-shorttest-rw.sh from the gateway, passing the primary namenode value.
    echo "=== running a simple test on  gateway=$gateway"
    set -x
    scp "$testfile"  ${gateway}:/tmp/${cluster}-${testname}
    set +x

    logfile="deploy_${cluster}_test.log"
    set -x
    # echo  GSHOME=$GSHOME yroothome=$yroothome sh /tmp/${cluster}-${testname} -c $cluster -N "'$namenode'" ${writeenabledflag} -P "${yarnProcessor}" -u "${MAPREDUSER}" | ssh $gateway su - hadoopqa
    ssh $gateway "su - hadoopqa -c '\
export GSHOME=$GSHOME && \
export yroothome=$yroothome && \
export HADOOP_27=$HADOOP_27 && \
export HOMEDIR=$HOMEDIR && \
sh /tmp/${cluster}-${testname} \
-c $cluster -N $namenode ${writeenabledflag} -P ${yarnProcessor} -u ${MAPREDUSER} \
> /tmp/$logfile 2>&1 \
'"
    RC=$?
    set +x

    set -x
    scp ${gateway}:/tmp/$logfile /grid/0/tmp/scripts.deploy.$cluster/$logfile
    set +x

    if [ "$RC" -ne 0 ]; then
        echo "500-installsteps-runsimpletest.sh: deployment verification test failed! exiting...."
        exit 1
    fi
fi

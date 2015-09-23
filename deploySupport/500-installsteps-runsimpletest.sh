if [ "$RUNSIMPLETEST" = false -o -z "$RUNSIMPLETEST" ]
then
	echo Running no tests for ${cluster}.
else
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
#  [ -n  "$PREFERREDJOBPROCESSOR" ] && export yarnProcessor=$PREFERREDJOBPROCESSOR
   if [ "$REMOVEEXISTINGDATA" = true ]
   then
      writeenabledflag=-w
   else
      writeenabledflag=
      sleep 180
   fi

   # Run $cluster-shorttest-rw.sh from the gateway, passing the primary namenode value.
   echo === running a simple test on  gateway=$gateway
   set +x
   scp "$testfile"  ${gateway}:/tmp/${cluster}-${testname}
   set -x

   # echo  GSHOME=$GSHOME yroothome=$yroothome sh /tmp/${cluster}-${testname} -c $cluster -N "'$namenode'" ${writeenabledflag} -P "${yarnProcessor}" -u "${MAPREDUSER}" | ssh $gateway su - hadoopqa
   ssh $gateway "su - hadoopqa -c '\
export GSHOME=$GSHOME && \
export yroothome=$yroothome && \
export HOMEDIR=$HOMEDIR && \
sh /tmp/${cluster}-${testname} \
-c $cluster -N $namenode ${writeenabledflag} -P ${yarnProcessor} -u ${MAPREDUSER} \
'"

   if [ "$?" -ne 0 ]; then
       echo "500-installsteps-runsimpletest.sh: deployment verification test failed! exiting...."
       exit 1
   fi
fi

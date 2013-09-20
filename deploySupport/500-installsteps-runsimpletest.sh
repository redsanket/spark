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


   echo === running a simple test on  gateway=$gateway
   scp "$testfile"  ${gateway}:/tmp/${cluster}-${testname}
   
   export yarnProcessor=yarn
#  [ -n  "$PREFERREDJOBPROCESSOR" ] && export yarnProcessor=$PREFERREDJOBPROCESSOR
   if [ "$REMOVEEXISTINGDATA" = true ]
   then
      writeenabledflag=-w
   else
      writeenabledflag=
      sleep 180
   fi

   echo  GSHOME=$GSHOME yroothome=$yroothome sh /tmp/${cluster}-${testname} -c $cluster -N "'$namenode'" ${writeenabledflag} -P "${yarnProcessor}" -u "${MAPREDUSER}" | ssh $gateway su - hadoopqa

   if [ "$?" -ne 0 ]; then
       echo "deployment veirification test failed! exiting...."
       exit 1
   fi
fi

set +x
if [ "$CHECKSSHCONNECTIVITY" != true ]; then
    echo "CHECKSSHCONNECTIVITY is not enabled. Nothing to do."
    return 0
fi

echo "CHECKSSHCONNECTIVITY is enabled"
for h1 in $jobtrackernode; do
    echo == checking that ssh to $h1 works.
    if [ "`echo date +%Y | ssh $jobtrackernode su - $MAPREDUSER `" != "$yr" ]; then
        echo "Cannot run command on $h1. Is ssh set up for user $USER?"
        errs=`expr $errs + 1`
    fi
    for h2 in $ALLSLAVES; do
        echo "==== testing $h1 -> $h2 for user=$MAPREDUSER."
        if [ "`echo date +%Y | ssh $jobtrackernode su - $MAPREDUSER ssh $h2 `" != "$yr" ]; then
        echo "Cannot run command on $h1 to $h2. Is ssh set up for user $MAPREDUSER?"
        errs=`expr $errs + 1`
        fi
    done
done

if [ "$errs" -ne 0 ]; then
    echo "$errs errors found for ssh connectivity. Fix and rerun."
    echo "Otherwise, the job-tracker will not be able to start task-trackers."
    exit 2
else
    echo "Pass: verified that ssh(1) connectivity works for JT->TT as user=$MAPREDUSER."
fi

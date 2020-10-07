#
# "run kinit on each namenode before starting it."
#
#
# (1) For some reason, we have to run this kinit for the machine-level
#     access to run a namenode.
#
# Inputs: $RUNKINIT	(boolean)
# Inputs: $namenode	(set in cluster-list.sh)
# Inputs: $HDFSUSER
#
set +x
if [ "$RUNKINIT" != true ]; then
    echo "RUNKINIT is not enabled. Nothing to do."
    return 0
fi

# /etc/grid-keytabs/<cluster shortname>.dev.service.keytab needs to be readable
# to hdfsqa
for node in $namenode; do
    shortname=`expr  $node : '(' '\([^\.]*\)\..*$' ')'`
    echo "name=$node shortname=$shortname"
    ktabfile=/etc/grid-keytabs/${shortname}.dev.service.keytab

(
echo 'export PATH=/usr/kerberos/bin:$PATH'
echo echo ======= NEED TO RUN kinit to deal with keytab on ${node} as ${HDFSUSER}
echo "if [  -f $ktabfile ]; then"
echo echo kinit -k -t /etc/grid-keytabs/${shortname}.dev.service.keytab hdfs/${node}@DEV.YGRID.YAHOO.COM
echo kinit -k -t /etc/grid-keytabs/${shortname}.dev.service.keytab hdfs/${node}@DEV.YGRID.YAHOO.COM
echo else
echo echo kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
echo kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
echo fi
echo klist
) > $scriptdir/$cluster.nninit.sh

    set -x
    $SCP $scriptdir/$cluster.nninit.sh $node:/tmp/$cluster.nninit.sh
    $SSH $node "sudo -su $HDFSUSER bash -c \"sh /tmp/$cluster.nninit.sh\""
    set +x
done

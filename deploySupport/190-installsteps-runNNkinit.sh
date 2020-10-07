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

for n in $namenode; do
    shortname=`expr  $n : '(' '\([^\.]*\)\..*$' ')'`
    echo name=$n shortname=$shortname
    ktabfile=/etc/grid-keytabs/${shortname}.dev.service.keytab
    set -x
    (
    echo 'export PATH=/usr/kerberos/bin:$PATH'
    echo echo ======= NEED TO RUN kinit to deal with keytab on ${n} as ${HDFSUSER}
    echo "if [  -f $ktabfile ] "
    echo "then "
        echo kinit -k -t /etc/grid-keytabs/${shortname}.dev.service.keytab hdfs/${n}@DEV.YGRID.YAHOO.COM
    echo else
        echo kinit -k -t /etc/grid-keytabs/hdfs.dev.service.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
    echo fi 
    #echo klist
    )| $SSH $n su - $HDFSUSER
    set +x
done


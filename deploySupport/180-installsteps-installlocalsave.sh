set +x
if [ "$INSTALLLOCALSAVE" != true ]; then
    echo "INSTALLLOCALSAVE is not enabled. Nothing to do."
    return 0
fi

set -x
# /home/gs/conf/local/slaves
# /home/gs/gridre/yroot.openphil1blue/conf/hadoop/slaves.localcopy.txt
cp $WORK_DIR/slaves.$cluster.txt $scripttmp/slaves.$cluster.txt
fanoutscp "$scripttmp/slaves.$cluster.txt" "${GSHOME}/conf/local/slaves" "$HOSTLIST"
fanoutscp "$scripttmp/slaves.$cluster.txt" "${GSHOME}/gridre/yroot.$cluster/conf/hadoop/slaves.localcopy.txt" "$HOSTLIST"
set +x

(
echo "cd /tmp"
echo "export GSHOME=${GSHOME} "
echo 'mkdir /tmp/$$ && cd /tmp/$$ '
echo "/usr/local/bin/yinst fetch  $LOCAL_CONFIG_INSTALL_STRING -br quarantine "
echo "tar xzf $LOCAL_CONFIG_PKG_NAME-*.tgz"
echo "cd share/localhadoopconfigs"
echo "[ -d ${GSHOME}/conf/local ] || mkdir -p ${GSHOME}/conf/local "
echo "for file in *.xml; do"
echo '    if [ '!' -f ${GSHOME}/conf/local/${file} ]; then '
echo '        cp $file ${GSHOME}/conf/local/${file}'
echo '    fi '
echo "done"
echo "for file in dfs.*; do"
echo '    if [ '!' -f ${GSHOME}/conf/local/${file} ]; then '
echo '        cp $file ${GSHOME}/conf/local/${file}'
echo '    fi '
echo "done"
echo 'cd /tmp && rm -rf /tmp/$$'
) > $scripttmp/$cluster.cplocalfiles.sh

fanoutscp "$scripttmp/$cluster.cplocalfiles.sh" "/tmp/$cluster.cplocalfiles.sh" "$HOSTLIST"
cmd="sh -x /tmp/$cluster.cplocalfiles.sh"
set -x
fanout "$cmd"
fanoutGW "$cmd"
set +x

if [ -n "$secondarynamenode" ]; then
    cmd="echo \"$secondarynamenode\" > ${GSHOME}/conf/local/masters"
    set -x
    for i in $namenode; do
	ssh $i "sudo bash -c \"$cmd\""
    done
    for j in $secondarynamenode; do
	ssh $j "sudo bash -c \"$cmd\""
    done
    ssh $jobtrackernode "sudo bash -c \"$cmd\""
    set +x
fi

set +x
if [ "$INSTALLLOCALSAVE" != true ]; then
    echo "INSTALLLOCALSAVE is not enabled. Nothing to do."
    return 0
fi

# /home/gs/conf/local/slaves
# /home/gs/gridre/yroot.openphil1blue/conf/hadoop/slaves.localcopy.txt
set -x
worker_node_filename="slaves.$cluster.txt"
cp $WORK_DIR/$worker_node_filename $scriptdir/$worker_node_filename
set +x
fanoutscp "$scriptdir/$worker_node_filename" "${GSHOME}/conf/local/slaves" "$HOSTLIST"
fanoutscp "$scriptdir/$worker_node_filename" "${GSHOME}/gridre/yroot.$cluster/conf/hadoop/slaves.localcopy.txt" "$HOSTLIST"

cplocalfiles_script="$cluster.cplocalfiles.sh"
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
) > $scriptdir/$cplocalfiles_script

fanoutscp "$scriptdir/$cplocalfiles_script" "/tmp/$cplocalfiles_script" "$HOSTLIST"
cmd="sh -x /tmp/$cplocalfiles_script"
fanout "$cmd"
fanoutGW "$cmd"

if [ -n "$secondarynamenode" ]; then
    cmd="echo \"$secondarynamenode\" > ${GSHOME}/conf/local/masters"
    set -x
    ssh $secondarynamenode "sudo bash -c \"$cmd\""
    set +x
fi

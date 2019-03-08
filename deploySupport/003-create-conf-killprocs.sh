filename="/grid/0/tmp/deploy.$cluster.processes.to.kill.sh"
(
    echo "[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst "
    # echo  yinst self-update -branch yinst7current
    echo "[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst "
    echo 'echo Step 1: kill any running processes.  "(without malice.)"'
    echo 'echo user=`whoami` '
    echo 'for sig in 15 1 9'
    echo 'do'
    echo '   for u in hdfs mapred hadoop1 hadoop2 hadoop3 root mapredqa hdfsqa hadoopqa strat_ci hitusr_1'
    echo '   do'
    echo '        for app in java jsvc'
    echo '        do'
    echo '            pkill -${sig}  -u  ${u}   ${app}'
    echo '        done'
    echo '   done'
    echo 'done'
    echo 'echo `hostname`: killall -1 jsvc'
    echo 'killall -1 jsvc'
    echo 'sleep 5'
    echo 'echo `hostname`: killall -9 jsvc'
    echo 'killall -9 jsvc'
)  > $filename
echo "Generated file $filename"
ls -l $filename

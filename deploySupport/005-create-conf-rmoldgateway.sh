filename="/grid/0/tmp/deploy.$cluster.remove.gateway.old.packages.sh"
(
    echo "[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst "
    echo "[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst "
    echo "chattr -a ${yroothome}/var/yinst/log/yinstlog"
    echo "rm -rf ${yroothome}"
    # clean up old dir /grid/0/gs
    echo "[ -f /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog ] && chattr -a /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog"
    echo "chattr -a /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog"
    echo "rm -rf /grid/0/gs/gridre/yroot.$cluster"
)  > $filename
echo "Generated file $filename"
ls -l $filename

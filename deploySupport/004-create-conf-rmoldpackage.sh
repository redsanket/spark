(
    echo "[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst "
    echo "[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst "
    ##  echo "$yinst remove -yes -live -all -root ${yroothome}  $confpkg "
    # echo "$yinst remove -yes -live -all -root ${yroothome}  gridjdk gridjdk64 hadoop yinst"
    echo "[ -f ${GSHOME}/gridre/yroot/var/yinst/log/yinstlog ] && chattr -a ${GSHOME}/gridre/yroot/var/yinst/log/yinstlog"
    echo "rm -rf ${GSHOME}/gridre/yroot"
    if [ "$CLEANLOCALCONFIG" = true ]; then
#    echo "mkdir -p ${GSHOME}/conf/local.${TIMESTAMP} && mv ${GSHOME}/conf/local/* ${GSHOME}/conf/local.${TIMESTAMP}"
    echo "rm -rf /home/gs/conf/local/local-capacity-scheduler.xml  && touch /home/gs/conf/local/* "
    fi
    # remove the health_check script incase an old one is laying around and until one is available for 23
    echo "rm -f /home/gs/conf/local/health_check"
    # echo "$yinst remove -yes -live -all -root ${yroothome}  $confpkg "
    # echo "$yinst remove -yes -live -all -root ${yroothome}  gridjdk gridjdk64 hadoop yinst"
    echo "chattr -a ${yroothome}/var/yinst/log/yinstlog"
    echo "rm -rf ${yroothome}"
    echo "mkdir -p ${yroothome}"
    # clean up old dir /grid/0/gs
    echo "[ -f /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog ] && chattr -a /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog"
    echo "chattr -a /grid/0/gs/gridre/yroot.$cluster/var/yinst/log/yinstlog"
    echo "rm -rf /grid/0/gs/gridre/yroot.$cluster"
)  > /grid/0/tmp/deploy.$cluster.remove.old.packages.sh

#
# "run the yinst-sets to make the JT config directories have JT content."
#
#
# (1) We run a lgroupadd operation to make sure that the mapred-user is in the group for tasktracker access.
# (2) we run "cfg-${cfgscriptnames}-namenode.sh" (made by the packaging of the configs)
#
# Inputs: $CONFIGUREJOBTRACKER	(boolean)
# Inputs: $jobtrackernode (set by cluster-list.sh)
# Inputs: $cluster
# Inputs: $MAPREDUSER
# Inputs: cfgscriptnames (relick from old install mechanisms.)
#
if [ "$CONFIGUREJOBTRACKER" = true ]
then
    echo == running jobtracker-configure script

    echo "*****"
    echo "*****"
    echo "*****"  "change of cluster:"
    echo "*****"  "we are adding users to group 'hadoop'"
    echo "*****"  "we are changing /etc/grid-keytabs so that $MAPREDUSER to read it."
    echo "*****"
    echo "*****"
    echo "*****"
    echo "*****"


    fanoutnogw "/usr/sbin/lgroupdel hadoop ; /usr/sbin/lgroupadd  -g  10787  hadoop ; /usr/sbin/lgroupmod -M $HDFSUSER,$MAPREDUSER,hadoopqa hadoop ; cd /etc/grid-keytabs ; [ -e tt.* ] && chmod +r tt.* ; [ -e dn.* ] && chmod +r dn.* ; chmod  +r *.keytab  "
    echo == "note short-term workaround for capacity scheduler (expires Sept 30)"
    fanout " export HADOOP_COMMON_HOME=${yroothome}/share/hadoop && export HADOOP_PREFIX=${yroothome}/share/hadoop && export HADOOP_MAPRED_HOME=${yroothome}/share/hadoop && export YARN_HOME=${yroothome}/share/hadoop"

    # fanout "chown $MAPREDUSER  /etc/grid-keytabs/tt.*.service.keytab"
    # fanout "usermod -G hadoop $MAPREDUSER "
    ssh $jobtrackernode "/bin/sh $yrootHadoopConf/cfg-${cfgscriptnames}-jtnode.sh "
fi

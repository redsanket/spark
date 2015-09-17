if [ "$REMOVERPMPACKAGES" = true ]
then
    echo == "== removing RPM packages, if appropriate."
    if [[ "$HADOOP_27" == "true" ]]; then
        fanout "yum -ty remove namenode jobtracker hackhadoop dfshadoop mrhadoop hadoopConfig gridjdk32 gridjdk64 yjava_jdk"
    else
        fanout "yum -ty remove namenode jobtracker hackhadoop dfshadoop mrhadoop hadoopConfig gridjdk32 gridjdk64"
    fi
else
    echo == "not removing RPM packages."
fi

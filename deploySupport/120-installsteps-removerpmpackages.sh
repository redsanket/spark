if [ "$REMOVERPMPACKAGES" = true ]
then
    echo == "== removing RPM packages, if appropriate."
    fanout "yum -ty remove namenode jobtracker hackhadoop dfshadoop mrhadoop hadoopConfig gridjdk32 gridjdk64"
else
    echo == "not removing RPM packages."
fi

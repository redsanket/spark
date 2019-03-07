set +x

if [ "$REMOVERPMPACKAGES" != true ]; then
    echo "REMOVERPMPACKAGES is not enabled. Nothing to do."
    return 0
fi

echo "REMOVERPMPACKAGES is enabled: removing RPM packages, if appropriate."
set -x
fanout "yum -ty remove namenode jobtracker hackhadoop dfshadoop mrhadoop hadoopConfig gridjdk32 gridjdk64 yjava_jdk"
set +x

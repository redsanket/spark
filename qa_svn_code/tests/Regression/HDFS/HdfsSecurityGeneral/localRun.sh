
# this is  provided from Hudson
export CLUSTER="sam"
export SG_WORKSPACE=`pwd`


# will be set up in hdfsRegression.sh
### export HADOOP_VERSION="22"
# export HDFT_TOP_DIR=`pwd`
# export HDFT_HOME=`pwd`

# Not strictly needed, but set up by hdfsRegression.sh for bootstrapping purpose
#export HADOOP_COMMON_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoopcommon
#export HADOOP_HDFS_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoophdfs
#export HADOOP_MAPRED_HOME=/grid/0/gs/gridre/yroot.$CLUSTER/share/hadoopmapred
#export HADOOP_CONF=/grid/0/gs/gridre/yroot.$CLUSTER/conf/hadoop
#export PATH=$HADOOP_COMMON_HOME/bin:$HADOOP_HDFS_HOME/bin:$HADOOP_MAPRED_HOME/bin:$PATH

# get sudo password
# sudo ls 
sh ./hdfsRegression.sh


## Put this in your .bashrc or link it in there

function getClusterName {
  local CLUSTER_NAME=`/usr/bin/curl -s $(/usr/local/bin/yinst which-dist)/igor-mirror/api/getHostRoles?host=$(hostname) | grep "grid_re.clusters" | head -n1 | cut -d"'" -f2 | tr "." " " | awk '{print $NF}'`

  ## If the box is a gateway node, then guess which one we want
  if [ "$CLUSTER_NAME" == "gateway" ]; then
    local host_num=`hostname | cut -d'.' -f1`
    ## remove the first 4 chars
    local host_num=${host_num#????}
    if [ $host_num -eq 2005 ]; then
      local CLUSTER_NAME="omegab"
    fi
    if [ $host_num -eq 2004 ]; then
      local CLUSTER_NAME="ugluk"
    fi
  fi

  echo $CLUSTER_NAME
}


## if user provided the cluster name use that, else try to figure it out
CLUSTER_NAME=${1:-$(getClusterName)}

if [ ! "$CLUSTER_NAME" = "" ];then
  export CLUSTER_NAME

  export GS_DEV=/grid/0/dev
  export GS_ROOT=/home/gs

  #export JAVA_HOME=${GS_ROOT}/java/jdk

  #PATH=/usr/local/bin:/sbin:/usr/sbin:/bin:/usr/bin:${JAVA_HOME}/bin:$HOME/bin:/home/y/bin:/usr/kerberos/bin

  ## Hadoop YRoot
  export HADOOP_YROOT=${GS_ROOT}/gridre/yroot.${CLUSTER_NAME}

  export JAVA_HOME=$HADOOP_YROOT/share/gridjdk-1.6.0_21

  PATH=/usr/local/bin:/sbin:/usr/sbin:/bin:/usr/bin:${JAVA_HOME}/bin:$HOME/bin:/home/y/bin:/usr/kerberos/bin

  export HADOOP_CONF_DIR=${HADOOP_YROOT}/conf/hadoop

  if [ -e ${HADOOP_YROOT}/share/hadoop-current ]; then
    ## we are in 0.20
    export HADOOP_HOME=${HADOOP_YROOT}/share/hadoop-current
    PATH=${PATH}:${HADOOP_HOME}/bin
    export HADOOP_VERSION="0.20.205"
  else
    ## we are in .NEXT
    export HADOOP_PREFIX=${HADOOP_YROOT}/share/hadoop
    ## Hadoop doesn't use this, its more for helping people migrate
    export HADOOP_HOME=${HADOOP_PREFIX}
    PATH=${PATH}:${HADOOP_PREFIX}/bin
    export HADOOP_VERSION="0.23.1"
  fi

  export PATH

fi


## Functions for the different components I work on
### Kerberos
function hadoop_login {
  user=${1:-hdfs}

  ## default keytab
  keytab="/etc/grid-keytabs/$(hostname  | cut -d. -f1).dev.service.keytab"

  if [ ! -e $keytab ]; then
    keytab="/etc/grid-keytabs/${user}.dev.service.keytab"
    echo "This box contains the old way of doing keytabs.  Falling back to: $keytab"
  fi

  if [ ! -e $keytab ]; then
    keytab="/etc/grid-keytabs/oldkeys/${user}.dev.service.keytab"
    echo "This box contains the old way of doing keytabs.  Falling back to: $keytab"
  fi

  case $user in
    hbase) keytab="/etc/grid-keytabs/hbase.service.keytab" ;;
    hadoopqa) keytab="/homes/hadoopqa/hadoopqa.dev.headless.keytab" ;;
    hcat_hadoopqa) keytab="/etc/grid-keytabs/gsbl90832.blue.ygrid.yahoo.com.dev.hcat_hadoopqa.service.keytab" ;;
    *) ;;
  esac

  if [ ! -e $keytab ]; then
    echo "Unable to find keytab $keytab"
    exit 1
  fi

  principal=`/usr/kerberos/bin/klist -k -t $keytab | grep ${user} | tail -n 1 | awk '{print $4}'`

  /usr/kerberos/bin/kinit -k -t $keytab ${principal}
  st=$?
  if [ $st -eq 0 ]; then
    echo "Logged in as $user"
  else
    echo "Unable to login as $user" >&2
    exit $st
  fi
}
### Hadoop
function hadoop_nodes {
  local NODES=$(/usr/bin/curl -s $(yinst which-dist)/igor-mirror/api/getRoleMembers?role=grid_re.clusters.${1:-$CLUSTER_NAME} | grep host | cut -d"'" -f2)
  echo $NODES
}

function hadoop_remote_cmd {
  local cluster=$1
  shift
  for node in $(hadoop_nodes $cluster)
  do 
    ssh -t $node "$@"
  done
}

function htrace {
  if [[ $1 == *gateway* ]]; then
    local grid=$(echo $1 | cut -d"." -f1)
    hadoop_remote_cmd $1  "ps -ef | grep -i $grid | grep -v grep | awk '{print \$2}' | xargs sudo kill -3"
  else
    hadoop_remote_cmd $1  "ps -ef | grep -i $1 | grep -v grep | awk '{print \$2}' | xargs sudo kill -3"
  fi
}

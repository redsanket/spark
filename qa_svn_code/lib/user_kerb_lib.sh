#!/bin/sh

##############################################
# function to create the dir for kerberos ticket
#############################################
function createKerbosTicketDir
{
  #if it does not then create the dir
  if [ -d $KERBEROS_TICKETS_DIR  ]; then
    return
  else
    local cmd="mkdir -p $KERBEROS_TICKETS_DIR"
    echo $cmd
    eval $cmd

    #if the dir did not get created then exit
    if [ "$?" -ne 0 ]; then
      echo "Could not create dir for kerberos tickets $KERBEROS_TICKETS_DIR"
      exit 1
    fi
  fi
}

###############################################
#function to get the kerberos ticket for a user
# $1 - user, valid values are HADOOPQA_USER, HDFS_USER, MAPRED_USER
###############################################
function getKerberosTicketForUser
{
  local user=$1

  #check if user is empty
  if [ -z "$user" ]; then
    echo "No value sent for user."
    exit 1
  fi

  #create the dir
  createKerbosTicketDir

  local kerbTicket=${KERBEROS_TICKETS_DIR}/${user}${KERBEROS_TICKET_SUFFIX}
  if [ $user == 'hadoopqa' ]; then
      local keytabFile=${KEYTAB_FILES_HOMES_DIR}/${user}/${user}${KEYTAB_FILE_NAME_SUFFIX}
  else 
      local keytabFile=${KEYTAB_FILES_DIR}/${user}${KEYTAB_FILE_NAME_SUFFIX}
  fi

  #make sure the keytab file exists
  if [ ! -f "$keytabFile" ]; then
    echo "keytab file $keytabFile for user $user does not exist"
    exit 1
  fi

  local cmd

  case $user in
  $HDFS_USER)
    cmd="kinit -c $kerbTicket -k -t $keytabFile $user/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM"
  ;;
  *)
    cmd="kinit -c $kerbTicket -k -t $keytabFile $user"
  ;;
  esac

  echo $cmd
  eval $cmd
  local rc="$?"

  #check if rc is not 0 then there was an error
  if [ "$rc" -ne "0" ];then
    echo "Error retreiveing the kerberos ticket for ${user}"
    exit 1
  fi
}

####################################################
#function get all users
####################################################
function getAllUserTickets
{

  #create the dir
  createKerbosTicketDir

  #get tickets for all users
  getKerberosTicketForUser $HADOOPQA_USER
  getKerberosTicketForUser $HDFS_USER
  getKerberosTicketForUser $HDFSQA_USER
  getKerberosTicketForUser $MAPRED_USER
  getKerberosTicketForUser $MAPREDQA_USER
  
  getKerberosTicketForUser $HADOOP1_USER
  getKerberosTicketForUser $HADOOP2_USER
  getKerberosTicketForUser $HADOOP3_USER
  getKerberosTicketForUser $HADOOP4_USER
  getKerberosTicketForUser $HADOOP5_USER
  getKerberosTicketForUser $HADOOP6_USER
  getKerberosTicketForUser $HADOOP7_USER
  getKerberosTicketForUser $HADOOP8_USER
  getKerberosTicketForUser $HADOOP9_USER
  getKerberosTicketForUser $HADOOP10_USER
  getKerberosTicketForUser $HADOOP11_USER
  getKerberosTicketForUser $HADOOP12_USER
  getKerberosTicketForUser $HADOOP13_USER
  getKerberosTicketForUser $HADOOP14_USER
  getKerberosTicketForUser $HADOOP15_USER
  getKerberosTicketForUser $HADOOP16_USER
  getKerberosTicketForUser $HADOOP17_USER
  getKerberosTicketForUser $HADOOP18_USER
  getKerberosTicketForUser $HADOOP19_USER
  getKerberosTicketForUser $HADOOP20_USER
}



###################################################
#function to clear all the users ticets
###################################################
function clearAllUserTickets
{
  #unset the env variable
  local cmd="unset KRB5CCNAME"
  echo $cmd
  eval $cmd

  #delete all the tickets
  cmd="rm -rf ${KERBEROS_TICKETS_DIR}"
  echo $cmd
  eval $cmd
}


###############################################
#function to set the kerberos ticket for a user
# $1 - user, valid values are HADOOPQA_USER, HDFS_USER, MAPRED_USER
###############################################
function setKerberosTicketForUser
{
  local user=$1
  
  #check if user is empty
  if [ -z "$user" ]; then
    echo "No value sent for user."
    exit 1
  fi
  
  local kerbTicket=${KERBEROS_TICKETS_DIR}/${user}${KERBEROS_TICKET_SUFFIX}

  #make sure the kerberos ticket file exists
  if [ ! -f "$kerbTicket" ]; then
    echo "kerberos ticket file $kerbTicket for user $user does not exist"
    exit 1
  fi
  
  local cmd="export KRB5CCNAME=${kerbTicket}"
  
  echo $cmd
  eval $cmd

  local rc="$?"

  if [ "$rc" -eq "0" ]; then
    echo "Running as user ${user}"
  else
    echo "Could not set the env to be running as user ${user}"
    exit 1
  fi
}

#!/bin/sh

testcluster=$1

#env var for different users
export HADOOPQA_USER="hadoopqa"
export HADOOP1_USER="hadoop1"
export HADOOP2_USER="hadoop2"
export HADOOP3_USER="hadoop3"
export HADOOP4_USER="hadoop4"
export HADOOP5_USER="hadoop5"
export HADOOP6_USER="hadoop6"
export HADOOP7_USER="hadoop7"
export HADOOP8_USER="hadoop8"
export HADOOP9_USER="hadoop9"
export HADOOP10_USER="hadoop10"
export HADOOP11_USER="hadoop11"
export HADOOP12_USER="hadoop12"
export HADOOP13_USER="hadoop13"
export HADOOP14_USER="hadoop14"
export HADOOP15_USER="hadoop15"
export HADOOP16_USER="hadoop16"
export HADOOP17_USER="hadoop17"
export HADOOP18_USER="hadoop18"
export HADOOP19_USER="hadoop19"
export HADOOP20_USER="hadoop20"
export HDFS_USER="hdfs"
export HDFSQA_USER="hdfsqa"
export MAPRED_USER="mapred"
export MAPREDQA_USER="mapredqa"

export HDFS_SUPER_USER=$HDFSQA_USER
export MAPRED_SUPER_USER=$MAPREDQA_USER


#variables that specify the location of the keytab files
export KEYTAB_FILES_HOMES_DIR="/homes"
export KEYTAB_FILES_DIR="/homes/hdfsqa/etc/keytabs"
export KEYTAB_FILE_NAME_SUFFIX=".dev.headless.keytab"

#variables that store the location of the kerb tickets
timestamp=`date +%s`
export KERBEROS_TICKETS_DIR="/grid/0/tmp/${testcluster}.kerberosTickets.${timestamp}"
export KERBEROS_TICKET_SUFFIX=".kerberos.ticket"

#!/bin/bash

hostname=`/bin/hostname`

JACOCO_PATH="/homes/hadoopqa/jacoco"
JACOCO_JAR="$JACOCO_PATH/jacocoagent.jar"
JACOCO_DEST="/homes/hadoopqa/jacoco/results/latest"

JACOCO_NN_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/namenode-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_DN_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/datanode-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_BAL_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/balancer-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"

JACOCO_RM_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/resourcemanager-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_NM_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/nodemanager-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_JHS_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/jhs-${hostname}.jacoco.exec,includes=org.apache.hadoop*,append=true"


#!/bin/bash

JACOCO_PATH="/homes/hadoopqa/jacoco"
JACOCO_JAR="$JACOCO_PATH/jacocoagent.jar"
JACOCO_DEST="/homes/hadoopqa/jacoco/results/latest"

JACOCO_NN_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/namenode.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_DN_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/datanode.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_BAL_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/balancer.jacoco.exec,includes=org.apache.hadoop*,append=true"

JACOCO_RM_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/resourcemanager.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_NM_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/nodemanager.jacoco.exec,includes=org.apache.hadoop*,append=true"
JACOCO_JHS_OPT="-javaagent:${JACOCO_JAR}=destfile=${JACOCO_DEST}/jhs.jacoco.exec,includes=org.apache.hadoop*,append=true"


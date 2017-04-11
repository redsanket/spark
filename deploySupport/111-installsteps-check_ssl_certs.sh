#!/bin/bash

CERT_HOME="/etc/ssl/certs/prod/_open_ygrid_yahoo_com"

echo == verify Core SSL certs are in place
 
fanout "if [ ! -d ${CERT_HOME} ] ; then 
           echo "Going to create ${CERT_HOME}";
           mkdir -p ${CERT_HOME};
           chmod 755 ${CERT_HOME};
        fi"

fanoutcmd "scp -r ${base}/core_ssl_certs/*.jks __HOSTNAME__:${CERT_HOME}/" "$HOSTLIST"



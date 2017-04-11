#!/bin/bash

CERT_HOME="/etc/ssl/certs/prod/_open_ygrid_yahoo_com"

echo == verify Core SSL certs are in place
 
fanout "if [ ! -d ${CERT_HOME} ] ; then 
           mkdir -p ${CERT_HOME};
           chmod 755 ${CERT_HOME};
        fi"

fanoutcmd "scp -r $scripttmp/core_ssl_certs/* __HOSTNAME__:/${CERT_HOME}/" "$SLAVELIST"
 



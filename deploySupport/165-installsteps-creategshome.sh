if [ "$CREATEGSHOME" = true ]
then
    echo == create ${GSHOME} dirs.
    fanout "if [ ! -d ${GSHOME} ] ; then  if [ $(df -k /home | awk ' /\// { print $2 } ') -le 100000000 ] ; then mkdir -p /grid/0/gshome; ln -s /grid/0/gshome ${GSHOME}; else mkdir -p ${GSHOME}; fi; fi "
    fanoutGW "if [ ! -d ${GSHOME} ] ; then if [ $(df -k /home | awk ' /\// { print $2 } ') -le 100000000 ] ; then mkdir -p /grid/0/gshome; ln -s /grid/0/gshome ${GSHOME}; else mkdir -p ${GSHOME}; fi; fi " 

    fanout "if [ ! -d /home/gs/var/jobstatus ] ; then mkdir -p /home/gs/var/jobstatus; chown -R $MAPREDUSER:users /home/gs/var/jobstatus; fi "
    
fi

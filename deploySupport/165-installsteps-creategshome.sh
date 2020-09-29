set +x
if [ "$CREATEGSHOME" = false ]; then
    echo "CREATEGSHOME is not enabled. Nothing to do."
    return 0
fi

echo "== create ${GSHOME} dirs."

set -x
fanout "if [ ! -d ${GSHOME} ] ; then  if [ $(df -k /home | awk ' /\// { print $2 } ') -le 100000000 ] ; then \
mkdir -p /grid/0/gshome; ln -s /grid/0/gshome ${GSHOME}; else mkdir -p ${GSHOME}; fi; fi"

fanoutGW "if [ ! -d ${GSHOME} ] ; then if [ $(df -k /home | awk ' /\// { print $2 } ') -le 100000000 ] ; then \
mkdir -p /grid/0/gshome; ln -s /grid/0/gshome ${GSHOME}; else mkdir -p ${GSHOME}; fi; fi"

fanout "if [ ! -d /home/gs/var/jobstatus ] ; then \
mkdir -p /home/gs/var/jobstatus; chown -R $MAPREDUSER:users /home/gs/var/jobstatus; fi"

set +x

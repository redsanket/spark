set +x

if [ "$REMOVE_EXISTING_LOGS" != true ]; then
    echo "REMOVE_EXISTING_LOGS is not enabled. Nothing to do."
    return 0
fi

echo "== Remove old logs."

set -x
fanout "rm -rf ${GSHOME}/var/log && if [ ! -d /home/gs/var ] ; then mkdir -p /home/gs/var; fi && \
if [ ! -d /grid/0/gs/var/log ] ; then mkdir -m 0755 -p /grid/0/gs/var/log; fi; \
rm -rf /grid/0/gs/var/log/* ; ln -s /grid/0/gs/var/log ${GSHOME}/var/log"

fanoutGW "rm -rf ${GSHOME}/var/log && if [ ! -d /home/gs/var ] ; then mkdir -p /home/gs/var; fi && \
if [ ! -d /grid/0/gs/var/log ] ; then mkdir -m 0755 -p /grid/0/gs/var/log; fi; \
rm -rf /grid/0/gs/var/log/* ; ln -s /grid/0/gs/var/log ${GSHOME}/var/log"
set +x

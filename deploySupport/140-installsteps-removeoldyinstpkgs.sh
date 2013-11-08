
if [ "$REMOVEOLDYINSTPKGS" = true ]
then
    echo === removing old yinst packages.
    ssh $gateway  "if [ ! -d /home/gs/gridre/javahome/share ]; then mkdir -p /home/gs/gridre/javahome/share; fi && if [ ! -d /grid/0/gshome/gridre/javahomedefault ]; then mkdir -p /grid/0/gshome/gridre/javahomedefault; fi && if [ ! -d /home/gs/gridre/javahome/share/gridjdk-${GRIDJDK_VERSION} ]; then yinst inst -yes -root /grid/0/gshome/gridre/javahomedefault gridjdk-1.7.0_17.1303042057 gridjdk64-1.7.0_17.1303042057 && ln -s /grid/0/gshome/gridre/javahomedefault/share/gridjdk-${GRIDJDK_VERSION} /home/gs/gridre/javahome/share/gridjdk-${GRIDJDK_VERSION} && ln -s /grid/0/gshome/gridre/javahomedefault/share/gridjdk64-${GRIDJDK_VERSION} /home/gs/gridre/javahome/share/gridjdk64-${GRIDJDK_VERSION}; fi && cd /home/gs/java/jdk32 && rm -f current && ln -s /home/gs/gridre/javahome/share/gridjdk-${GRIDJDK_VERSION} current && cd /home/gs/java/jdk64 && rm -f current && ln -s /home/gs/gridre/javahome/share/gridjdk64-${GRIDJDK_VERSION} current "
    fanoutnogw "scp  $ADMIN_HOST:/grid/0/tmp/deploy.$cluster.remove.old.packages.sh   /tmp/deploy.$cluster.remove.old.packages.sh && GSHOME=$GSHOME yroothome=$yroothome sh /tmp/deploy.$cluster.remove.old.packages.sh && rm /tmp/deploy.$cluster.remove.old.packages.sh "
    ssh $gateway  "scp  $ADMIN_HOST:/grid/0/tmp/deploy.$cluster.remove.gateway.old.packages.sh   /tmp/deploy.$cluster.remove.gateway.old.packages.sh && sh /tmp/deploy.$cluster.remove.gateway.old.packages.sh && rm /tmp/deploy.$cluster.remove.gateway.old.packages.sh "
else
    echo === not removing old yinst packages.
fi

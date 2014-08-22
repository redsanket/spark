echo ================= Install Tez on /sharelib/v1/tez hdfs directory =================
export TEZ_CONF_DIR=/home/gs/conf/tez/
export TEZ_HOME=/home/gs/tez/
# workaround for openstack
export PATH=$PATH:/home/gs/current/bin 

if [ $TEZVERSION != none ] && [ $INSTALL_TEZ != false ]; then
cmd="echo installing Tez on Tez hosts ; \
     export TEZ_HOME=/home/gs/tez/ ; \
     mkdir -p /grid/0/Releases/tez_conf-$TEZVERSION/tez ; \
     mkdir -p /grid/0/Releases/tez-$TEZVERSION ; \
     yinst inst -root /grid/0/Releases/tez_conf-$TEZVERSION/tez -same -live -yes ytez_conf-$TEZVERSION -br quarantine ; \
     rm -f /home/gs/conf/tez /home/gs/tez ; \
     ln -s  /grid/0/Releases/tez_conf-$TEZVERSION/tez/conf/tez /home/gs/conf/tez ; \
     yinst inst -root /grid/0/Releases/tez-$TEZVERSION -same -live -yes ytez-$TEZVERSION -br quarantine ; \
     ln -s /grid/0/Releases/tez-$TEZVERSION/libexec/tez /home/gs/tez "
fanoutTez "$cmd"
st=$?
[ "$st" -ne 0 ] && echo ">>>>>>>> Error in running fanoutTez <<<<<<<<<<" && exit $st
cmd="export TEZ_CONF_DIR=/home/gs/conf/tez/ ; \
     export TEZ_HOME=/home/gs/tez/ ; \
     export GSHOME=/home/gs/ ; \
     export HADOOP_HOME=$GSHOME/hadoop/current ; \
     export HADOOP_CLASSPATH="$TEZ_CONF_DIR:$TEZ_HOME/*:$TEZ_HOME/lib/*" ; \
     export JAVA_HOME=$GSHOME/java/jdk ; \
     kinit -k -t /homes/hdfsqa/hdfsqa.dev.headless.keytab hdfsqa ; \
     echo installing Tez into hdfs ; \
     echo `pwd` ; \
     /home/gs/hadoop/current/bin/hadoop fs -rm -R /tmp/output/ ; \
     /home/gs/hadoop/current/bin/hadoop fs -mkdir -p /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez ; \
     /home/gs/hadoop/current/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
     /home/gs/hadoop/current/bin/hadoop fs -put $TEZ_HOME/* /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez ; \
     /home/gs/hadoop/current/bin/hadoop fs -chmod 755 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib ; \
     /home/gs/hadoop/current/bin/hadoop fs -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/*.jar ; \
     /home/gs/hadoop/current/bin/hadoop fs -chmod 744 /sharelib/v1/tez/ytez-$TEZVERSION/libexec/tez/lib/*.jar ; \
     /home/gs/hadoop/current/bin/hadoop fs -chmod -R 755 /sharelib/ ; \
     /home/gs/hadoop/current/bin/hadoop fs -put /home/gs/conf/tez/tez-site.xml /tmp/ ; \
     echo Running hadoop jar $TEZ_HOME/tez-examples-$TEZVERSION.jar orderedwordcount -Dtez.queue.name=default /tmp/tez-site.xml /tmp/output/ ; \
     /home/gs/hadoop/current/bin/hadoop jar $TEZ_HOME/tez-examples-$TEZVERSION.jar orderedwordcount -Dtez.queue.name=default /tmp/tez-site.xml /tmp/output/ ; \
     /home/gs/hadoop/current/bin/hadoop fs -ls /sharelib/v1/tez/ytez-$TEZVERSION/ "
fanoutOneTez "$cmd"
fi

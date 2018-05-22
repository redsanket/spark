#!/bin/bash
# installation instruction can be found in bug 4289416

echo === installing hdfsproxy pkg version=\"$HDFSPROXYVERSION\"
export HIT_NN=`echo $HIT_NN |cut -f1 -d,`
echo HIT_NN=$HIT_NN
echo HIT_HDFS_PROXY=$HIT_HDFS_PROXY
echo YROOT=hit_hp

if [ -z "$HIT_NN" ] 
then
	echo HIT_NN not defined. Exiting.
	exit 1
fi

if [ -z "$HIT_HDFS_PROXY" ] 
then
	echo HIT_HDFS_PROXY not defined. Exiting.
	exit 1
fi


export YR=hit_hp # the name of yroot
export HOSTNAME=$HIT_HDFS_PROXY
export YR_INSTALL_BASE=/grid/0/yroot
(
    echo set -x
    echo export HOSTNAME=$HIT_HDFS_PROXY
    echo SUDO_USER=hadoopqa sudo /usr/local/bin/yinst install yroot -yes
    echo SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set yroot.install_base=$YR_INSTALL_BASE
#    echo SUDO_USER=hadoopqa sudo /usr/local/bin/yinst self-update -branch yinst7stable  -yes
    echo hostname ;echo id
    echo export YR=$YR # the name of yroot
    echo export PATH=/home/y/bin:/usr/local/bin:\$PATH
    echo exit
) | ssh -ttt   $HIT_HDFS_PROXY

# remove old yroot
ssh -ttt $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY   SUDO_USER=hadoopqa sudo /home/y/bin/yroot --remove $YR
SN_HDFS_PROXY=`echo $HIT_HDFS_PROXY | cut -d. -f1`

set -x
# create new yroot
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY   SUDO_USER=hadoopqa sudo /home/y/bin/yroot --create $YR

# Restrict os to rhel-6.x inside the yroot
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set root.os_restriction=rhel-6.x -yroot $YR -yes 

# install headless user dfsproxy and hadoopqa inside the yroot
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst install admin/user-dfsproxy admin/sudo-hadoopqa -yroot $YR -yes 

# copy keytab file
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  SUDO_USER=hadoopqa sudo cp -p -R /etc/yum.repos.d $YR_INSTALL_BASE/var/yroots/$YR/etc/
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  SUDO_USER=hadoopqa sudo cp -p /etc/krb5.conf $YR_INSTALL_BASE/var/yroots/$YR/etc/krb5.conf



# yum installation inside the yroot
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  "/home/y/bin/yroot $YR --user root --cmd 'yum -y install cronolog rcs gcc '"
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  "/home/y/bin/yroot $YR --user root --cmd 'yum -y install kernel-devel'"
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  "/home/y/bin/yroot $YR --user root --cmd 'mkdir -p /etc/grid-keytabs/'"

# copy combine keytab
# jinsun debug, the file name could be diff
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  SUDO_USER=hadoopqa sudo cp -p /etc/grid-keytabs/$SN_HDFS_PROXY.hdfsproxy.dev.service.keytab $YR_INSTALL_BASE/var/yroots/$YR/etc/grid-keytabs/
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  SUDO_USER=hadoopqa sudo chown dfsproxy $YR_INSTALL_BASE/var/yroots/$YR/etc/grid-keytabs/$SN_HDFS_PROXY.hdfsproxy.dev.service.keytab
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY  SUDO_USER=hadoopqa sudo chmod 400 $YR_INSTALL_BASE/var/yroots/$YR/etc/grid-keytabs/$SN_HDFS_PROXY.hdfsproxy.dev.service.keytab

# for debugging purpose
configpkg=$HADOOP_CONFIG_INSTALL_STRING

# install config pkg
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY SUDO_USER=hadoopqa sudo /usr/local/bin/yinst install $configpkg -branch quarantine -yroot $YR -yes -set $confpkg.YINST_VAR_TODO_RUNMKDIRS=false 

# install hdfsproxy pkg inside the new yroot
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst install $HDFSPROXYVERSION $HADOOP_CORETREE_INSTALL_STRING -yroot $YR

st=$?
if [ "$st" -ne 0 ] ; then
    echo "*****" Failed to install HDFSPROXY "*****"
    exit $st

fi

# yinst setting for pkg ygrid_hdfsproxy and ygrid_hdfsproxy_hit
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.namenode_address="hdfs://$HIT_NN" -yroot $YR
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.namenode_principal="hdfs/_HOST@DEV.YGRID.YAHOO.COM" -yroot $YR
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.superuser_kerberos_keytab="/etc/grid-keytabs/$SN_HDFS_PROXY.hdfsproxy.dev.service.keytab" -yroot $YR
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.superuser_kerberos_principal="dfsproxy/@DEV.YGRID.YAHOO.COM"  -yroot $YR
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.webserver_kerberos_keytab="/etc/grid-keytabs/$SN_HDFS_PROXY.hdfsproxy.dev.service.keytab" -yroot $YR
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set ygrid_hdfsproxy.webserver_kerberos_principal="HTTP/$HIT_HDFS_PROXY"  -yroot $YR

ssh -ttt  $HIT_HDFS_PROXY  SUDO_USER=hadoopqa HOSTNAME=$HIT_HDFS_PROXY sudo /usr/local/bin/yinst restart ygrid_hdfsproxy -yroot   $YR

st=$?
if [ "$st" -ne 0 ] ; then
    echo "*****" Failed to restart HDFSPROXY "*****"
    exit $st

fi

# Generate hdfs proxy manifest
/bin/mkdir -p /grid/0/tmp/$cluster.$TIMESTAMP/HIT
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst ls -yroot $YR > /grid/0/tmp/$cluster.$TIMESTAMP/HIT/hp-manifest.txt
ssh -ttt  $HIT_HDFS_PROXY HOSTNAME=$HIT_HDFS_PROXY     SUDO_USER=hadoopqa sudo /usr/local/bin/yinst set -yroot $YR >> /grid/0/tmp/$cluster.$TIMESTAMP/HIT/hp-manifest.txt
scp $HIT_HDFS_PROXY:/grid/0/yroot/var/yroots/hit_hp/home/y/libexec/yjava_tomcat/webapps/logs/hdfsproxy.log /grid/0/tmp/$cluster.$TIMESTAMP/HIT/hdfsproxy.log
# files created has owner and group of root, but need to be changed to user and
# group nobody to accommodate the rsync that will occur at the end of the HIT
# run when test results are copied back from the gateway to the adm machine. 
/bin/chown nobody -R /grid/0/tmp/$cluster.$TIMESTAMP/HIT
/bin/chgrp nobody -R /grid/0/tmp/$cluster.$TIMESTAMP/HIT

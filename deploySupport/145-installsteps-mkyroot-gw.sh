#!/usr/local/bin/bash

set -x 

# for each gateway/yroot, remove it then create it.
#
# Populate with things we will need: updated yinst,
#  username hadoopqa, kerberos config file(s), hadoopqa's home-dir.

if [ -n "$gateways" ]
then
for g in $gateways
do
    case $g in
    *:*)
	machname=`echo $g | cut -f1 -d:`
        yrootname=`echo $g | cut -f2 -d:`
        ;;
    *)
        machname=`echo $g | cut -f1 -d:`
        yrootname=hadoop.${cluster}
        ;;
    esac
    yrootname=${yrootname}

    case $g in
    gwbl2008*|gwbl2009*)
        yrootimage=5.6-20110328
        ;;
    *)
        yrootimage=6.5-20140110
        ;;
    esac

    banner Installing onto machine $machname yroot $yrootname

    
    banner  Step 1 of $machname/$yrootname.  Removing $yrootname from $machname.
    yroot_output=`ssh $machname "/home/y/bin/yroot --set $yrootname" `
    if [ -n "$yroot_output" ]; then
        export removeOldYroot=true
    	echo SUDO_USER=hadoopqa sudo /home/y/bin/yroot --stop $yrootname | ssh $machname
    	echo SUDO_USER=hadoopqa sudo /home/y/bin/yroot --remove $yrootname | ssh $machname
    else
	echo "yroot $yrootname doesn't exist, skip yroot remove now...."
    fi

    banner  Step 2 of $machname/$yrootname.  Creating $yrootname on $machname.
    echo SUDO_USER=hadoopqa sudo /home/y/bin/yroot --create ${yrootname} ${yrootimage} | ssh $machname
    echo SUDO_USER=hadoopqa sudo /home/y/bin/yroot ${yrootname}  --cmd \'/usr/local/bin/yinst install admin/user-${HDFSUSER} admin/user-hadoopqa admin/user-${MAPREDUSER}\'  | ssh $machname
    
#    banner  Step 3 of $machname/$yrootname.  Upgrading yinst to v7(skipped)
    
#    (
#            echo "echo SUDO_USER=hadoopqa  /usr/local/bin/yinst self-update -branch yinst7stable -downgrade -same | sudo /home/y/bin/yroot  ${yrootname}"
#    ) | ssh $machname
#    if [ $? -ne 0 ]
#    then
#    	echo "/usr/local/bin/yinst self-update -branch yinst7stable hadoopqa failed"
#    	exit 1
#    fi
    
    echo "/home/y/bin/yrootcp /etc/krb5* ${yrootname}:/etc/" |  ssh $machname
    echo "/home/y/bin/yroot ${yrootname} --cmd 'mkdir -p /etc/grid-keytabs'" | ssh $machname
    echo "/home/y/bin/yrootcp /etc/grid-keytabs/* ${yrootname}:/etc/grid-keytabs/" |  ssh $machname
    echo "/home/y/bin/yrootcp /usr/lib/liblzo2* ${yrootname}:/usr/lib/" |  ssh $machname
    echo "/home/y/bin/yrootcp /usr/lib64/liblzo2* ${yrootname}:/usr/lib64/" |  ssh $machname
    echo 'chsh -s /bin/bash hadoopqa' | ssh ${machname} /home/y/bin/yroot ${yrootname}
    echo "mkdir -p /home/y/var/yroots/${yrootname}/home/hadoopqa/.ssh " |  ssh $machname
    echo "/home/y/bin/yrootcp /$HOMEDIR/hadoopqa/.ssh/* ${yrootname}:/$HOMEDIR/hadoopqa/.ssh/ " |  ssh $machname
    echo "/home/y/bin/yrootcp /$HOMEDIR/hadoopqa/hadoopqa.dev* ${yrootname}:/$HOMEDIR/hadoopqa/ " |  ssh $machname
    echo "chown -R hadoopqa /home/y/var/yroots/${yrootname}/home/hadoopqa " |  ssh $machname
    echo "chgrp -R users /home/y/var/yroots/${yrootname}/home/hadoopqa " |  ssh $machname
    echo "chmod 700 /home/y/var/yroots/${yrootname}/home/hadoopqa/.ssh " |  ssh $machname
    echo "/home/y/bin/yroot --mount ${yrootname} /root" |  ssh $machname
    echo "mkdir -p /home/y/var/builds " | ssh $machname
    echo "chown hadoopqa /home/y/var/builds " | ssh $machname
    echo "chmod 755 /home/y/var/builds " | ssh $machname
    echo "yinst install yhudson_slave " | ssh $machname

#    m=`df ~hadoopqa | sed -n 2p`
#    echo "echo 'mount $m ~hadoopqa' | /home/y/bin/yroot  ${yrootname}" |  ssh $machname
done
fi
return 0 ;
# fanoutGW "hostname"

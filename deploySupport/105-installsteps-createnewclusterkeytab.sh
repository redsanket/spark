if [ "$CREATE_NEW_CLUSTER_KEYTAB" = true ] 
then
    /root/pushkey_http.sh $CLUSTER
    /root/pushkey.sh $CLUSTER
    /gridadmin/bin/krbComboKeytabGenForDev.sh -f jt $jobtrackernode
    /root/pushkey_namenode.sh $CLUSTER
    sleep 300
    return 0
fi

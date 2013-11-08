if [ "$CREATE_NEW_CLUSTER_KEYTAB" = true ] 
then
    /gridadmin/bin/pushkey_http.sh $CLUSTER
    /gridadmin/bin/pushkey.sh $CLUSTER
    /gridadmin/bin/krbComboKeytabGenForDev.sh -f jt $jobtrackernode
    /gridadmin/bin/pushkey_namenode.sh $CLUSTER
    sleep 300
    return 0
fi

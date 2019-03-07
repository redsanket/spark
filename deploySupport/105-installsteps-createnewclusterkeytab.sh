set +x
if [ "$CREATE_NEW_CLUSTER_KEYTAB" != true ]; then
    echo "CREATE_NEW_CLUSTER_KEYTAB is not enabled. Nothing to do."
    exit 0
fi

echo "CREATE_NEW_CLUSTER_KEYTAB is enabled"
set -x
/gridadmin/bin/pushkey_http.sh $CLUSTER
/gridadmin/bin/pushkey.sh $CLUSTER
/gridadmin/bin/krbComboKeytabGenForDev.sh -f jt $jobtrackernode
/gridadmin/bin/pushkey_namenode.sh $CLUSTER
sleep 300
set +x
return 0

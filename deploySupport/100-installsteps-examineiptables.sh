set +x
StopIPtables=true

if [ "$StopIPtables" != true ]; then
    echo "StopIPtables is not enabled. Nothing to do"
    return 0
fi

fanoutslownogw 'echo === stopping any fire-wall on `hostname` ; /etc/init.d/iptables stop ; sleep 3 ;/etc/init.d/iptables status'
return $?

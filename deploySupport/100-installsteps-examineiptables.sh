set +x
StopIPtables=true

if [ "$StopIPtables" != true ]; then
    echo "StopIPtables not enabled. Nothing to do"
    return 0
fi

set -x
slownogwfanout 'echo === stopping any fire-wall on `hostname` ; /etc/init.d/iptables stop ; sleep 3 ;/etc/init.d/iptables status'
set +x
return 0

StopIPtables=true
if [ "$StopIPtables" = true ] 
then
    slownogwfanout 'echo === stopping any fire-wall on `hostname` ; /etc/init.d/iptables stop ; sleep 3 ;/etc/init.d/iptables status'
    return 0
fi

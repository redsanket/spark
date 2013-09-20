echo cluster=$cluster
echo gateway=$gateway

(
    echo cd ${yroothome}
    echo "a=\`ls -d share/gridjdk-${GRIDJDK_VERSION} 2> /dev/null\`"
    echo echo \$a
    echo "
          if [ -e \$a/bin/java ]
          then
		echo yinst set -root ${yroothome}   $confpkg.TODO_JAVA_HOME=${yroothome}/\$a
          else
			echo No yinst install for 
          fi "
) | ssh $gateway

echo cluster=$cluster
echo gateway=$gateway

JAVA_RPATH="share/yjava_jdk/java"

(
    echo cd ${yroothome}
    echo "a=\`ls -d $JAVA_RPATH 2> /dev/null\`"
    echo echo \$a
    echo "
          if [ -e \$a/bin/java ]
          then
		echo yinst set -root ${yroothome}   $confpkg.TODO_JAVA_HOME=${yroothome}/\$a
          else
			echo No yinst install for 
          fi "
) | ssh $gateway

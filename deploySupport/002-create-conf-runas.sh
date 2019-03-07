filename="/grid/0/tmp/deploy.$cluster.createrunAs.sh"
(
    echo "[ -x /usr/local/bin/yinst ] && export yinst=/usr/local/bin/yinst "
    echo "[ -x /usr/y/bin/yinst ] && export yinst=/usr/y/bin/yinst "
    echo "echo ======= compiling runAs for Herriot deployment."
    echo "cd ${yroothome}/share/hadoop-current/src/test/system/c++/runAs/  &&  \\"
    echo "sh configure --with-home=${yroothome}/share/hadoop-current && make && cp runAs ${yroothome}/conf/hadoop/"
    echo "cd ${yroothome}"
    echo "chown root:users  conf/hadoop/runAs"
    echo "chmod 4711  conf/hadoop/runAs"
) >  $filename
echo "Generated file $filename"
ls -l $filename

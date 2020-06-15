*************************
Commands and Common Tasks
*************************


This section lists useful commands and frequently executed tasks.

- Java PS: ``jps``
- Java Stack: ``jstack``
- ``Hadoop fs -put, -get, -cat, -ls``
- Example of running a test in a loop:

  .. code-block:: bash

    while :;do mvn surefire:test -Dtest=TestJobImpl#testUnusableNodeTransition || break;done


- Build command:

  .. code-block:: bash

    # to compile C code Add -Pnative
    mvn install -Pdist -Dtar -DskipTests -DskipShade -Dmaven.javadoc.skip
    # to build with shading
    mvn clean install -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip

- More repos for linux

  .. code-block:: bash

    --enablerepo=latest-rhel-7-server-optional-rpms
    --enablerepo=latest-rhel-7-server-extras-rpms install docker libcgroup-tools

- Restart hadoop processes on QE clusters:

  .. code-block:: bash

    yinst stop resourcemanager -root /home/gs/gridre/yroot.openqe99blue; yinst start resourcemanager -root /home/gs/gridre/yroot.openqe99blue
    yinst stop namenode -root /home/gs/gridre/yroot.openqe99blue; yinst start namenode -root /home/gs/gridre/yroot.openqe99blue
    yinst stop nodemanager -root /home/gs/gridre/yroot.openqe99blue; yinst start nodemanager -root /home/gs/gridre/yroot.openqe99blue
    yinst stop datanode -root /home/gs/gridre/yroot.openqe99blue; yinst start datanode -root /home/gs/gridre/yroot.openqe99blue
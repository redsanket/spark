******************
Commands and Tools
******************


.. _common_tasks:

Common Tasks
============

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


.. _cmds_tools:

Tools
=====

.. _cmds_tools_ysar:

ysar
----

:abbr:`ysar (Yahoo! System Activity Reporter)` collects data about system
activity and reports it.  The data collector is a perl script which is run by
a ycronjob every 5 minutes. |br|
The reporter is another perl script which can either be run in a shell
or as a CGI (http://<host>/ysar).

Example of ysar commands are as (see :yahoo_github:`CoreCommit/ysar`)

The ysar comand has the following format


  .. code-block:: bash

    ysar [options] [groups]


.. table:: `Options for ysar`
  :widths: auto
  :name: table-ysar-options

  +------------------+------------------------------------------------------------------+
  | Option           | Description                                                      |
  +==================+==================================================================+
  | -accumulate      | Accumulate data for past n days (preserving time-of-day)         |
  +------------------+------------------------------------------------------------------+
  | -day             | Show single value for each day                                   |
  +------------------+------------------------------------------------------------------+
  | -current         | Show average of current and previous sampling interval           |
  +------------------+------------------------------------------------------------------+
  | -header <lines>  | Show header every lines (default: 40)                            |
  +------------------+------------------------------------------------------------------+
  | -interval <mins> | Show values every  minutes (default: 30)                         |
  +------------------+------------------------------------------------------------------+
  | -ndays <days>    | Show values for past days (default: 1)                           |
  +------------------+------------------------------------------------------------------+
  | -raw             | Show values only (no headers or statistics)                      |
  +------------------+------------------------------------------------------------------+
  | -log <filename>  | Specify ysar log data file (default: /home/y/logs/ysar/ysar.dat) |
  +------------------+------------------------------------------------------------------+


.. table:: `acceptable groups for ysar`
  :widths: auto
  :name: table-ysar-groups

  +---------------+--------------------------------------------------------------------------+
  | Group         | Description                                                              |
  +===============+==========================================================================+
  | Summary       | An overall summary of activity (the default)                             |
  +---------------+--------------------------------------------------------------------------+
  | util          | Utilization overview of cpu, memory, disk, and network.                  |
  +---------------+--------------------------------------------------------------------------+
  | cpu           | CPU share (user, system, interrupt, nice, idle & iowait).                |
  +---------------+--------------------------------------------------------------------------+
  | mem           | Physical memory share (active, inactive, cached, free, wired)            |
  +---------------+--------------------------------------------------------------------------+
  | memx          | Memory extended counters on Linux (buffers, cached, swap and huge pages) |
  +---------------+--------------------------------------------------------------------------+
  | ps            | Process state (# of procs in run, sleep, idle, disk-wait)                |
  +---------------+--------------------------------------------------------------------------+
  | sys           | System (context switches, syscalls, interrupts, traps per sec)           |
  +---------------+--------------------------------------------------------------------------+
  | swap          | Page-outs/ins (# swap, vnode pages out/in per sec)                       |
  +---------------+--------------------------------------------------------------------------+
  | fork          | Forks (# forks, vforks per sec)                                          |
  +---------------+--------------------------------------------------------------------------+
  | tcp           | TCP (input pkts/bytes, output pkts/bytes, rexmit pkts/bytes)             |
  +---------------+--------------------------------------------------------------------------+
  | udp           | UDP (input pkts, output pkts, dropped input pkts)                        |
  +---------------+--------------------------------------------------------------------------+
  | ip            | IP (input pkts, output pkts)                                             |
  +---------------+--------------------------------------------------------------------------+
  | dev           | Devices (read ops/bytes, write ops/bytes, %busy)                         |
  +---------------+--------------------------------------------------------------------------+
  | if            | Network interfaces (input pkts/bytes, output pkts/bytes)                 |
  +---------------+--------------------------------------------------------------------------+
  | df            | Disk space                                                               |
  +---------------+--------------------------------------------------------------------------+
  | load          | Load                                                                     |
  +---------------+--------------------------------------------------------------------------+
  | version       | Displays installed ysar version number                                   |
  +---------------+--------------------------------------------------------------------------+
  | yapache       | Yapache (req/sec, avg response time, bytes in/out per request,           |
  |               | avg concurrent requests, max concurrent requests). |br|                  |
  |               | If configured, optional ``-yapache-name`` parameter must be specified    |
  |               | the yapache instance name to retrieve statistics for.                    |
  +---------------+--------------------------------------------------------------------------+
  | kmem          | Kernel memory statistics                                                 |
  +---------------+--------------------------------------------------------------------------+


.. _cmds_tools_heatmap:

Heatmaps
--------

Jim created scripts to generate heatmaps. Files are in the ``adm-scripts`` and
``trace2heatmap.pl`` in
`Google Drive <https://drive.google.com/drive/folders/1hCkzKhHIakMAF37lf84hFjH0itZw8qBz?usp=sharing>`_.


On an adm node, you need both of those in the current directory, and run,
for example:

  .. code-block:: bash

    ./generate-heatmaps.sh jet


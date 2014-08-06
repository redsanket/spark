=========
Reference
=========

.. include:: ../faq/index.rst
.. include:: ../troubleshooting/index.rst


Deployment at Yahoo
===================

.. image:: images/deployment.jpg
   :height: 100px
   :width: 200 px
   :scale: 50 %
   :alt: alternate text
   :align: right

Use Case Example
================

The Search team wants to index editorial real-time content that users 
can search. The editorial content is available in Apache HBase. 

Topography Overview
-------------------

Spouts
######

They will need a spout that scans HBase since the last scan 
until the current time to get the editorial content.

Bolts
##### 

They will need two bolts: The first to build the index
and store it back into HBase, and the 
second to push the index for serving.

Throughput
----------

1. Supervisor-Level Information
###############################

.. csv-table:: Configurations for storm.yaml or multitenant-scheduler.yaml 
   :header: "Configuration", "Default", "Description"
   :widths: 30, 20, 45

   "``topology.receiver.buffer.size``", "<8> messages", "The queue size of the incoming (worker) messages."
   "``topology.transfer.buffer.size``", "<1024> tuples", "The queue size of outgoing (worker) messages."
   "``topology.executor.receive.buffer.size``", "<1024> tuples", "The queue size of the incoming (executor) tuple."
   "``topology.executor.send.buffer.size``", "<1024> tuples", "The queue size of the outgoing (executor) tuple."
   "``supervisor.slots.ports``", "<24> hyper-threaded cores for dual hex-core machines", "The slots available per supervisor."
   "``multitenant.scheduler.user.pools``", "N/A", "The user pools for the multi-tenant scheduler: ``<users>:<#nodes>``" 
   "``topology.isolate.machine``", "The number of machines for a topology."


2. Servers Based on Throughput 
##############################

.. csv-table:: Server Requirements for Topology
   :header: "", "Default"
   :widths: 40, 50

   "Events processed with single spout per worker", "1000 messages/second"
   "Target throughput required in next six months", "8000 messages/second"
   "The number of spout executors required.", "8000/1000 = 8"
   "The number of tuples executed across the first bolt (5 executors)", "10000 tuples/second"
   "The total number of executors required across the first bolt", "8 x 5= 40"
   "The number of tuples executed across the secondd bolt (5 executors)",  "8 x 5= 40"
   "The total number of executors required across second bolt", "8 x 5 = 40"
   "The total number of executors and workers (4 executions per worker slot)", "8 + 40 + 40 = 88 executors (i.e., 88/4 = 22 Slots)"
   "**Number of Supervisors required to process data**", "**22/24 =~ 1 supervisors (24 slots per supervisor)**"


CPU vs. Throughput
------------------

1. Track CPU usage either by JVM debugging (jmap/jstack)
########################################################



.. csv-table:: Server Requirements for Topology
   :header: "", "Default"
   :widths: 40, 50

   "Max CPU cores per Supervisor", "C-78U/48/4000 (four 4 TB disks) =  12 Physical cores"
   "CPU Usage for processing 1000 messages/second", "4 Physical core (32.12%) OR 8 Hyper threads
                                                     - Includes 1 Spout, 5 Bolt 1 and 5 Bolt 2 executors
                                                     - Includes CPU Usage for inter-messaging (0mq or Netty)"

   "Assuming equal core division among Spout and Bolt executors", "Each executor’s CPU need = 4 / (1+5+5) = 4/11 Cors"
   "Total Workers (equal to number of executors)", "TOPOLOGY_WORKERS, Config#setNumWorkers"
   "Tasks per component", "TOPOLOGY_TASKS, ComponentConfigurationDeclarer#setNumTasks()"

2. Extrapolate for target throughput (Assuming a liner increase of resources)
#############################################################################


.. csv-table:: Server Requirements for Topology
   :header: "", "Default"
   :widths: 40, 50

   "Target Spout executors", "TopologyBuilder#setSpout() 8"
   "Target Bolt executors", "TopologyBuilder#setBolt()  40"
   "Total CPU need for Spout executors", "8 x 4/11 Physical core = ~ 3 Physical cores"
   "Total CPU need for Bolt 1 executors", "40 x 4/11 Physical core = ~ 15 Physical cores"
   "Total CPU need for Bolt 2 executors", "40 x 4/11 Physical core = ~ 15 Physical cores"
   "Total CPU need for Topology", "3 + 15 +15 = 33 Physical cores"
   "Total Supervisors needed", "33/12 = ~3 Supervisor"

Memory vs. Throughput
---------------------

1. Supervisor Level-Information 
###############################

(configured values in storm.yaml or Storm-yarn.yaml

.. csv-table:: Configuration Values for storm.yaml/Storm-yarn.yaml
   :header: "", ""
   :widths: 40, 50

   "Max memory available per Supervisor Node", "C-78U/48/4000 (four 4 TB disks) =  48 GB"
   "Memory available to Supervisor container (logical)", "``Storm-yarn.yaml`` > master.container.size-mb 42 GB"

2. Servers Based on Memory Needs
################################

.. csv-table:: Configuration Values for storm.yaml/Storm-yarn.yaml
   :header: "", ""
   :widths: 40, 50

   "Events processed across Spout executors", "8000 messages/second"
   "Avg. event or message size", "3 MB"
   "Data processed per second across spout executors", "8000 x 3 MB = ~ 24 GB/sec"
   "Events processed per second across bolt 1 executors", "10000 x 8 = 80000 tuples/second"
   "Average tuple size", "100 KB"
   "Data processed per second across bolt 1 executors", "80000 tuples/sec x 100 KB = ~8 GB/sec"
   "Events processed per second across bolt 2 executors", "15000 x 8 = 120000 tuples/second"
   "Average tuple size", "100 KB"
   "Data processed per second across bolt 1 executors", "120000 tuples/sec x 100 KB = ~12 GB/sec"
   "Total data processed", "24 GB/second + 8 GB/second + 12 GB/second = 44 GB/second"
   "**Number of Supervisors required to process data**", "**44 / 42 = ~2 Supervisor**"


















  


 
Logging
=======

Storm daemon are located by default in ${storm.home}/logs. ystorm installs logs in ${YINST_ROOT}/y/lib64/storm/current/logs.
For example:
/home/y/lib64/storm/current/logs
/home/y/lib64/storm/current/logs/logviewer.log
/home/y/lib64/storm/current/logs/nimbus.log
/home/y/lib64/storm/current/logs/supervisor.log
/home/y/lib64/storm/current/logs/ui.log
...
The ystorm_* launcher scripts have logs as well. If the various ystorm daemons (nimbus, ui, supervisor, etc.) fail even to write the logs referenced above, then check the launcher logs to see what happened:
/home/y/logs/ystorm_daemons/nimbus/current
/home/y/logs/ystorm_daemons/supervisor/current
/home/y/logs/ystorm_daemons/ui/current
...
Nimbus nimbus.log
Startup & Shutdown
The first thing we expect to see when nimbus starts is some diagnostic output from ZooKeeper/Curator code, followed by a Starting nimbus with conf... message:
Show Example 

    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:zookeeper.version=3.4.5--1, built on 09/11/2013 20:08 GMT
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:host.name=tellingsmelling.corp.gq1.yahoo.com
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.version=1.7.0_13
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.vendor=Oracle Corporation
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.home=/home/y/libexec64/jdk1.7.0/jre
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.class.path=/home/y/lib64/storm/0.9.0-wip21/storm-netty-0.9.0-wi
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.library.path=/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64:
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.io.tmpdir=/tmp
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:java.compiler=<NA>
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:os.name=Linux
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:os.arch=amd64
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:os.version=2.6.32-358.6.2.el6.YAHOO.20130516.x86_64
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:user.name=nobody
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:user.home=/
    2013-09-21 22:47:04 o.a.z.ZooKeeper [INFO] Client environment:user.dir=/home/y/var/daemontools/ystorm_nimbus
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:zookeeper.version=3.4.5--1, built on 09/11/2013 20:08 GMT
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:host.name=tellingsmelling.corp.gq1.yahoo.com
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.version=1.7.0_13
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.vendor=Oracle Corporation
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.home=/home/y/libexec64/jdk1.7.0/jre
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.class.path=/home/y/lib64/storm/0.9.0-wip21/storm-netty-
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.library.path=/home/y/lib64:/usr/local/lib64:/usr/lib64:
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.io.tmpdir=/tmp
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:java.compiler=<NA>
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:os.name=Linux
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:os.arch=amd64
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:os.version=2.6.32-358.6.2.el6.YAHOO.20130516.x86_64
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:user.name=nobody
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:user.home=/
    2013-09-21 22:47:04 o.a.z.s.ZooKeeperServer [INFO] Server environment:user.dir=/home/y/var/daemontools/ystorm_nimbus
    2013-09-21 22:47:09 b.s.d.nimbus [INFO] Starting Nimbus with conf {"dev.zookeeper.path" "/tmp/dev-storm-zookeeper", "topology.tick


Shutdown is similar, with a Shutting down master followed by Shut down master
Show Example 

::

    2013-11-08 18:44:32 b.s.d.nimbus [INFO] Shutting down master
    2013-11-08 18:44:32 o.a.z.ZooKeeper [INFO] Session: 0x142368cbb2f0001 closed
    2013-11-08 18:44:32 o.a.z.ClientCnxn [INFO] EventThread shut down
    2013-11-08 18:44:32 b.s.d.nimbus [INFO] Shut down master
Topology Sumbission, Assignment, & Killing
When topologies are submitted: Received topology submission for...

::

    2013-11-08 18:09:21 b.s.d.nimbus [INFO] Received topology submission for test-topo-derekd2 with conf {"storm.id" "test-topo-derekd2-8-1383934161", "topology.users" ("derekd" "derekd@DEREKD.YSTORM.NET"), "topology.acker.executors" nil, "to ...
    2013-11-08 18:09:21 b.s.d.nimbus [INFO] nimbus file location:/home/y/var/storm/nimbus/stormdist/test-topo-derekd2-8-1383934161 ...

Activation usually follows: Activating ...

::

    2013-11-08 18:09:21 b.s.d.nimbus [INFO] Activating test-topo-derekd2: test-topo-derekd2-8-1383934161 ...

Assignments are the result of scheduling, so when a topology has successfully been scheduled, or has been rebalanced, etc., we see Setting new assignment for topology id...

::

    2013-11-08 18:09:21 b.s.d.nimbus [INFO] Setting new assignment for topology id test-topo-derekd2-8-1383934161: #backtype.storm.daemon.common.Assignment{:master-code-dir "/home/y/var/storm/nimbus/stormdist/test-topo-derekd2-8-1383934161", ...

When topologies are killed, we see
Delaying event :remove for X secs for ...
Updated ... with status {:type :killed, :kill-time-secs X}
Killing topology: ...
Cleaning up ...

::

    2013-11-08 18:13:40 b.s.d.nimbus [INFO] Delaying event :remove for 30 secs for test-topo-derekd2-9-1383934302
    2013-11-08 18:13:40 b.s.d.nimbus [INFO] Updated test-topo-derekd2-9-1383934302 with status {:type :killed, :kill-time-secs 30}
    2013-11-08 18:14:10 b.s.d.nimbus [INFO] Killing topology: test-topo-derekd2-9-1383934302
    2013-11-08 18:14:15 b.s.d.nimbus [INFO] Cleaning up test-topo-derekd2-9-1383934302

Supervisors can be seen in the nimbus log by looking for their IDs, which look like UUIDs, i.e., 7c024f9d-673d-49e7-aa7f-56d9e535f994.
Supervisor supervisor.log
Startup & Shutdown
Supervisors start with a line like Starting supervisor with id .... The supervisor does not log a message when it is stopped manually.
Launching & Killing Workers
It is the supervisor's job to start workers. When the supervisor launches a worker, we expect a pair of log messages beginning with Launching worker ...

::

    2013-11-08 18:11:43 b.s.d.supervisor [INFO] Launching worker with assignment #backtype.storm.daemon.supervisor.LocalAssignment{:st
    2013-11-08 18:11:43 b.s.d.supervisor [INFO] Launching worker with command: java -server -Xmx768m  -Djava.library.path=/home/y/var/

:timed-out versus :disallowed
There could be several reasons a worker shuts down. If the worker has been "un-scheduled" there will be a log message that includes Shutting down and clearing state for id ... State: :disallowed.
If a worker hangs such that it does not heartbeat to the supervisor within the expected interval (5s default), then we expect a log message like Shutting down and clearing state for id ... State: :timed-out.
If a worker crashes, we expect to see a log message involving an exit code, i.e., Worker process ... exited with code: X

::

    g2013-11-08 06:04:19 b.s.d.supervisor [INFO] Shutting down and clearing state for id 0c8439c6-a4fe-47c4-9c62-b8d06e44aa98. Current supervisor time: 1383890658. State: :timed-out, Heartbeat: #backtype.storm.daemon.common.WorkerHeartbeat{:ti ...
    g...
    g2013-11-08 18:22:13 b.s.d.supervisor [INFO] Shutting down and clearing state for id e7846155-3656-424f-84bf-f133e5891c81. Current supervisor time: 1383934933. State: :disallowed, Heartbeat: #backtype.storm.daemon.common.WorkerHeartbeat{:t ...
    g2013-11-08 18:22:13 b.s.d.supervisor [INFO] Worker Process e7846155-3656-424f-84bf-f133e5891c81 exited with code: 137

TIP If a log message indicates that a worker is :timed-out, then it means the heartbeat thread was starved from being scheduled to run. This can happen because the garbage collection takes over the JVM. If this happens repeatedly with a worker, try submitting the topology with an increased worker JVM heap size: append -Xmx${SIZE_MB}m to topology.worker.childopts.
Worker worker.log
Startup & Shutdown
When a worker starts, we expect similar ZooKeeper/Curator diagnostic logs, followed by a line like Launching worker for ${TOPOLOGY_NAME} on ${SUPERVISOR_ID}:${WORKER_PORT} with id ${WORKER_ID}.

::

    Launching worker for test-topo-derekd-1-1383340860 on 04fa4628-2ab9-468b-b457-c36079921b80:6701 with id 7737e1f4-eec4-4975-87ef-81541496009e

Normally a worker is not shut down. When it is, the current storm implementation kills the process (kill -9), so we do not expect the logs to show anything as the worker does not know what is happening.

Storm Web UI
============


Storm Support
=============

- **iList** - storm-devel@yahoo-inc.com
- **Bugzilla Ticket** - 
  - **Dev:** http://bug.corp.yahoo.com/enter_bug.cgi?product=Low%20Latency
  - **Grid Ops:** http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite
- **Phone:** Check on-call in the Service Now group "Dev-Spark”

Other Resources
===============

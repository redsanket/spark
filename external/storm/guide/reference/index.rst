=========
Reference
=========

.. Status: First draft. This chapter might need more sections.

.. include:: ../faq/index.rst
.. include:: ../troubleshooting/index.rst

Deployment at Oath
===================

We host several non-production and production storm clusters for Oath and Verizon. Please visit `dopper <http://yo/doppler-storm>`_ for the details.


Use Case Example
================

In this particular use case, the Search team wants to 
index editorial real-time content that users 
can search. The editorial content needs to be available in Apache HBase. 

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

   "<code>topology.receiver.buffer.size</code>", "<8> messages", "The queue size of the incoming (worker) messages."
   "<code>topology.transfer.buffer.size</code>", "<1024> tuples", "The queue size of outgoing (worker) messages."
   "<code>topology.executor.receive.buffer.size</code>", "<1024> tuples", "The queue size of the incoming (executor) tuple."
   "<code>topology.executor.send.buffer.size</code>", "<1024> tuples", "The queue size of the outgoing (executor) tuple."
   "<code>supervisor.slots.ports</code>", "<24> hyper-threaded cores for dual hex-core machines", "The slots available per supervisor."
   "<code>multitenant.scheduler.user.pools``", "N/A", "The user pools for the multi-tenant scheduler: ``<users>:<#nodes></code>" 
   "<code>topology.isolate.machine</code>", "The number of machines for a topology."


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
   "The number of tuples executed across the second bolt (5 executors)",  "8 x 5= 40"
   "The total number of executors required across second bolt", "8 x 5 = 40"
   "The total number of executors and workers (4 executions per worker slot)", "8 + 40 + 40 = 88 executors (i.e., 88/4 = 22 Slots)"
   "**Number of Supervisors required to process data**", "**22/24 =~ 1 supervisors (24 slots per supervisor)**"


CPU vs. Throughput
------------------

1. Track CPU Usage Either by JVM Debugging (``jmap/jstack``)
############################################################


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

2. Extrapolate for Target Throughput 
####################################

We're assuming a liner increase of resources in this case.

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

.. csv-table:: Configuration Values for storm.yaml/Storm-yarn.yaml
   :header: "", ""
   :widths: 40, 50

   "Max memory available per Supervisor Node", "C-78U/48/4000 (four 4 TB disks) =  48 GB"
   "Memory available to Supervisor container (logical)", "<code>Storm-yarn.yaml</code> > master.container.size-mb 42 GB"

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

The logs for the Storm daemon are located by default in ``${storm.home}/logs``. 
The ystorm daemons (``nimbus``, ``ui``, ``supervisor``, etc.) write 
logs to ${YINST_ROOT}/y/lib64/storm/current/logs.

For example::

    /home/y/lib64/storm/current/logs
    /home/y/lib64/storm/current/logs/logviewer.log
    /home/y/lib64/storm/current/logs/nimbus.log
    /home/y/lib64/storm/current/logs/supervisor.log
    /home/y/lib64/storm/current/logs/ui.log
    ...

The ``ystorm_*`` launcher scripts have logs as well. If the ystorm daemons 
fail to write logs, check the launcher logs on rhel6::

    /home/y/logs/ystorm_daemons/nimbus/current
    /home/y/logs/ystorm_daemons/supervisor/current
    /home/y/logs/ystorm_daemons/ui/current
    ...
    
If it's on Rhel7, check logs using the following command (replace <daemon> with actual daemon name, e.g. nimbus)::

   systemctl status ystorm_<daemon>.service
   journalcrtl -u ystorm_<daemon>.service
   
    
Nimbus Log
----------

**File:** ``/home/y/lib64/storm/current/logs/nimbus.log``

Startup
#######

The first thing we expect to see when Nimbus starts is some diagnostic output from ZooKeeper/Curator code and then 
the message "Starting Nimbus with conf" as seen below::


   2018-11-09 19:48:04.543 o.a.s.v.ConfigValidation main [WARN] topology.backpressure.enable is a deprecated config please see class org.apache.storm.Config.TOPOLOGY_BACKPRESSURE_ENABLE for more information.
   2018-11-09 19:48:04.610 o.a.s.v.ConfigValidation main [WARN] task.heartbeat.frequency.secs is a deprecated config please see class org.apache.storm.Config.TASK_HEARTBEAT_FREQUENCY_SECS for more information.
   2018-11-09 19:48:04.715 o.a.s.s.o.a.c.u.Compatibility main [INFO] Running in ZooKeeper 3.4.x compatibility mode
   2018-11-09 19:48:04.767 o.a.s.z.ClientZookeeper main [INFO] Staring ZK Curator
   2018-11-09 19:48:04.767 o.a.s.s.o.a.c.f.i.CuratorFrameworkImpl main [INFO] Starting
   2018-11-09 19:48:04.778 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:zookeeper.version=3.4.6-1569965, built on 02/20/2014 09:09 GMT
   2018-11-09 19:48:04.778 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:host.name=openstorm3blue-n1.blue.ygrid.yahoo.com
   2018-11-09 19:48:04.779 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:java.version=1.8.0_102
   2018-11-09 19:48:04.779 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:java.vendor=Oracle Corporation
   2018-11-09 19:48:04.779 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:java.home=/home/y/libexec64/jdk64-1.8.0/jre
   2018-11-09 19:48:04.779 o.a.s.s.o.a.z.ZooKeeper main [INFO] Client environment:java.class.pat ...
   ...
   2018-11-09 19:48:08.202 o.a.s.d.n.Nimbus main [INFO] Starting Nimbus with conf {storm.messaging.netty.min_wait_ms=100, topology.backpressure.wait.strategy=org.apache.storm.policy.WaitStrategyProgressive, ...(omitted)

Shutdown
########

Shutdown is similar with the message "Shutting down master" followed by "Shut down master"::

   2018-11-09 19:47:51.162 o.a.s.s.o.a.c.f.i.CuratorFrameworkImpl Curator-Framework-0 [INFO] backgroundOperationsLoop exiting
   2018-11-09 19:47:51.164 o.a.s.s.o.a.z.ZooKeeper Thread-19 [INFO] Session: 0x166f9b5ba250247 closed
   2018-11-09 19:47:51.164 o.a.s.s.o.a.z.ClientCnxn main-EventThread [INFO] EventThread shut down
   2018-11-09 19:47:51.199 o.a.s.d.n.Nimbus Thread-19 [INFO] Shut down master


Topology Submission, Assignment, and Killing
--------------------------------------------

When topologies are submitted, the log message will being with "Received topology submission for..."::

   2018-11-09 19:43:59.914 o.a.s.d.n.Nimbus pool-24-thread-203 [INFO] Received topology submission for topology-testSpreadBasedOnWorkerHeapLimit-1 (storm-0.10.2.y.278 JDK-1.8.0_102) with conf {topology.users=[hadoopqa@DEV.YGRID.YAHOO.COM, hadoopqa], topology.acker.executors=0, storm.zookeeper.superACL=sasl:gstorm, topology.workers=3, topology.submitter.principal=hadoopqa@DEV.YGRID.YAHOO.COM, topology.debug=true, topology.disable.loadaware.messaging=true, storm.zookeeper.topology.auth.payload=*****, topology.name=topology-testSpreadBasedOnWorkerHeapLimit-1, storm.zookeeper.topology.auth.scheme=digest, topology.kryo.register={}, nimbus.task.timeout.secs=200, storm.id=topology-testSpreadBasedOnWorkerHeapLimit-1-2-1541792639, topology.kryo.decorators=[], topology.eventlogger.executors=0, topology.submitter.user=hadoopqa, topology.max.task.parallelism=null}
   2018-11-09 19:44:10.627 o.a.s.d.n.Nimbus pool-24-thread-203 [INFO] uploadedJar /home/y/var/storm/nimbus/inbox/stormjar-3b2593a3-742b-42e4-a528-6021bf7cf8e8.jar

This is following by "Activating ..."::

    2018-11-09 19:44:11.894 o.a.s.d.n.Nimbus pool-24-thread-203 [INFO] Activating topology-testSpreadBasedOnWorkerHeapLimit-1: topology-testSpreadBasedOnWorkerHeapLimit-1-2-1541792639

Assignments are the result of scheduling, so when a topology has successfully 
been scheduled, or has been re-balanced, etc., you'll see the message "Setting new assignment for topology id..."::

    2018-11-09 19:44:20.669 o.a.s.d.n.Nimbus timer [INFO] Setting new assignment for topology id topology-testSpreadBasedOnWorkerHeapLimit-1-2-1541792639: Assignment(master_code_dir:/home/y/var/storm, ...

When topologies are killed, you'll see the following log messages:

- Delaying event REMOVE for X secs for <topology-id>
- Killing topology:  <topology-id>
- Cleaning up <topology-id>

For example::

   2018-11-09 19:43:36.906 o.a.s.d.n.Nimbus pool-24-thread-124 [INFO] Delaying event REMOVE for 0 secs for topology-testClientSideVerifySchedulable-1-1-1541792592
   2018-11-09 19:43:36.913 o.a.s.d.n.Nimbus pool-24-thread-124 [INFO] Adding topo to history log: topology-testClientSideVerifySchedulable-1-1-1541792592
   2018-11-09 19:43:37.905 o.a.s.d.n.Nimbus timer [INFO] TRANSITION: topology-testClientSideVerifySchedulable-1-1-1541792592 REMOVE null false
   2018-11-09 19:43:37.907 o.a.s.d.n.Nimbus timer [INFO] Killing topology: topology-testClientSideVerifySchedulable-1-1-1541792592
   2018-11-09 19:53:23.906 o.a.s.d.n.Nimbus timer [INFO] Cleaning up topology-testClientSideVerifySchedulable-1-1-1541792592

Supervisors can be seen in the Nimbus log by looking for their IDs, which look 
like UUIDs. For example: ``7c024f9d-673d-49e7-aa7f-56d9e535f994``

Supervisor Log 
--------------

**File:** /home/y/lib64/storm/current/logs/supervisor.log

    
Startup/Shutdown
################

Supervisors start with a log message similar to "Starting supervisor for storm version ...". 
The supervisor does not log a message when it is stopped manually.

Launching & Killing Workers
###########################

The Supervisor's job is to start workers. When the supervisor launches a worker, 
we expect a pair of log messages beginning with "Launching worker ..."
as shown below::

    2018-11-09 19:44:22.026 o.a.s.d.s.BasicContainer SLOT_6700 [INFO] Launching worker with assignment LocalAssignment(topology_id:topology-testSpreadBasedOnWorkerHeapLimit-1-2-1541792639, ...
    2018-11-09 19:44:22.030 o.a.s.d.s.BasicContainer SLOT_6700 [INFO] Launching worker with command: '/home/y/share/yjava_jdk/java/bin/java' '-cp'...

Worker Log
----------

**File:**  ``/home/y/var/storm/workers-artifacts/${YOUR-TOPOLOGY-ID}/${PORT-NUMBER}/worker.log``
    
Startup/Shutdown
################

When a worker starts, you should see similar ZooKeeper/Curator diagnostic logs, followed by a 
log message similar to "Launching worker for ${TOPOLOGY_ID} on ${SUPERVISOR_ID}:${WORKER_PORT} with id ${WORKER_ID} and conf ...".

For example::

    2018-11-01 15:47:32.341 b.s.d.worker main [INFO] Launching worker for blitz-dsp-hdfs-fubariteblue-1541086925-227-1541087122 on 003fc909-a3e6-41a2-a553-10a661de9748-10.211.243.59:6707 with id 716953cc-2f7c-4036-a56d-95d705863ee7 and conf ...

Normally, a worker is not shut down. When it is, the current storm implementation 
kills the process (``kill -9``), so we do not expect the logs to show anything as the 
worker does not know what is happening.

Cleanup configuration
################

For limiting the disk usage of workers' logs and dump files, two parameters are defined to restrict all workers' total usage and each worker's usage: ``logviewer.max.sum.worker.logs.size.mb``, ``logviewer.max.per.worker.logs.size.mb``. Cluster admins may customize their thresholds as needed.

Other Resources
===============

- `Apache Storm Documentation <http://storm.apache.org/index.html>`_
- `Hortonworks: Apache Storm <http://hortonworks.com/hadoop/storm/>`_


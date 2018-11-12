Troubleshooting/Debugging
=========================

.. Status: first draft. May need to revise sections based on technical review.

Topologies
----------

Heartbeats & Timeouts
#####################

Storm is a distributed system and uses heartbeating to keep track of state. 
Heartbeats are used in several places in a Storm cluster and occur at 
different frequencies, which we'll discuss in the following sections.

Worker -> Supervisor
********************

Workers heartbeat to the Supervisor on the same node through the local file system.
To find the heartbeats, you can go to ``/home/y/var/storm/workers/${worker-id}/heartbeats``.

On a multi-tenant cluster, the file system permissions should be set such that no 
worker can access another worker's heartbeats.

When we list the information about the ``/workers`` directory, we can learn some information about the topology and cluster.
In this example, we see ``pint_storm_dev`` is who submitted the topology, that ``gstorm`` is the cluster group, and that
the mode ``770`` has the sticky bit set::

    ``drwxrws--- 5 pint_storm_dev gstorm 4096 Nov 11 06:13 5300499a-d91b-4c33-8099-ac8be56e7d6e`` 

Beats should happen once per second, and the supervisor will time out a Worker after thirty seconds by default.
Workers that time out in this way are rescheduled.

Worker -> Pacemaker -> Nimbus
****************

Workers also heartbeat to Nimbus through Pacemaker. Previously this used to happen via Zookeeper but that became a bottleneck on larger clusters. Authentication with Pacemaker is done via Kerberos.

Beats should happen once per second, and the Nimbus will time out a Worker after 30 seconds by default and reschedule the assignment.
Workers are killed with ``SIGKILL`` by the supervisor when they have timed out.

Supervisor -> Nimbus
********************

Supervisors heartbeat to Nimbus through ZooKeeper.

Beats should happen five times per seconds, and Nimbus will time out a Supervisor after 30 seconds by default.
Supervisor that have timed out are ignored.

Persistence and Bouncing Daemons
################################

We'll look at how state is maintained, how bouncing occurs, and how to delete/kill jobs/topologies.

Nimbus
******

**State**

Nimbus stores topologies and their configurations on local disk at ``/home/y/var/storm/nimbus/stormdist/``
and stores scheduling assignments in ZooKeeper. Supervisor heartbeats are also stored there.

**Bouncing**

Everything should carry on as normal (even user topologies).

**Deleting**

You can delete Nimbus scheduler data in ZooKeeper (or wipe ZooKeeper): the topologies will likely die.
When you delete the Nimbus local disk state, the topologies will die. It is recommended that you use the storm admin subcommand to access Zookeeper instead of accessing Zookeeper directly

Supervisors
***********

**State**

Local disk stores topologies (and configurations) are downloaded from Nimbus.

**Bouncing**

Nothing should happen. Workers continue processing:

- Workers will not terminate on their own without the Supervisor killing them. So 
  if the Supervisor is down permanently, you must kill the Workers manually.

**Deleting**

You can delete the Supervisor state. The Workers may die, but Supervisor should re-launch them 
when it next checks scheduling assignments.

Workers
*******

**State**

This relies on serialized Storm configuration and topology (on disk, downloaded from 
Supervisor) to run.

**Deleting**

Workers don't normally die. They are typically killed by the Supervisor when 
they time out or are no longer un-scheduled on the node.

UI and Logviewer
****************

**State**

There is no state (reads logs).

**Bouncing**

You can bounce whenever you want.

Pacemaker
****************

**State**

There is no state - handles incoming messages without writing it out.

**Bouncing**

You can bounce whenever you want (preferably in a staggered way).

ZooKeeper 
---------

It has become clear that ZooKeeper cannot scale well. There are several issues we 
have faced and here are the work-arounds.

Disk Load
#########

- Cleaning up many huge old data logs at once can peg the disk unless spaced out.
  - Wrote custom purge script available in dist package ``zkp_txnlogs_cleanup``.
  - Migrated to RHEL6 ext4 (slowed down ZooKeeper until "forceSync=no" was used).

ZNode Size 
##########

- Nimbus uses ZooKeeper to store scheduling assignments for topologies and stores these 
  atomically in one shot. With larger topologies, this overruns an internal ZooKeeper 
  buffer that defaults to 1MB.
- Mitigations
  - Add ``-Djute.maxbuffer=4097150`` to ZooKeeper ``jvm_opts``, and to Nimbus, Supervisor, and Worker ``.childopts``.  


JVM
---

jstack (Stack Traces)
#####################

You can take a jstack of a worker from the topology component page and download it.

jmap (Heap Dumps)
#################

You can take a heapdump of a worker from the topology component page and download it.

profiling (Flight Recorder)
#################

You can take a flight recording (by setting a timeout) of a worker from the topology component page and download it.


gdb (For Memory leaks/Direct Byte Buffers)
##########################################

#. Follow similar steps as above to discover the user and PID.
#. Execute a gdb to attach to the PID: gdb --pid.

   ::

       bash-4.1$ sudo -u derekd /usr/bin/gdb --pid 1870
       handle SIGSEGV noprint nostop
       set pagination off
       br mmap if $rsi > 67000000
       commands
       print $rsi
       bt

       c
       end
       c
#. This should help you get stack trace for non-heap stacktraces.
#. The gdb hookup can pause the process causing heartbeat miss and supervisor killing 
   that processes. You may have to stop supervisor in order to avoid worker process getting killed.

Profiling with YourKit
######################

`YourKit <http://twiki.corp.yahoo.com/view/Grid/YourKit>`_ is a popular tool for debugging and profiling Java applications, and it is mentioned with frequency on the Storm mailing lists.


Installing YourKit
******************

#. Download from http://yourkit.com/java/profiler/index.jsp
#. The program may prompt you for a License Key, but if it does not, 
   you can choose "Enter License Key..." from the Help menu.
   - Select "Use a license server, and Enter java.corp.yahoo.com. 

     .. note:: Note that there is a limited pool of licenses, so avoid leave YourKit 
               running if you are not using it.

Deploying YourKit
*****************

#. Check if ``yjava_yourkit`` is installed on the host.  If it is not, then download 
   the Linux ``tar.bz2`` of YourKit and unpack it on the host.
#. Attach the profiler daemon to the target process:

   ::

       bash-4.1$ bin/yjp.sh -attach 1924
       Picked up JAVA_TOOL_OPTIONS:
       Attaching to process 1924 using default options
       The profiler agent has attached. Waiting while it initializes...
       [YourKit Java Profiler 12.0.5] Log file: /home/derekd/.yjp/log/yjp-12726.log
       The agent is loaded and is listening on port 10001.
       You can connect to it from the profiler UI.
#. Create an SSH tunnel if you cannot ``telnet`` from your machine to the target host and port.

   - If ``telnet $host $port`` times out, you need a tunnel.
   - You can tunnel through a third host, such as a gateway, or you can create a tunnel to the same remote host.

     For example: ``ssh -L 10001:gsrd453n26.red.ygrid.yahoo.com:10001 gsrd453n26.red.ygrid.yahoo.com``
     will connect to ``gsrd453n26.red.ygrid.yahoo.com``, and it will open a port 10001 on your machine 
     that connects to port 10001 on the remote host, which is the port on which the profiler daemon is listening. 

     This would also work, by connecting to a gateway box with the same tunnel: 
     ``ssh -L 10001:gsrd453n26.red.ygrid.yahoo.com:10001 gwrd111n02.red.ygrid.yahoo.com``. 
     The ``-L`` specifies the local port, remote host, and remote port for the tunnel. The 
     argument to SSH is the normal host, such that you will be presented a prompt at 
     ``gwrd111n02.red.ygrid.yahoo.com``. 

#. In the YourKit UI on your machine, click "Connect to remote application...".

   - If not using the SSH tunnel, just enter the remote host and port number. If using 
     the tunnel, use localhost for the host name: e.g., ``localhost:10001``
   - This opens a UI presentation showing CPU usage, threads, automatic deadlock detection, 
     memory, and garbage collection.


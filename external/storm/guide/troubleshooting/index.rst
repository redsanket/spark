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
In this example, we see ``derekd`` is who submitted the topology, that ``gstorm`` is the cluster group, and that
the mode ``770`` has the sticky bit set::

    ``drwxrws--- 4 derekd gstorm 4096 Dec 18 21:45 145bbb17-6a29-4396-bc20-3bb6b96fb4a1`` 

Beats should happen once per second, and the supervisor will time out a Worker after five seconds by default.
Workers that time out in this way are rescheduled.

Worker -> Nimbus
****************

Workers also heartbeat to Nimbus through ZooKeeper.
On a multi-tenant cluster, heartbeats are protected with ACLs within 
ZooKeeper such that workers cannot access each others' data.

Beats should happen three times per second, and the Nimbus will time out a Worker after 30 seconds by default.
Workers are killed with ``SIGKILL`` when they have timed out.

Supervisor -> Nimbus
********************

Supervisors heartbeat to Nimbus through ZooKeeper.

Beats should happen five times per secons, and Nimbus will time out a Supervisor after 30 seconds by default.
Supervisor that have timed out are ignored.

Persistence and Bouncing Daemons
################################

We'll look at how state is maintained, how bouncing occurs, and how to delete/kill jobs/topologies.

Nimbus
******

**State**

Nimbus stores topologies and their configurations on local disk at ``/home/y/var/storm/nimbus/stormdist/``
and stores scheduling assignments in ZooKeeper. Worker & Supervisor heartbeats are also stored there.

**Bouncing**

Everything should carry on as normal (even user topologies).

**Deleting**

You can delete Nimbus scheduler data in ZooKeeper (or wipe ZooKeeper): the topologies will likely die.
When you delete the Nimbus local disk state, the topologies will die.

Supervisors
***********

**State**

Local disk stores topologies (and configurations) are downloaded from Nimbus and worker heartbeats.

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

ZooKeeper 
---------

It has become clear that ZooKeeper cannot scale well. There are several issues we 
have faced and here are the work-arounds.

Disk Load
#########

- Workers heartbeat through ZooKeeper to Nimbus. When many workers are alive, this puts tremendous 
  load on ZooKeeper. Heavy disk load means anything using the ZooKeeper cluster slows down and 
  things start timing out or becoming slow and unresponsive.
  - Mitigations:
    - Mount disk with nobarrier (Good improvement while on ``ext3``)
    - Add ``-Dzookeeper.forceSync=no`` to ``zookeeper_server.jvm_args`` and you will see a major improvement.
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

``jstack`` works best when run with the same JDK and as the same user as the target process.

#. Find the PID of the target process using jps and the port number. In this example, 
   we are looking for a the worker running on 6734 on a particular host.
  
   ::

       -bash-4.1$ sudo jps -v | grep 6734
       1870 worker -Xmx1024m -Djute.maxbuffer=4097150 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC 
       -XX:NewSize=128m -XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true 
       -Djava.security.auth.login.config=/home/y/lib/storm/current/conf/storm_jaas.conf -Djute.maxbuffer=4097150 
       -Djava.library.path=/home/y/var/storm/supervisor/stormdist/test-word_count-5-1387400559/resources/Linux-amd64:/home/y/var/storm/supervisor/stormdist/test-word_count-5-1387400559/resources:/home/y/lib64:/usr/local/lib64:/usr/lib64:/lib64: 
       -Dlogfile.name=test-word_count-5-1387400559-worker-6734.log -Dstorm.home=/home/y/lib64/storm/0.9.0-wip21 
       -Dlogback.configurationFile=/home/y/lib64/storm/0.9.0-wip21/logback/worker.xml -Dstorm.id=test-word_count-5-1387400559 
       -Dworker.id=145bbb17-6a29-4396-bc20-3bb6b96fb4a1 -Dworker.port=6734

   Core is the storm UI. The other daemons appear as Nimbus, Supervisor, Logviewer, and Workers as worker.

#. Find the user and JDK used.

   ::

       bash-4.1$ ps -fp 1870
       UID        PID  PPID  C STIME TTY          TIME CMD
       derekd    1870 17840  9 21:02 ?        00:03:55 /home/y/share/yjava_jdk/java/bin/java -server -Xmx1024m -D

#. Obtain a stack trace as that user by calling the appropriate jstack executable.

   ::

       -bash-4.1$ sudo -u derekd /home/y/share/yjava_jdk/java/bin/jstack 1870 > stack.txt
       
       2013-12-18 21:45:38
       Full thread dump Java HotSpot(TM) 64-Bit Server VM (23.7-b01 mixed mode):
       
       "Attach Listener" daemon prio=10 tid=0x00007f6194001000 nid=0x294e waiting on condition [0x0000000000000000]
          java.lang.Thread.State: RUNNABLE
       
       "New I/O client worker #1-1" prio=10 tid=0x00007f612801b000 nid=0x857 waiting on condition [0x00007f61a7eb8000]
          java.lang.Thread.State: WAITING (parking)
               at sun.misc.Unsafe.park(Native Method)
               - parking to wait for  <0x00000000d10f6790> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
               at java.util.concurrent.locks.LockSupport.park(LockSupport.java:186)
               at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(AbstractQueuedSynchronizer.java:2043)
               at java.util.concurrent.LinkedBlockingQueue.take(LinkedBlockingQueue.java:442)
               at backtype.storm.messaging.netty.Client.takeMessages(Client.java:126)
               at backtype.storm.messaging.netty.StormClientHandler.messageReceived(StormClientHandler.java:56)
               at org.jboss.netty.channel.SimpleChannelUpstreamHandler.handleUpstream(SimpleChannelUpstreamHandler.java:80)
               at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:545)
               at org.jboss.netty.channel.DefaultChannelPipeline$DefaultChannelHandlerContext.sendUpstream(DefaultChannelPipeline.java:754)
               at org.jboss.netty.channel.Channels.fireMessageReceived(Channels.java:302)
               at org.jboss.netty.handler.codec.frame.FrameDecoder.unfoldAndFireMessageReceived(FrameDecoder.java:317)
               at org.jboss.netty.handler.codec.frame.FrameDecoder.callDecode(FrameDecoder.java:299)
               at org.jboss.netty.handler.codec.frame.FrameDecoder.messageReceived(FrameDecoder.java:216)
               at org.jboss.netty.channel.SimpleChannelUpstreamHandler.handleUpstream(SimpleChannelUpstreamHandler.java:80)
               at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:545)
               at org.jboss.netty.channel.DefaultChannelPipeline.sendUpstream(DefaultChannelPipeline.java:540)
               at org.jboss.netty.channel.Channels.fireMessageReceived(Channels.java:274)
               at org.jboss.netty.channel.Channels.fireMessageReceived(Channels.java:261)
               at org.jboss.netty.channel.socket.nio.NioWorker.read(NioWorker.java:350)
               at org.jboss.netty.channel.socket.nio.NioWorker.processSelectedKeys(NioWorker.java:281)
               at org.jboss.netty.channel.socket.nio.NioWorker.run(NioWorker.java:201)
               at org.jboss.netty.util.ThreadRenamingRunnable.run(ThreadRenamingRunnable.java:108)
               at org.jboss.netty.util.internal.IoWorkerRunnable.run(IoWorkerRunnable.java:46)
               at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
               at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
               at java.lang.Thread.run(Thread.java:722)
       
       "New I/O server worker #1-1" prio=10 tid=0x00007f60e4002000 nid=0x854 runnable [0x00007f61a7fb9000]
          java.lang.Thread.State: RUNNABLE
               at sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
               at sun.nio.ch.EPollArrayWrapper.poll(EPollArrayWrapper.java:228)
               at sun.nio.ch.EPollSelectorImpl.doSelect(EPollSelectorImpl.java:81)
               at sun.nio.ch.SelectorImpl.lockAndDoSelect(SelectorImpl.java:87)
               - locked <0x00000000d1188bf0> (a sun.nio.ch.Util$2)
               - locked <0x00000000d1188c08> (a java.util.Collections$UnmodifiableSet)
               - locked <0x00000000d117be78> (a sun.nio.ch.EPollSelectorImpl)
               at sun.nio.ch.SelectorImpl.select(SelectorImpl.java:98)
               at org.jboss.netty.channel.socket.nio.SelectorUtil.select(SelectorUtil.java:38)
               at org.jboss.netty.channel.socket.nio.NioWorker.run(NioWorker.java:164)
               at org.jboss.netty.util.ThreadRenamingRunnable.run(ThreadRenamingRunnable.java:108)
               at org.jboss.netty.util.internal.IoWorkerRunnable.run(IoWorkerRunnable.java:46)
               at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
               at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
               at java.lang.Thread.run(Thread.java:722)
       
       "DestroyJavaVM" prio=10 tid=0x00007f61b800a800 nid=0x78a waiting on condition [0x0000000000000000]
          java.lang.Thread.State: RUNNABLE
       
       "New I/O server boss #1 ([id: 0x4364cbbb, /0.0.0.0:6734])" prio=10 tid=0x00007f60e8003800 nid=0x84d runnable [0x00007f61ac18d000]
          java.lang.Thread.State: RUNNABLE
               at sun.nio.ch.EPollArrayWrapper.epollWait(Native Method)
               at sun.nio.ch.EPollArrayWrapper.poll(EPollArrayWrapper.java:228)
               at sun.nio.ch.EPollSelectorImpl.doSelect(EPollSelectorImpl.java:81)
       ...

jmap (Heap Dumps)
#################

#. Follow similar steps as above to discover the user and PID.
#. Execute a binary heap dump with jmap.

   ::

       bash-4.1$ sudo -u derekd /home/y/share/yjava_jdk/java/bin/jmap -dump:format=b,file=heap.bin 1870
       Dumping heap to /home/derekd/heap.bin ...
       Heap dump file created

gdb (For Memory leaks/Direct Byte Buffers)
##########################################

#. Follow similar steps as above to discover the user and PID.
#. Execute a gdb to attach to the pid gdb --pid.

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
#. The gdb hookup can pause the process causing heatbeat miss and supervisor killing 
   that processes. You may have to stop supervisor in order to avoid worker process getting killed.

Profiling with YourKit
######################

`YourKit <http://twiki.corp.yahoo.com/view/Grid/YourKit>`_ is a popular tool for debugging and profiling Java applications, and it is mentioned with frequency on the Storm mailing lists.


Installing YourKit
******************

#. Download from http://yourkit.com/java/profiler/index.jsp
#. The program may prompt you for a License Key, but if it does not, 
   you can choose "Enter License Key..." from the Help menue.
   - Select "Use a license server, and Enter java.corp.yahoo.com. 

     .. note:: Note that there is a limited pool of licenses, so avoid leave YourKit 
               running if you are not using it.

Deploying YourKit
*****************

#. Check if ``yjava_yourkit`` is installed on the host.  If it is not, then download 
   the Linux ``tar.bz2`` of YourKit and unpack it on the host.
#. Attach the profiler daemon to the targed process:
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


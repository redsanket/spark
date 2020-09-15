..  _yarn_troubleshooting_rm-failover:

ResourceManager Failover |nbsp| |green-badge|
=============================================

This section documents the process for failing over to a new ResourceManager
node when the daemons are using the leveldb state store databases.  Each section
covers the process for migrating the various daemons that normally run on RM
nodes.

ResourceManager
---------------

The ResourceManager is configured to store state to a leveldb database, and that
state needs to be migrated to the new node, if possible.  The config property
that controls the store location is
``yarn.resourcemanager.leveldb-state-store.path`` and is currently configured to
``/home/gs/var/run/mapred/yarn-resourcemanager/``.

.. rubric:: To migrate the RM:

#. Shutdown the ``ResourceManager``, as we cannot copy the state of a "live" database
#. Copy the RM state store contents
   (e.g.: ``/home/gs/var/run/mapred/yarn-resourcemanager/`` or wherever
   it's configured) to the same location on the new node
#. Startup the RM on the new node, once service IP names have been moved over, etc.

If the state cannot be preserved due to the local store of the old node is
unavailable/inaccessible then the ResourceManager can be started on the new node
but all applications will be told to die when they try to reconnect with the RM
and the RM will not attempt to relaunch any active applications that had failed.
In other words, all jobs on the cluster will be lost.


.. rubric:: Important Note about Recover Failure:


If the RM restarts and does not recover the prior state then we normally do
*not* want to fix the state store issue and restart the RM again on the original
state.  At that point we have two state stores: the original state and the new
one created when the RM restarted.  |br| Replacing the new state with the old
and restarting has two main drawbacks:

#. All jobs launched in the interim period will be lost, since we clobbered the
   new state with the old.
#. We could end up with duplicate jobs from the old state.
   
The reason we can end up with duplicate jobs is that once the RM comes up
without recovering it will not have recovered any applications nor any tokens. 
|br| Existing applications will try to heartbeat into the new RM instance and be
rejected due to lack of a valid token.  That causes the application attempt to
kill itself.  The RM then eventually expires the expected app attempt and
launches a new attempt.  If that attempt was an `Oozie` launcher, then the Oozie
launcher will re-launch its jobs again, but some of those older jobs could have
also been restarted by the RM. 

Now we have duplicate jobs.  Another way we can end up with duplicate jobs is
that the new RM instance that did not recover will not know about the old apps. 
When the `Oozie` server queries the RM about the old launcher, the RM will claim
that is an unknown app ID.  The `Oozie` server will then relaunch the workflow
as a new instance, but we could end up recovering the old instance.  Then we
could have two independent launchers running for the same workflow.   



MapReduce Job History Server
----------------------------


The MapReduce job history server has a very similar setup to the RM in terms of
migration needs.  It is also storing state to a leveldb database with the path
configured by ``mapreduce.jobhistory.recovery.store.leveldb.path`` and currently
set to ``/home/gs/var/run/mapred/mapred-historyserver/``.

.. rubric:: To migrate the MapReduce job history server: 

#. Shutdown the job history server, as we cannot copy the state of a "`live`"
   database
#. Copy the JHS leveldb state over to the new node in the same path
#. Startup the JHS on the new node

If the state cannot be preserved then the job history server can be started on
the new node, but there will be some failures of `Oozie` jobs.  The jobs will no
longer have a token that the new job history server instance recognizes and
therefore will be rejected when trying to connect to the new history server
instance.


YARN Timeline Server
--------------------


The YARN timeline server is currently configured to store states in two leveldb
databases, controlled by the properties
``yarn.timeline-service.leveldb-timeline-store.path`` and
``yarn.timeline-service.leveldb-state-store.path``.  These are currently
configured to the same location at
``/home/gs/var/run/mapred/yarn-timeline-service``, and the code creates unique
database names underneath the specified path so they won't conflict.

.. rubric:: To migrate the YARN timeline server:

#. Shutdown the timeline server, as we cannot copy the state of a "`live`" database
#. Copy the timeline server leveldb path over to the new node in the same path
#. Startup the timeline server on the new node


If the state cannot be preserved then the timeline server can be started on
the new node, but the previous history of events in the timeline server will
be lost.  In addition there could be some failures from `Oozie` jobs as
launcher clients will be running with a timeline server token that the new
timeline server instance will not recognize.

===============
Getting Started
===============

.. _hbase_getting_started-installation:

In this chapter, we'll cover the basic steps required to experiment with Storm at Yahoo. 
You'll simple convenience package (ystorm_onabox) and by using an OpenStack VM, you can be writing and executing storm topologies in a few simple steps.

Installation
============

#. Create yourself a virtual machine (VM) to work within `OpenHouse <http://openhouse.corp.yahoo.com/>`_.
   Select a **Medium** instance with an **RHEL6.x** image.
#. SSH into the VM.
#. Install the requisite packages:: 

       yinst set root.os_restriction=rhel-6.x
       yinst i ystorm_onabox -br test
       yinst i ystorm_starter -br test
#. At this point Storm should be running, so you should be able to attach to the UI via http://hostname:8080

   .. note:: If you were installing Storm in a ``yroot``, do the following:: 

                 yinst start daemontools_y zookeeper_server ystorm_onabox

#. Now we will launch one of the sample topologies from the storm-starter package::

       storm jar /home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ExclamationTopology excltopology
#. When you refresh the UI, your sample topology should be running
#. The ``storm`` CLI command can also be used to list or kill the topology (e.g., 
   ``storm kill excltopology`` to kill the topology).

Building a Topology From Source
===============================

#. Install the requisite packages::

       yinst i git
       yinst i yjava_maven

#. Clone the ``src`` for ``storm-starter``. (You may need to `set up SSH keys <https://git.corp.yahoo.com/settings/ssh>`_.)

       git clone git@git.corp.yahoo.com:storm/storm-starter.git 
       cd storm-starter
       mvn -f m2-pom.xml compile package

#. Verify you can launch a sample topology from the built package::

       storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ExclamationTopology topo1
       storm list 
       storm kill topo1

#. Make a copy of the **Exclamation** topology and then edit it::

       SRCDIR=./src/jvm/storm/starter
       cp $SRCDIR/ExclamationTopology.java $SRCDIR/MyExclamationTopology.java
       vi $SRCDIR/MyExclamationTopology.java (rename class to MyExclamationTopology, and any other edits you would like to experiment with)
       mvn -f m2-pom.xml compile package


#. Launch your copy of the sample topology and check that it's running::

       storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.MyExclamationTopology mytopo
       storm list


Next Steps
==========

See `On-Boarding With Storm <>`_ to start using Storm on Yahoo's Grid. 

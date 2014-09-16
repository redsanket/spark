===========
Quick Start 
===========

.. Status: First draft. This has been tested and written by the developer team. More notes could be added to elucidate certain steps. 
.. Reference: http://twiki.corp.yahoo.com/view/Grid/StormQuickStart

This quick start shows you how to use a simple convenience package (``ystorm_onabox``) to launch a sample Storm topology on an OpenStack instance.
We'll be launching the topology from your machine instead of using the Yahoo Grid. 


Prerequisites
=============

- Linux RHEL box or OpenStack instance


Setting Up
==========

#. SSH into your OpenStack instance.
#. Set a root OS restriction: ``yinst set root.os_restriction=rhel-6.x``
#. Install the following packages::

        yinst i git
        yinst i yjava_maven
        yinst i ystorm_onabox -br test
        yinst i ystorm_starter -br test
        yinst i ystorm_logviewer -br test

Launching Topology
==================

You should see Storm running with the Storm UI located at http://{hostname}:8080.
If you installed the packages in the last section inside a ``yroot``, however, you will
need to manually start Storm with the following: ``yinst start daemontools_y zookeeper_server ystorm_onabox``


#. To launch one of the sample topologies from the storm-starter package, run the following::
 
        storm jar /home/y/lib/storm-starter/0.0.1-SNAPSHOT/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ExclamationTopology excltopology
   
   If the topology is already running, try killing it (``storm kill excltopology``) and running the command above again.

#. Go to your Storm UI (make sure to refresh the page), and you should see the topology running.
#. You can also list the topologies that are running with ``storm list``.
#. Go ahead and kill the topologies with ``storm kill {topology_name}``.

Building a Topology From Source
===============================

#. Use Git to clone the source for ``storm-starter``. You may need to `set up SSH keys <https://git.corp.yahoo.com/settings/ssh>`_.

       git clone git@git.corp.yahoo.com:storm/storm-starter.git
#. Change to ``storm-starter``.
#. Compile the package with Maven: ``mvn -f m2-pom.xml compile package`` 
#. Verify you can launch a sample topology from the built package: 

       storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ExclamationTopology topo1 
#. Confirm that the topology is running and then kill it::

       storm list
       storm kill topo1

#. Try making some edits and re-running the topology::

       SRCDIR=./src/jvm/storm/starter
       cp $SRCDIR/ExclamationTopology.java $SRCDIR/MyExclamationTopology.java
       vi $SRCDIR/MyExclamationTopology.java (rename class to MyExclamationTopology, and any other edits you would like to experiment with)
       mvn -f m2-pom.xml compile package
       storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.MyExclamationTopology mytopo

Next Steps
==========

We're going to cover how to run topologies on the Yahoo Grid, but before you can do that, you'll need to 
`On-Board <../onboarding/>`_ to Storm.

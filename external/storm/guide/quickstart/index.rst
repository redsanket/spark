===========
Quick Start 
===========

.. Status: First draft. This has been tested and written by the developer team. More notes could be added to elucidate certain steps. 
.. Reference: http://twiki.corp.yahoo.com/view/Grid/StormQuickStart

This quick start shows you how to use the ystorm package to launch a sample Storm topology on an OpenStack instance.
We'll be launching the topology from your VM instead of using the Yahoo Grid. 


Prerequisites
=============

- Linux RHEL 6.x box or OpenStack instance
  (`ystorm` packages 0.10.x will not be available for RHEL 7.x)


Setting Up
==========

#. SSH into your OpenStack instance.
#. Install the following packages for running Storm::

        # Go to https://dist.corp.yahoo.com//by-package/ystorm/ and find the ystorm-0.10.* VERSION that is on the test branch for rhel-6.x.
        yinst i ystorm-$VERSION
        yinst i ystorm_nimbus-current
        yinst i ystorm_supervisor-current
        yinst i ystorm_ui-current
        yinst i ystorm_logviewer-current
        yinst i zookeeper_server

#. Setup Storm::

        yinst start zookeeper_server
        yinst set ystorm.ui_port=4443
        yinst set ystorm.logviewer_port=9999
        yinst start ystorm_nimbus
        yinst start ystorm_ui
        yinst start ystorm_supervisor
        yinst start ystorm_logviewer


Launching Topology
==================

You should see Storm running with the Storm UI located at http://{hostname}:4443.

#. You can download and build the Storm code to use a sample topology for testing::

        yinst i git
        yinst i yjava_maven
        git clone -b master-security git@git.ouroath.com:storm/storm.git
        cd storm
        mvn '-P!include-shaded-deps' -Pexternals,examples install -DskipTests

#. To launch one of the sample topologies from the storm-starter package, run the following::
 
        /home/y/lib64/storm/current/bin/storm jar ./examples/storm-starter/target/storm-starter-*.jar org.apache.storm.starter.WordCountTopology wc
   
   If the topology is already running, try killing it (``storm kill wc``) and running the command above again.

#. Go to your Storm UI (make sure to refresh the page), and you should see the topology running.
#. You can also list the topologies that are running with ``storm list``.
#. Go ahead and kill the topologies with ``storm kill {topology_name}``.


Next Steps
==========

We're going to cover how to run topologies on the Yahoo Grid, but before you can do that, you'll need to 
`On-Board <../onboarding/>`_ to Storm.

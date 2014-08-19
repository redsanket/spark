=========
Tutorials
=========

.. _hbase_getting_started-installation:

In this chapter, you'll find a tutorial that 
shows you how to run the `Quick Start <../quickstart>`_ on
the Yahoo Grid. We hope to have more tutorials in the future,
so please come back, or better yet, contribute your own tutorials
to help others.

.. _storm_tutorials-counting:

Counting Data Highway Events
============================

We're going to revisit the `Quick Start <../quickstart>`_ and run
the same code using your Storm project and topology that you
created when  you `on-boarded <../onboarding>`_.

Prerequisites
-------------

You should have completed the following:

- `On-Boarding <../onboarding>`_
- `Quick Start <../quickstart>`_

Setting Up
----------

#. Log onto the Red Ebony (or the non-production environment that you created a topology for) cluster.
#. Clone the ``storm-contrib`` repository: git@git.corp.yahoo.com:storm/storm-contrib.git
   .. note:: We'll be using ``/src/main/java/com/yahoo/spout/http/rainbow/EventCountBolt.java``.
#. Set up your launcher box:

   #. Perform Kerberos authentication:: ``kinit {your_user_name}@Y.CORP.YAHOO.COM``

Launch Storm Topology
---------------------

For example, we will launch our sample topology with 2 machines and 2 spout instances:

#. Configure the topology to use two machines and two spout instances::

       yinst set ystorm.topology_isolate_machines=2
#. Launch storm with the two spouts::

       storm jar /home/y/lib/jars/rainbow_spout_example-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://dh-demo-ebonyred.ygrid.local:50700 -n dh-demo-w-2spouts -p 2
 
   .. TBD: Will probably need to change the command above.

Storm With HBase
================

.. See http://tiny.corp.yahoo.com/3qM6Bg

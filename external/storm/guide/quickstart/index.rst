===========
Quick Start 
===========

This quick start will show you how to use the DH Rainbow spout and
count DH Rainbow events from your own OpenStack instance.  

.. images:: images/dh_rb-event_count_bolt.jpg
   :height: 100px
   :width: 200 px
   :scale: 100 %
   :alt: Schematic diagram for the DH Rainbow spout and the event count bolt.
   :align: left


Prerequisites
=============

- Linux RHEL box or OpenStack instance

Setting Up
==========

#. Install the DH Rainbow spout: ``yinst i ystorm_contrib``. 
#. Clone the ``storm-contrib`` repository: git@git.corp.yahoo.com:storm/storm-contrib.git
   .. note:: We'll be using ``/src/main/java/com/yahoo/spout/http/rainbow/EventCountBolt.java``.
#. Set up your launcher box:

   #. Perform Kerberos authentication:: ``kinit {your_user_name}@Y.CORP.YAHOO.COM`` 
   #. Specify the registry service: ``yinst set ystorm.http_registry_uri=http://registry-a.{red|blue}.ygrid.yahoo.com:4080/registry/v1/``
   #. Obtain a unique virtual host name and port from http://twiki.corp.yahoo.com/view/Grid/SupportStormDHRegistry. This tutorial will use the following:
      - **EbonyRed** (cluster)
      - **virtual host** - ``dh-demo-ebonyred.ygrid.local (must be unique within a colo, red/BF1)
      - **virtual port** - 50700 (must be unique within each storm cluster, EbnoyRed?)
#. Create a virtual service URI with the following syntax: ``http://<virtual_host>:<virtual_post>``  
#. Add the virtual service URI to Storm: ``storm jar /home/y/lib/jars/rainbow_spout_example-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat addVH http://dh-demo-ebonyred.ygrid.local:5070011

Launch Storm Topology
=====================

For example, we will launch our sample topology with 2 machines and 2 spout instances:
#. Configure the topology to use two machines and two spout instances::

       yinst set ystorm.topology_isolate_machines=2
#. Launch storm with the two spouts::

       storm jar /home/y/lib/jars/rainbow_spout_example-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://dh-demo-ebonyred.ygrid.local:50700 -n dh-demo-w-2spouts -p 2

Inject Sample Rainbow Events
============================

To inject events, we'll be using ``yfor`` to enable communication with multiple spouts that we have launched.

#. Install ``yfor``: ``yinst i yfor -b test``
#. Configure ``yfor`` for routing by adding the following configuration to ``/home/y/etc/yfor/dh-demo-ebonyred.conf``::

       name dh-demo-ebonyred.ygrid.local
       config-url http://registry-a.red.ygrid.yahoo.com:4080/registry/v1/virtualHost/dh-demo-ebonyred.ygrid.local/ext/yahoo/yfor_config
       
#. ``LD_PRELOAD=/home/y/lib64/libyfor.so.1``
. Use cURL to inject an event from a file: ``LD_PRELOAD=/home/y/lib64/libyfor.so.1 curl --data-binary @/homes/afeng/dh_events/out.prism.3 http://dh-demo-ebonyred.ygrid.local:50700``

Next Steps
==========

`On-Board <../onboarding/>`_ to Storm.

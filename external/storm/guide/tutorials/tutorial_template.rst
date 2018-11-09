[Tutorial Name]
===============

This tutorial will show you how to use Storm with ...
We'll be using ...


Prerequisites
-------------

You should have completed the following:

- `Quick Start <../quickstart>`_
- `On-Boarding <../onboarding>`_


Setting Up
----------

.. These steps should not include the on-boarding, but should include any step
   needed to launch (run) the storm job.

#. Log onto the [Storm environment: should be the same as your topology].
#. Now, ...

Launch Storm Topology
---------------------

.. Here, we're just running the Storm jobs w/ the specified number of spout instances.

For example, we will launch topology with [n] spout instances:

#. Launch storm with the two spouts::

       storm jar /home/y/lib/jars/rainbow_spout_example-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://dh-demo-ebonyred.ygrid.local:50700 -n dh-demo-w-2spouts -p 2
 
   .. TBD: Will probably need to change the command above.

Look at the Code
----------------

Spouts
######

-  In the spout below, you ....

   .. code-block:: java

      public class HttpSpout extends BaseRichSpout {
          final static Logger LOG = LoggerFactory.getLogger(HttpSpout.class);
          private SpoutOutputCollector collector;
          private Server server;
          private BlockingQueue<Object> queue;      

          ...

Bolts
#####

- In this bolt, you

  .. code-block:: java

     public static class HBaseInjectionBolt extends BaseRichBolt {
            byte[] hbase_conf_buff;
    	byte[] table_name;
            transient OutputCollector collector;
    	transient HTable table;
    	String topology_name;

.. See http://tiny.corp.yahoo.com/3qM6Bg

Next Steps
----------

.. Point to tutorials that are related or at least reference/overview docs that might further the understanding of this tutorial.



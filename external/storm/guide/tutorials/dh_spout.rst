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
#. Authenticate with ``kinit``: ``$ kinit {your_user_name}@Y.CORP.YAHOO.COM``

Launch Storm Topology
---------------------

For example, we will launch our sample topology with 2 machines and 2 spout instances:

#. Configure the topology to use two machines and two spout instances::

       yinst set ystorm.topology_isolate_machines=2
#. Launch storm with the two spouts::

       storm jar /home/y/lib/jars/rainbow_spout_example-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://{your_topology_name}.ygrid.local:50700 -n {your_topology_name} -p 2

   The main difference between the ``storm`` command in this tutorial from that in the quick start is that you are specifying the topology that you requested. The topology gives you an instance within
   in the environment that you requested when you on-boarded.
      
#. You can see your job running in the **Storm UI**. The URL to the **Storm UI** depends on your
   environment. The URL syntax is ``http://{environment}-ni.blue.ygrid.yahoo.com:9999/``, so the
   URL to the **Storm UI** for Ebony Red is ``http://ebonyblue-ni.blue.ygrid.yahoo.com:9999/``.

#. Click on your job and take a look at your spouts, bolts, the number of executors, tasks, and the topology
   configuration.
#. Kill your job to let others use the limited resources in the non-production environments:

   ``
Looking at the Code
-------------------

Spouts
######

This example uses the Rainbow DH spout that gets data from the Data Highway through the Registry Service.
The Registry Service requires YCA v2 authentication.  

In `EventCountTopology.java <https://git.corp.yahoo.com/storm/storm-contrib/blob/master/rainbow_spout_example/src/main/java/com/yahoo/spout/http/rainbow/EventCountTopology.java>`_,
the method ``runTopology`` creates the topology builder, sets the spot, and attaches the bolt before submitting the topology for execution.
Here we are using two workers and set credentials that were pushed by the method ``pushCreds``.

.. code-block:: java

   public void runTopology(URI serviceURI) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        RainbowSpout s = new RainbowSpout(serviceURI, _byteBlobs, _allowedAppIds);
        s.setUseSSLEncryption(!_noSslEncryption);
        s.setEventQueueSize(_queueSize);
        s.setAcking(_acked);
        builder.setSpout("rainbow", s, _spoutParallel);
        builder.setBolt("count", new EventCountBolt(), 1).shuffleGrouping("rainbow");

        _conf.registerSerialization(AvroEventRecord.class,  KryoEventRecord.class);
        _conf.registerSerialization(ByteBlobEventRecord.class,  KryoEventRecord.class);

        if (_yamasApp != null) {
            _conf.registerMetricsConsumer(YamasMetricsConsumer.class, _yamasApp, 1);
        }
 
         if (_debug) {
            _conf.setDebug(true);
         }
 
        _conf.put(backtype.storm.Config.TOPOLOGY_SPREAD_COMPONENTS, Arrays.asList("rainbow"));
        _conf.setNumWorkers(2);

        SubmitOptions opts = new SubmitOptions(TopologyInitialStatus.ACTIVE);
        opts.set_creds(new Credentials(_creds));
        StormSubmitter.submitTopology(_topologyName, _conf, builder.createTopology(), opts);
    }

Bolts
#####

The `EventCountBolt.java <https://git.corp.yahoo.com/storm/storm-contrib/blob/master/rainbow_spout_example/src/main/java/com/yahoo/spout/http/rainbow/EventCountBolt.java>`_
extends the class ``BaseBasicBolt``, which is the simplest of the many built-in `Storm classes <http://nathanmarz.github.io/storm/doc-0.8.1/index.html>`_ for bolts. 

The ``execute`` method in our classes counts the number of records it receives from our spout and emits the value.
In a more real-life example, you would probably want to process the data from the spout and have the spout possibly emit results 
that could be further processed or written to an HBase or Hive table.

.. code-block:: java

   public void execute(Tuple tuple, BasicOutputCollector collector) {
        LOG.info("Received tuple " + tuple);
        AvroEventRecord rec = (AvroEventRecord)tuple.getValue(0);
        Object at = rec.getData();
        for (String part : path) {
            if (at instanceof GenericRecord) {
                at = ((GenericRecord)at).get(part);
            } else {
                LOG.error("Could not find "+Arrays.toString(path)+" inside "+rec.getData());
                return;
            }
        }
        if (at == null) {
            LOG.error("Could not find "+Arrays.toString(path)+" inside "+rec.getData());
            return;
        }
        String val = at.toString();
        int count = 0;
        if (counts.get(val) != null) {
            count = counts.get(val);
        }
        count++;
        counts.put(val, count);

        collector.emit(new Values(val, count));
    }


Next Steps
----------

- See `Programming Storm <../programming>`_ for more code examples.

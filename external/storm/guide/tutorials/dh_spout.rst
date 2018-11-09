Counting Data Highway Events
============================

.. Status: first draft. Need more examples.

This tutorial shows you how to set up a Storm project on an OpenStack instance, launch it on the Grid cluster Ebony Red, and
then create events to alter the results of your project. You will also be using the Data Highway (DH) Rainbow spout,
a built-in Yahoo spout, to count DH Rainbow events.

The diagram below shows how the DH Rainbow bolt gets data from the Data Highway and feeds it to the bolt that counts events.

.. image:: images/dh_rb-event_count_bolt.jpg
   :height: 100px
   :width: 200 px
   :scale: 100 %
   :alt: Schematic diagram for the DH Rainbow spout and the event count bolt.
   :align: left

Prerequisites
-------------

You should have completed the following:

- `Quick Start <../quickstart>`_ - You'll need a basic understanding of how Storm topologies are launched, listed, and killed.
- `On-Boarding <../onboarding>`_ - You'll need a topology that you requested when you on-boarded, which provides you with an environment in the Storm cluster.

Setting Up
----------

#. Log onto an OpenHouse instance. We use OpenHouse to compile the topology and tools as the grid nodes and gateway do not have public internet access for security reasons.
#. Install ``yinst i yjava_maven yjava_yca``.
#. Clone the ``storm-contrib`` repository: git@git.ouroath.com:storm/storm-contrib.git
#. Change to ``storm-contrib``.
#. Build the package with Maven: ``LD_LIBRARY_PATH=/home/y/lib64 mvn clean install``
#. Copy ``storm-contrib`` to Ebony Red: ``rsync -razv --progress ../storm-contrib/ ebony-gw.red.ygrid.yahoo.com:./storm-contrib/``

Defining Your Virtual Host
--------------------------

Originally Data Highway would have a real host registered for it to send data to.  With Storm there is no real host, 
as the spouts can be scheduled anywhere on the cluster.  To work around this Storm Data Highway Spouts 
register themselves with the registry service under a virtual host name, that corresponds with a topology.
The virtual host URI should have the following syntax: ``http://<virtual_host>``  If you are doing a production
topology you will need to use https to ensure that everything is secure.

For example, if your topology name is ``mydemo`` and you were using the Ebony Red cluster,
your virtual host URI might be something like ``http://mydemo-ebonyred.ygrid.local``.


#. Before you set the name of your virtual host, check the `Storm Data Highway Registry <http://twiki.corp.yahoo.com/view/Grid/SupportStormDHRegistry>`_
   to make sure that the  virtual host is not be used.  You can do this by calling `registry_client list` from a gateway.
#. Add your virtual service URI using the following command::

       registry_client addvh  http://{topology_name}-ebonyred.ygrid.local


Launching Your Storm Topology
-----------------------------

For our example, we will launch our sample topology with 2 machines and 2 spout instances:

#. Log onto the cluster Ebony Red (ebony-gw.red.ygrid.yahoo.com) or another non-production environment that you created a topology for.
#. Authenticate with ``kinit``: ``$ kinit {your_user_name}@Y.CORP.YAHOO.COM``
#. Launch storm with the two spouts below. Replace ``{your_topology_name}`` with the topology name you requested during on-boarding::

       storm-contrib/rainbow_spout_example/target/rainbow_spout_example-*-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://{topology_name}-ebonyred.ygrid.local/ -n {topology_name} -p 2 -c http.registry.uri='https://registry-a.red.ygrid.yahoo.com:4443/registry/v1/'

   The main difference between the topology name in this tutorial from that in the 
   quick start is that the topology here represents an instance on the Storm
   cluster as well as the name of the topology running.

.. Ex: storm-contrib/rainbow_spout_example/target/rainbow_spout_example-*-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.EventCountTopologyCompat run http://RainbowSpoutTest-ebonyred.ygrid.local/ -n RainbowSpoutTest -p 2 -c http.registry.uri='https://registry-a.red.ygrid.yahoo.com:4443/registry/v1/'
 
      
#. You can see your job running in the **Storm UI**. 
   The URL to the **Storm UI** depends on your
   environment. The URL syntax is ``https://{environment}-ni.{color}.ygrid.yahoo.com:4443``, so the
   URL to the **Storm UI** for Ebony Red is ``https://ebonyred-ni.red.ygrid.yahoo.com:4443``.

#. Click on your job and take a look at your spouts, bolts, the number of executors, tasks, and the topology
   configuration.

Injecting Sample Rainbow Events
-------------------------------

To inject events, we'll be using a DataHighwaySimulator built into the storm-contrib example.

``storm jar storm-contrib/rainbow_spout_example/target/rainbow_spout_example-*-jar-with-dependencies.jar com.yahoo.spout.http.rainbow.DHSimulator -r https://registry-a.red.ygrid.yahoo.com:4443/registry/v1/ http://{topology_name}-ebonyred.ygrid.local --batches 100``

This code looks up the spouts associated with the http://{topology_name}-ebonyred.ygrid.local virtual host and sends 100 batches of data highway events to them.

#. Go back to the `Storm UI <http://ebonyred-ni.red.ygrid.yahoo.com:9999>`_. You should see changes in the topology statistics.  You should see that some events were emitted from the spouts and processed by the bolts.
  

Killing Your Topology
---------------------

We recommend killing the topologies you create in tutorials to save Grid resources for others: ``$ storm kill {topology_name}``


Looking at the Code
-------------------

Spouts
######

This example uses the Rainbow DH spout that gets data from the Data Highway through the Registry Service.
The Registry Service requires YCA v2 authentication when security is enabled (which it must be in production).  

In `EventCountTopology.java <https://git.ouroath.com/storm/storm-contrib/blob/master/rainbow_spout_example/src/main/java/com/yahoo/spout/http/rainbow/EventCountTopology.java>`_,
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

The `EventCountBolt.java <https://git.ouroath.com/storm/storm-contrib/blob/master/rainbow_spout_example/src/main/java/com/yahoo/spout/http/rainbow/EventCountBolt.java>`_
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

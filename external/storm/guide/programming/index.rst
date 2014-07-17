=================
Programming Storm
=================

The following sections get 

Overview
========

Types of Available Grouping
---------------------------

- Shuffle grouping: pick a random task (but with load balancing)
- Fields grouping: consistent hashing on a subset of tuple fields
- All grouping: send to all tasks 
- Global grouping: pick task with lowest id
- Shuffle or Local grouping: If there is a local bolt (in the same worker process) use it otherwise use shuffle.

Yahoo Spouts
------------

DH Rainbow Spout
################

The Data Highway (DH) Rainbow spout allows Storm to ingest Data Highway Rainbow events. 
DH Rainbow pushes events directly to customers over HTTP. This model is different from most 
pub-sub systems that Storm normally interacts with, as such setting up and using 
the DH Rainbow spout is a bit more involved then a typical Storm spout.

CMS Spout
#########

Integrate the Storm Topology with Yahoo Cloud Messaging Service Queue.

Redis Spout
###########

Integrate the Storm Topology with Redis database

Yahoo Bolts
-----------

HBase Bolt
##########

Write data/results to the HBase

HDFS Bolt
#########

Write data/results to the HDFS storage

Sherpa Bolt
###########

Write data/results to the Sherpa for serving.

Using Yahoo Spouts
==================

Rainbow Spout
-------------

Find the DH Rainbow Spout
#########################

Because DH uses a push model it needs a way to find the HTTP servers (spouts) that 
should receive the data. To allow DH to find the spouts we have developed a 
`Registry Service <../registry_service_api/>`_ as a bridge. The spouts tell the 
service where they are and DH, through ``yfor``, queries it to know where they are 
and to know how to authenticate with them.

.. image:: images/dh_spout.png
   :height: 404 px
   :width: 283 px
   :scale: 100 %
   :alt: The relationship between DH Spout, the Storm Registry Service, and Data Highway. 
   :align: left


Getting the Rainbow Spout
#########################

#. Install the yinst package `ystorm_contrib <http://dist.corp.yahoo.com/by-package/ystorm_contrib/>`_. 
   (It is still a bit of a work in progress so please check back regularly to be 
   sure you get all of the latest updates.).
#. Add the following dependencies to your ``pom.xml`` to pull in the DH Rainbow spout::

       <dependency>
           <groupId>yahoo.yinst.ystorm_contrib</groupId>
           <artifactId>http_spout</artifactId>
           <version>${ystorm_contrib.version}</version>
       </dependency>
       <dependency>
           <groupId>yahoo.yinst.ystorm</groupId>
           <artifactId>storm</artifactId>
           <version>${ystorm.version}</version>
           <exclusions>
               <exclusion>
                   <groupId>javax.servlet</groupId>
                   <artifactId>servlet-api</artifactId>
               </exclusion>
               <exclusion>
                   <groupId>ring</groupId>
                   <artifactId>ring-core</artifactId>
               </exclusion>
               <exclusion>
                   <groupId>ring</groupId>
                   <artifactId>ring-jetty-adapter</artifactId>
               </exclusion>
               <exclusion>
                   <groupId>org.mortbay.jetty</groupId>
                   <artifactId>jetty</artifactId>
               </exclusion>
           </exclusions>
           <!-- keep storm out of the jar-with-dependencies -->
           <scope>provided</scope>
       </dependency>
Notes
*****

The exclusions in the ``ystorm`` package are important for the spout to run properly. 
Storm uses a very old version of Jetty for the Web UI. It is not needed when 
running the worker process, but still remains as a dependency in Maven. The 
DH Rainbow Spout uses a much newer and more improved version of Jetty, which can 
have a few conflicts with the older version of Jetty when running tests through maven.

Another thing to be aware of is that many dependencies, the Data Highway APIs in 
particular, use ``slf4j`` as their logging API, but also include the backend bridge 
to write the logs out through ``log4j``. Storm 0.9.0 and above has replaced ``log4j`` 
with logback and included a ``log4j`` compatibility layer so calls to ``log4j`` go 
through ``slf4j`` and on to logback (log4j-over-slf4j). If any of your dependencies 
include ``log4j`` or slf4j-logj* as dependencies please be sure to exclude these too.

DH Rainbow Spout Location
#########################

The RainbowSpout is located at ``com.yahoo.spout.http.rainbow.RainbowSpout``, but 
is really just a wrapper around ``com.yahoo.spout.http.HttpSpout`` with some 
defaults that are particular to Data Highway Rainbow. These include a plug-in to 
de-serialize the Data Highway payload and a default list YCA roles that Data Highway 
uses to authenticate itself with the spout. All of these are pluggable and we 
encourage you to look at potentially using the ``HttpSpout`` for other situations 
where you may want to push data to a storm topology.

Configuring DH Rainbow Spout
############################

The DH Rainbow spout tries to use a builder like model for most optional configuration. 
The required configuration is passed to the constructor, or if it is a cluster 
wide value, as in the case of the registry service, or it may come from the Storm configuration.

.. csv-table:: DH Rainbow Spout Configuration
   :header: "Configuration", "Required?", "Default", "Description", "How to Configure"
   :widths: 25, 10, 20, 40

   "Registry Service Location", "Yes", "None", "This should be set by the cluster you are running on
   and is required when using YCA v2 for authenticating with the registry service, so any production topology should have this set.", "To set the
   configuration, set ``http.registry.uri`` in the configuration or use the method ``setRegistryProxy`` on the spout."
   "HTTP Proxy for Accessing Registry", "Yes", "None", "The HTTP proxy to use to access the registry. This should be set by the cluster you're running on
   and is required when using YCA v2 for authenticating with the registry with the registry service, so any production topology should
   have this set.", "To set this configuration, set ``http.registry.proxy`` in the configuration file or use the method ``setRegistryProxy`` on the spout."
   "Service URI of Spout", "Yes", "None", "This tells the spout three pieces of information. The scheme to run the web server with ``http`` or ``https``. 
   The virtual host that the spout should add itself to in the registry, and the port number that it should listen on. The rest of the URL is ignored, 
   but may be used in the future.", "You pass this information to the constructor of the spout."
   "YCA Application IDs", "No", "``NULL`` for ``HttpSpout`` and ``yahoo.dh.prod.backend``, ``yahoo.dh.staging.backend``, ``yahoo.dh.sandbox.backend`` for
   ``RainbowSpout``."
   "Deserialize Byte Blobs / Use Scheme Cache for Avro Schema", "No", "``false``", "The configuration to deserialize byte blobs or use the schema cache for the avro schema. (This is specific to the RainbowSpout).", "Pass the value to the constructor of the spout (Or to the constructor of the RainbowEnqueuer if using a custom ``Enqueuer``)."	
   "``Enqueuer`` for HTTP Payload and Queue it for Spout", "No", "``SimpleEnqueuer`` for ``HttpSpout`` (The payload is 
   enqueued as a byte array), and ``RainbowEnqueuer`` for the ``RainbowSpout``."
   "Heartbeat Frequency", "No", "30 seconds", "The frequency in milliseconds how often to ping the registry service to confirm that the spout is alive (minimum of 10 seconds).", "Use the ``setRegHbFreq`` method on the spout to define the heartbeat frequency."
   "Queue Size", "No", "50", "The number of items that the spout can have queued before it pushes back.", "Use the method ``setEventQueueSize`` on the spout to set the queue size."
   "SSL Data Encryption", "No", "true", "Determines whether the spout uses SSL encryption for data. In general, ``https`` is encouraged for everyone using spouts, so the client can validate it is communicating to the correct server, but for
  that only occur within the colo, the ``SL_RSA_WITH_NULL_MD5`` cipher can be used to provide authentication although no
  data encryption.", "Use the ``setUseSSLEncryption`` method from the spout to set or unset SSL encryption."
  "Set Registry Role", "No", "``NULL`` (disabled)", "If you are running on a storm cluster that is not multi-tenant you may want to avoid the hassle of pushing new YCA v2 creds periodically. In this case you can use YCA v1 to authenticate with the registry service and have the credentials pulled from each of the compute nodes. Be aware this requires you to trust anyone with access to those compute nodes.", "Use the ``setV1RegistryRole`` method with the role to use."

TBD: Avro Schemas

http://tiny.corp.yahoo.com/tG2SFQ

.. code-block:: java

   RainbowSpout s = new RainbowSpout();
   s.setEventQueueSize(1000);
   builder.setSpout("rainbow", s, 5);
   ... 
   conf.registerSerialization(AvroEventRecord.class, KryoEventRecord.class);
   conf.registerSerialization(ByteBlobEventRecord.class, KryoEventRecord.class);   
   conf.put(backtype.storm.Config.TOPOLOGY_SPREAD_COMPONENTS, Arrays.asList("rainbow"));
   conf.setNumWorkers(5);
   conf.put("yahoo.autoyca.appids",”my.ycav2.appid”);

CMS (JMS) Spout
---------------

No Official generic Spout YET (http://tiny.corp.yahoo.com/yJ6EYw) is a good starting point.

.. code-block:: java

   String lookupServiceHostPortUrl = "http://" + host + ":" + port;
   String fullyQualifiedNamespace = String.format("%s/%s/%s", property, cluster, namespace);
   ConnectionFactoryBuilder connectionFactoryBuilder = new DefaultConnectionFactoryBuilder(lookupServiceHostPortUrl, principal, fullyQualifiedNamespace);
   CMSSpout s = new CMSSpout(connectionFactoryBuilder, fullyQualifiedNamespace, topic);
   builder.setSpout(”cms", s, 5);

Redis Spout
-----------

Old example that needs a few updates to work with newer storm release. http://tiny.corp.yahoo.com/BPQCDA

In the example code please don’t sleep if the queue is empty (Storm will do that for you)

.. code-block:: java

   RedisPubSubSpout s = new RedisPubSubSpout (host, port, pattern);
   builder.setSpout(“redis", s, 1);

Using Yahoo Bolts
=================

HDFS Bolt
---------

Should be officially supported soohttps://github.com/apache/incubator-storm/pull/128

.. code-block:: java


    // use "|" instead of "," for field delimiter
    RecordFormat format = new DelimitedRecordFormat()
    .withFieldDelimiter("|");
    
    // sync the filesystem after every 1k tuples
    SyncPolicy syncPolicy = new CountSyncPolicy(1000);
    
    // rotate files when they reach 5MB
    FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);
    FileNameFormat fileNameFormat = new DefaultFileNameFormat() 
    .withPath("/foo/");
    
    HdfsBolt bolt = new HdfsBolt()
    .withFsUrl("hdfs://localhost:54310”)
    .withFileNameFormat(fileNameFormat)
    .withRecordFormat(format)
    .withRotationPolicy(rotationPolicy)
    .withSyncPolicy(syncPolicy);

HBase Bolt
----------

Example at http://tiny.corp.yahoo.com/3qM6Bg

.. code-block:: java

   public static class HBaseInjectionBolt extends BaseRichBolt {
       ... 
       @Override
       public void prepare(Map storm_conf, TopologyContext context, OutputCollector collector) {
           this.table = new HTable(hbase_conf, table_name);
       }
        
       @Override
       public void execute(Tuple tuple) {
           String word = (String)tuple.getValue(0);
           Put row = new Put(Bytes.toBytes(word));
           String val = new Date().toString();
           row.add(FAMILY, COLUMN, Bytes.toBytes(val));
           table.put(row);
       }
   }

Using Trident With Storm
========================

Provides exactly once semantics like transactional topologies.
In trident state is a first class citizen, but the exact implementation of state is up to you.
There are many prebuilt connectors to various NoSQL stores like HBase
Provides a high level API (similar to cascading for Hadoop)


Example
-------
Aggregates values and stores them.

.. code-block:: java

   TridentTopology topology =  new TridentTopology();        
   TridentState wordCounts =
     topology.newStream("spout1", spout)
       .each(new Fields("sentence"), new Split(), new Fields("word"))
       .groupBy(new Fields("word"))
       .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
       .parallelismHint(6);



Acking not required.

.. code-block:: java

   public class Split extends BaseFunction {

       public void execute(TridentTuple tuple, TridentCollector collector) {
           String sentence = tuple.getString(0);
           for (String word: sentence.split(" ")) {
               collector.emit(new Values(word));                
           }
       }
   }

Distributed Remote Procedural Calls (DRPC)
==========================================

Turns a RPC call into a tuple sent from a spout
Takes a result from that and sends it back to the user.

Example
-------

Client
######

.. code-block:: java

   DRPCClient client = new DRPCClient("drpc.server.location", 3772);
   System.out.println(client.execute("words", "cat dog the man");
   // prints the JSON-encoded result, e.g.: "[[5078]]"


Topology
########

.. code-block:: java

   topology.newDRPCStream("words")
       .each(new Fields("args"), new Split(), new Fields("word"))
       .groupBy(new Fields("word"))
       .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
       .each(new Fields("count"), new FilterNull())
       .aggregate(new Fields("count"), new Sum(), new Fields("sum"));



REST DRPC
---------

Support HTTP requests to DRPC servers.DRPC server will receive GET requests in 
the following format: ``http://<DRPC_host>:<HTTP_port>/drpc/<FunctionName>/<Arguments>;``

.. note:: Not in Apache yet.

Client
######

::

    # prints the JSON-encoded result, just like normal DRPC
    curl http://drpc.server.location:3773/drpc/words/cat%20dog%20the%20man



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

Integrate the Storm Topology with Data Highway Rainbow pipeline for audience and 
ads data.

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



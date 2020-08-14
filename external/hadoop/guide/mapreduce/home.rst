********
Overview
********

MapReduce is a framework using which we can write applications to process huge amounts of data, in parallel, on large clusters of commodity hardware in a reliable manner.

MapReduce is a processing technique and a program model for distributed computing based on java. The MapReduce algorithm contains two important tasks, namely Map and Reduce. Map takes a set of data and converts it into another set of data, where individual elements are broken down into tuples (key/value pairs). Secondly, reduce task, which takes the output from a map as an input and combines those data tuples into a smaller set of tuples. As the sequence of the name MapReduce implies, the reduce task is always performed after the map job.

The major advantage of MapReduce is that it is easy to scale data processing over multiple computing nodes. Under the MapReduce model, the data processing primitives are called mappers and reducers. Decomposing a data processing application into mappers and reducers is sometimes nontrivial. But, once we write an application in the MapReduce form, scaling the application to run over hundreds, thousands, or even tens of thousands of machines in a cluster is merely a configuration change. This simple scalability is what has attracted many programmers to use the MapReduce model.

Typically the compute nodes and the storage nodes are the same, that is, the MapReduce framework and the `Hadoop Distributed File System` (see HDFS :ref:`hdfs_overview`) are running on the same set of nodes. This configuration allows the framework to effectively schedule tasks on the nodes where data is already present, resulting in very high aggregate bandwidth across the cluster.

.. admonition:: Reading...
   :class: readingbox

   .. include:: /common/mapreduce/mapreduce-reading-resources.rst

.. topic:: Definitions
  :class: definitionbox

  .. glossary::

     Map stage
       The map or mapper's job is to process the input data. Generally the input data is in the form of file or directory and is stored in the Hadoop file system (HDFS). The input file is passed to the mapper function line by line. The mapper processes the data and creates several small chunks of data.
      
     Reduce stage
       This stage is the combination of the Shuffle stage and the Reduce stage. The Reducer's job is to process the data that comes from the mapper. After processing, it produces a new set of output, which will be stored in the HDFS.

.. rubric:: The Algorithm

* Generally MapReduce paradigm is based on sending the computer to where the data resides.
* MapReduce program executes in three stages, namely :term:`Map stage`, shuffle stage, and :term:`Reduce stage`.
* During a MapReduce job, Hadoop sends the Map and Reduce tasks to the appropriate servers in the cluster.
* The framework manages all the details of data-passing such as issuing tasks, verifying task completion, and copying data around the cluster between the nodes.
* Most of the computing takes place on nodes with data on local disks that reduces the network traffic.
* After completion of the given tasks, the cluster collects and reduces the data to form an appropriate result, and sends it back to the Hadoop server.

.. rubric:: Inputs and Outputs (Java Perspective)

The MapReduce framework operates on <key, value> pairs, that is, the framework views the input to the job as a set of <key, value> pairs and produces a set of <key, value> pairs as the output of the job, conceivably of different types.

The key and the value classes should be in serialized manner by the framework and hence, need to implement the Writable interface. Additionally, the key classes have to implement the Writable-Comparable interface to facilitate sorting by the framework. Input and Output types of a MapReduce job − .
 
  .. code-block:: rst

    (Input) <k1, v1> → map → <k2, v2> → reduce → <k3, v3>(Output).


.. topic:: Terminology
   :class: definitionbox

   .. glossary::

      Mapper
        It maps input key/value pairs to a set of intermediate key/value pairs.

      Reducer
        It reduces a set of intermediate values which share a key to a smaller set of values.
      
      Suffle
        Input to the Reducer is the sorted output of the mappers. In this phase the framework fetches the relevant partition of the output of all the mappers, via HTTP. Reducer has 3 primary phases: `shuffle`, `sort` and `reduce`.
      
      Sort
        The framework groups Reducer inputs by keys (since different mappers may have output the same key) in this stage. |br| The shuffle and sort phases occur simultaneously; while map-outputs are being fetched they are merged.
      
      Secondary Sort
        If equivalence rules for grouping the intermediate keys are required to be different from those for grouping keys before reduction, then one may specify a `Comparator` which controls how intermediate keys are grouped.
      
      Reduce
        In this phase the ``reduce(WritableComparable, Iterable<Writable>, Context)`` method is called for each ``<key, (list of values)>`` pair in the grouped inputs. The output is not `Sorted`
      
      Reducer NONE		
        It is legal to set the number of reduce-tasks to zero if no reduction is desired.
      
      Partitioner
        partitions the key space. It controls the partitioning of the keys of the intermediate map-outputs. The key (or a subset of the key) is used to derive the partition, typically by a hash function. The total number of partitions is the same as the number of reduce tasks for the job.
      
      Counter
        Counter is a facility for MapReduce applications to report its statistics. Mapper and Reducer implementations can use the Counter to report statistics.

      Job
        is the top unit of work in the MapReduce process. It is an assignment that Map and Reduce processes need to complete. A job is divided into smaller tasks over a cluster of machines for faster execution.

      Task
        A task in MapReduce is an execution of a Mapper or a Reducer on a slice of data. It is also called Task-In-Progress (TIP). It means processing of data is in progress either on mapper or reducer.

      Task Attempt
        Task Attempt is a particular instance of an attempt to execute a task on a node. There is a possibility that anytime any machine can go down. For example, while processing data if any node goes down, framework reschedules the task to some other node. This rescheduling of the task cannot be infinite. There is an upper limit for that as well. The default value of task attempt is 4. If a task (Mapper or reducer) fails 4 times, then the job is considered as a failed job. For high priority job or huge job, the value of this task attempt can also be increased.

      NameNode
   	    Node that manages Hadoop Distributed File System (HDFS). This is also known as `Master`. (See :numref:`hdfs_overview_nn`)

      DataNode
        Node where data is presented in advance before any processing takes place. DataNode is responsible for storing the actual data in HDFS. (See :numref:`hdfs_overview_nn`)

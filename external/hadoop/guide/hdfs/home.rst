.. _hdfs_overview:

********
Overview
********

.. contents:: Table of Contents
  :local:
  :depth: 4

-----------

Architecture
============

HDFS is highly fault-tolerant and is designed to be deployed on low-cost hardware.
HDFS provides high throughput access to application data and is suitable for
applications that have large data sets.
HDFS relaxes a few POSIX requirements to enable streaming access to file system
data.

.. topic:: Terminology
   :class: definitionbox

   .. glossary::

      Block
        Files in HDFS are broken into block-sized chunks called data blocks.
        |br| These blocks are stored as independent units. Hadoop distributes
        these blocks on different slave machines (:term:`DataNode`),
        and the master machine (:term:`NameNode`) stores the metadata about
        blocks location. See :numref:`hdfs_overview_data_replication`. |br|
        Applications that are compatible with HDFS are those that deal with
        large data sets. These applications write their data only once but they
        read it one or more times and require these reads to be satisfied at
        streaming speeds.
        |br| HDFS supports write-once-read-many semantics on files.
        A typical block size used by HDFS is `128 MB`.
        Thus, an HDFS file is chopped up into `128 MB` chunks, and if possible,
        each chunk will reside on a different DataNode.
       
      Replication Factor
        The number of copies of a file. This information is stored by the
        :term:`NameNode`. It is commonly set to `3`.
        See :numref:`hdfs_overview_data_replication` for more details.
       
      Blockreport
        :term:`DataNode` sends periodically to the :term:`NameNode` a report that
        contains a list of all blocks on a DataNode.

      Safemode
        On startup, the :term:`NameNode` enters a special state called `Safemode`.
        Replication of data blocks does not occur when the :term:`NameNode` is in the
        `Safemode` state. (see :numref:`hdfs_overview_data_replication`)

      EditLog
        Is a transaction log used by the :term:`NameNode` to persistently record
        every change that occurs to file system metadata. It is stored as a file
        in the NameNode’s local file system.
        (see :numref:`hdfs_overview_presistence`)

      FsImage
        Is stored as a file in the NameNode’s local file system.
        It contains mapping of blocks to files and file system properties.

      Replication Pipelining
        This is the terminology to describe that :term:`DataNode` can be
        receiving data from the previous one in the pipeline and
        at the same time forwarding data to the next one in the pipeline. Thus,
        the data is `pipelined` from one :term:`DataNode` to the next. |br|
        When a client is writing data to an HDFS file with a
        :term:`Replication Factor` of three, the NameNode retrieves a list of
        DataNodes using a replication target choosing algorithm.
        This list contains the DataNodes that will
        host a replica of that block. The client then writes to the first
        DataNode. |br| The first DataNode starts receiving the data in portions,
        writes each portion to its local repository and transfers that portion
        to the second DataNode in the list.
        |br| The second DataNode, in turn starts
        receiving each portion of the data block, writes that portion to its
        repository and then flushes that portion to the third DataNode.
        |br| Finally,
        the third DataNode writes the data to its local repository.

Main Concepts and Assumptions
-----------------------------

.. rubric:: Hardware Failure

Hardware failure is the norm rather than the exception. An HDFS instance may
consist of hundreds or thousands of server machines, each storing part of the
file system’s data. The fact that there are a huge number of components and that
each component has a non-trivial probability of failure means that some
component of HDFS is always non-functional. Therefore, detection of faults and
quick, automatic recovery from them is a core architectural goal of HDFS.

.. rubric:: Moving Computation is Cheaper than Moving Data

A computation requested by an application is much more efficient if it is
executed near the data it operates on. This is especially true when the size of
the data set is huge. This minimizes network congestion and increases the
overall throughput of the system. The assumption is that it is often better to
migrate the computation closer to where the data is located rather than moving
the data to where the application is running. HDFS provides interfaces for
applications to move themselves closer to where the data is located.


.. rubric:: Simple Coherency Model

HDFS applications need a write-once-read-many access model for files. A file
once created, written, and closed need not be changed except for appends and
truncates. Appending the content to the end of the files is supported but cannot
be updated at arbitrary point. This assumption simplifies data coherency issues
and enables high throughput data access. A MapReduce application or a web
crawler application fits perfectly with this model.


.. rubric:: Streaming Data Access

Applications that run on HDFS need streaming access to their data sets. They are
not general purpose applications that typically run on general purpose file
systems. HDFS is designed more for batch processing rather than interactive use
by users. The emphasis is on high throughput of data access rather than low
latency of data access. POSIX imposes many hard requirements that are not needed
for applications that are targeted for HDFS. POSIX semantics in a few key areas
has been traded to increase data throughput rates.

.. rubric:: Large Data Sets

Applications that run on HDFS have large data sets. A typical file in HDFS is
gigabytes to terabytes in size. Thus, HDFS is tuned to support large files. It
should provide high aggregate data bandwidth and scale to hundreds of nodes in a
single cluster. It should support tens of millions of files in a single
instance.

.. _hdfs_overview_nn:

NameNode and DataNodes
----------------------

HDFS has a master/slave architecture. An HDFS cluster consists of a single
:term:`NameNode` , a master server that manages the file system namespace and
regulates access to files by clients. In addition, there are a number of
`DataNodes`, usually one per node in the cluster, which manage storage
attached to the nodes that they run on.

The existence of a single :term:`NameNode` in a cluster greatly simplifies the
architecture of the system. The :term:`NameNode` is the arbitrator and
repository for all HDFS metadata. The system is designed in such a way that user
data never flows through the :term:`NameNode`.


HDFS exposes a file system namespace and allows user data to be stored in files.
Internally, a file is split into one or more blocks and these blocks are stored
in a set of DataNodes. The :term:`NameNode` executes file system namespace
operations like opening, closing, and renaming files and directories. It also
determines the mapping of blocks to DataNodes.

The DataNodes are responsible for serving read and write requests from
the file system’s clients. The DataNodes also perform block creation, deletion,
and replication upon instruction from the NameNode.


The NameNode and DataNode are pieces of software designed to run on commodity
machines. These machines typically run a GNU/Linux operating system (OS). HDFS
is built using the Java language; any machine that supports Java can run the
:term:`NameNode` or the :term:`DataNode` software.

Usage of the highly portable Java language means that HDFS can be deployed on a
wide range of machines. A typical deployment has a dedicated machine that runs
only the :term:`NameNode` software. Each of the other machines in the cluster
runs one instance of the :term:`DataNode` software. The architecture does not
preclude running multiple DataNodes on the same machine but in a real deployment
that is rarely the case.

.. rubric:: Communication Protocols

All HDFS communication protocols are layered on top of the TCP/IP protocol. A
client establishes a connection to a configurable TCP port on the NameNode
machine. It talks the `ClientProtocol` with the NameNode. The DataNodes talk to
the NameNode using the DataNode Protocol. |br|
A Remote Procedure Call (RPC) abstraction wraps both the Client Protocol and
the DataNode Protocol.

By design, the NameNode never initiates any RPCs. Instead, it only responds to
RPC requests issued by DataNodes or clients.

.. _fig-hdfs-overview-architecture:

.. figure:: https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-hdfs/images/hdfsarchitecture.png
  :alt: progress rate of a reduce task
  :width: 100%
  :align: center

  HDFS Architecture. `Source from Apache Hadoop Docs -` :hadoop_rel_doc:`hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#NameNode_and_DataNodes>`.


The File System Namespace
=========================

HDFS supports a traditional hierarchical file organization. |br|
A user or an application can create directories and store files inside these
directories. |br| The file system namespace hierarchy is similar to most other
existing file systems; one can create and remove files, move a file from one
directory to another, or rename a file.

HDFS supports :hadoop_rel_doc:`user quotas <hadoop-project-dist/hadoop-hdfs/HdfsQuotaAdminGuide.html>`
and :hadoop_rel_doc:`access permissions <hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`.
HDFS does not support hard links or soft links.

HDFS has rserves some paths and names (e.g. "`/.reserved`" and "`.snapshot`").
Features such as
:hadoop_rel_doc:`transparent encryption <hadoop-project-dist/hadoop-hdfs/TransparentEncryption.html>`
and :hadoop_rel_doc:`snapshot <hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html>`
use reserved paths.

The :term:`NameNode` maintains the file system namespace. Any change to the
file system namespace or its properties is recorded by the NameNode. |br|
The number of copies of a file is called the :term:`Replication Factor` of
that file. This information is stored by the :term:`NameNode`.

.. _hdfs_overview_data_replication:

Data Replication   
================

HDFS is designed to reliably store very large files across machines in a large
cluster. It stores each file as a sequence of blocks. The blocks of a file are
replicated for fault tolerance. The block size and replication factor are
configurable per file.

All blocks in a file except the last block are the same size, while users can
start a new block without filling out the last block to the configured block
size after the support for variable length block was added to append and hsync.

An application can specify the number of replicas of a file. The replication
factor can be specified at file creation time and can be changed later. Files in
HDFS are write-once (except for appends and truncates) and have strictly one
writer at any time.

The :term:`NameNode` makes all decisions regarding replication of blocks. It
periodically receives a Heartbeat and a :term:`Blockreport` from each of the
DataNodes in the cluster. |br|
*Receipt* of a Heartbeat implies that the :term:`DataNode` is
functioning properly. A Blockreport contains a list of all blocks on a DataNode.


.. _fig-hdfs-architecture-dn-replicas:

.. figure:: https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-hdfs/images/hdfsdatanodes.png
  :alt: progress rate of a reduce task
  :width: 100%
  :align: center

  HDFS DataNode and Block Replication.
  `Source from Apache Hadoop Docs -` :hadoop_rel_doc:`HDFS Architecture <hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Replication>`.

Replica Placement
-----------------

The current implementation for the replica placement policy is a rack-aware
replica placement policy is to improve data reliability, availability, and
network bandwidth utilization. The :term:`NameNode` determines the `rack-id`
each DataNode belongs to via the process outlined in 
:hadoop_rel_doc:`Hadoop Rack Awareness <hadoop-project-dist/hadoop-common/RackAwareness.html>`.
This policy evenly distributes replicas in the cluster which makes it easy to
balance load on component failure. However, this policy increases the cost of
writes because a write needs to transfer blocks to multiple racks.


For the common case, when the replication factor is three, HDFS’s placement
policy is to put one replica on the local machine if the writer is on a
datanode, otherwise on a random datanode, another replica on a node in a
different (remote) rack, and the last on a different node in the same remote
rack. This policy cuts the inter-rack write traffic which generally improves
write performance.

The chance of rack failure is far less than that of node
failure; this policy does not impact data reliability and availability
guarantees. However, it does reduce the aggregate network bandwidth used when
reading data since a block is placed in only two unique racks rather than three.
With this policy, the replicas of a file do not evenly distribute across the
racks.

One third of replicas are on one node, two thirds of replicas are on one
rack, and the other third are evenly distributed across the remaining racks.
This policy improves write performance without compromising data reliability or
read performance.

.. note:: If the replication factor is greater than `3`, the placement of the
   4:sup:`th` and following replicas are determined randomly while keeping the number
   of replicas per rack below the upper limit, which is basically
   :math:`\left(\dfrac{\textit{replicas} - 1}{racks} + 2\right)`.

The :term:`NameNode` chooses nodes based on rack awareness at first, then checks
that the candidate node have storage required by the policy associated with the
file. If the candidate node does not have the storage type, the NameNode looks
for another node. If enough nodes to place replicas can not be found in the
first path, the :term:`NameNode` looks for nodes having fallback storage types
in the second path.
(See Apache Hadoop Docs - :hadoop_rel_doc:`Support for Storage Types and Policies <hadoop-project-dist/hadoop-hdfs/ArchivalStorage.html>`)


Replica Selection
-----------------

To minimize global bandwidth consumption and read latency, HDFS tries to satisfy
a read request from a replica that is closest to the reader. If there exists a
replica on the same rack as the reader node, then that replica is preferred to
satisfy the read request. If HDFS cluster spans multiple data centers, then a
replica that is resident in the local data center is preferred over any remote
replica.

.. _hdfs_overview_data_replication_safemode:

Safemode
--------

The :term:`NameNode` receives `Heartbeat` and :term:`Blockreport` messages from
the DataNodes. A :term:`Blockreport` contains the list of data blocks that a
:term:`DataNode` is hosting. Each block has a specified minimum number of
replicas. A :term:`Block` is considered `safely replicated` when the minimum
number of replicas of that data block has checked in with the :term:`NameNode`.

After a configurable percentage of safely replicated data blocks checks in with
the :term:`NameNode` (plus an additional 30 seconds), the :term:`NameNode` exits
the Safemode state. It then determines the list of data blocks (if any) that
still have fewer than the specified number of replicas. The :term:`NameNode`
then replicates these blocks to other DataNodes.


.. _hdfs_overview_presistence:

The Persistence of File System Metadata
=======================================

.. sidebar:: Robustness ...

   Read more details about persistence and reliability in different types of
   failures. |br|
   Apache Docs - :hadoop_rel_doc:`HDFS Robustness <hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Robustness>`

The HDFS namespace is stored by the :term:`NameNode`. The NameNode records file
system metadata changes in the :term:`EditLog`. For example, creating a new
file in HDFS causes the NameNode to insert a record into the :term:`EditLog`
indicating this. Similarly, changing the replication factor of a file causes a
new record to be inserted into the EditLog. 


Resources
=========

.. include:: /common/hdfs/hdfs-reading-resources.rst




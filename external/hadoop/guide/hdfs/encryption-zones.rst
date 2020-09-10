.. _hdfs_ez:

****************
Encryption Zones
****************


.. contents:: Table of Contents
  :local:
  :depth: 4

-----------

Overview
========

HDFS implements transparent, end-to-end encryption. Once configured, data read
from and written to special HDFS directories is transparently encrypted and
decrypted without requiring changes to user application code.

This encryption is *end-to-end*, which means the data can only be encrypted and
decrypted by the client. |br|
HDFS never stores or has access to unencrypted data or unencrypted data
encryption keys. This satisfies two typical requirements for encryption:

* `storage encryption`: meaning data on persistent media, such as a disk; and
* `transit encryption`: when data is travelling over the network.


A brief list of features is as follows:

* End-to-end transparent encryption - transit + storage
* Algorithm: AES/CTR/NoPadding, Block size: 16 bytes (128 bits)
* Key granularity is at file level - Each file has unique DEK
* Keys stored in CKMS (VZM’s central key management service)
* Less than 10% performance degradation, many jobs far less
* Mixed-mode is supported - Enable EZ on existing data directory
  
The followiing functionalities are *not supported*:

* REST access via webhdfs (e.g. curl)
* Webhdfs writes 
* Nested encryption zones
* Key re-encrypt in the event of compromised key - data copy is required
* Renames across zones


Best Practices
==============

.. rubric:: If your data is replicated to one or more clusters

.. _fig-hdfs-ez-dependency:

.. figure:: /images/hdfs/hdfs-ez-dependency-tree.png
  :alt: EZ dependency tree
  :width: 100%
  :align: center


* If your data is replicated to one or more clusters

   #. Create a dependency tree and enable EZs from the leaves upward.
      In the example shown in :numref:`fig-hdfs-ez-dependency`,
      enable encryption in the following order: Cluster 2, Cluster 1, Cluster 3, and
      finally Source Cluster. If you do not follow this order, GDM and/or DISTCP will
      refuse to copy your data which could result in a production incident.
   #. The Source and Destination of a GDM replication
      should use the same Encryption Zone (same key). If they are different, GDM by
      default will refuse to copy the data. Paranoid approval is required to override
      this default behavior.

* Do not create fine-grained encryption zones. The main intention is for
  encryption zones to provide end to end hdfs encryption, not access control.
  Encryption zones should be created at the `/projects/foo` or
  `/projects/foo/sub_foo1` levels.
* Nested encryption zones aren't validated and therefore aren't a supported
  configuration.
* Limit the number of proxy-users (e.g. hdfsproxy, oozie, sqoop, etc.)
  having access to the keys. proxy-users have significant power so if `sqoop`
  (by example) does not need to access your data, don't give sqoop access to the
  keys.
* If a key needs to be rotated, file a ticket at yo/hadooppf. Newly written data
  will use new key, older data will still be readable. The key management service
  used at VZM (CKMS), has a limited number of key versions supported.
  It is important when rolling keys to verify there is no data currently using
  a key that is older than this limit. Failing to do so could result in un-readable
  data.

.. important:: Data is never allowed to be copied from an encryption zone to a
               non encrypted area, hence the ordering requirement.


On Boarding
===========

* File request in `Doppler <yo/doppler-ez>`_ specifying:

  #. `[cluster,path] pairs` (e.g. `[kessel.vcg , /projects/foo]`)
  #. Headless users and groups that need access to the keys
  #. all the services (from hdfs proxy, gdm, oozie, hcat, hive server, hue)
     that will access your encrypted directory.

* Converting an existing data set to be encrypted:

  #. Assuming the data set has a retention policy (i.e. old data is constantly
     aging out as new data arrives) - the simplest approach is to just enable the
     encryption zone. Newly written data will be encrypted so after a
     retention-period number of days, all data will be encrypted.
  #. If it is not possible to just let data naturally become encrypted, the other
     option is to move the data to a new location, then create empty EZ, then
     distcp data back into EZ.  

.. _fig-hdfs-ez-write-workflow:

.. figure:: /images/hdfs/hdfs-ez-write-workflow.png
  :alt: EZ write Workflow
  :width: 100%
  :align: center

  Write workflow (detailed)


.. _fig-hdfs-ez-read-workflow:

.. figure:: /images/hdfs/hdfs-ez-read-workflow.png
  :alt: EZ read Workflow
  :width: 100%
  :align: center

  Read workflow (detailed)


References
==========

* `Internal HDFS Encryption Zones Slides <https://docs.google.com/presentation/d/1CDPImcXGGGxMR3lBp-8WAnVUYS7TVaGicSTnPM7x6Qw/edit#slide=id.g1d5caaedb5_0_54>`_
* Apache Docs - :hadoop_rel_doc:`Transparent Encryption in HDFS <hadoop-hdfs/TransparentEncryption.html>`
* `Presentation slides by Cloudera <https://www.slideshare.net/Hadoop_Summit/transparent-encryption-in-hdfs>`_
* `Doppler EZ Home <yo/doppler-ez>`_
* `Search For Doppler EZ zones <yo/doppler-ez-search>`_
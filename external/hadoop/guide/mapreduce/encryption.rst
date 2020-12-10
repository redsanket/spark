..  _mapreduce_encryption:

******************************************
Encryptionn 
******************************************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------


.. important::
   * Encrypting intermediate data (shuffle/spill) will incur in a
     significant performance impact. |br|
     Users should profile this and potentially reserve 1 or more cores for
     encrypted shuffle.
   * Enabling encryption for intermediate data spills will restrict the number
     of attempts for a job to 1.

.. _mapreduce_shuffle_encryption:

Encrypted Shuffle
=================

The Encrypted Shuffle capability allows encryption of the MapReduce shuffle
using HTTPS and with optional client authentication (also known as
bi-directional HTTPS, or HTTPS with client certificates). It comprises:

* A Hadoop configuration setting for toggling the shuffle between HTTP and HTTPS.
* A Hadoop configuration settings for specifying the keystore and truststore
  properties (location, type, passwords) used by the shuffle service and the
  reducers tasks fetching shuffle data.
* A way to re-load truststores across the cluster (when a node is added or
  removed).


To enable encrypted shuffle, set ``mapreduce.shuffle.ssl.enabled`` to true
in ``mapred-site.xml`` of all nodes in the cluster. The default is false.


To configure encrypted shuffle, set the following properties ``in core-site.xml``
of all nodes in the cluster:

.. table:: `core-site.xml Properties for shuffle encryption. Prefix hadoop.ssl`
  :widths: auto
  :name: table-mr-shuffle-configs

  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | Property                    | Default                       | Description                                                                             |
  +=============================+===============================+=========================================================================================+
  | ``require.client.cert``     | false                         | Whether client certificates are required                                                |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | ``hostname.verifier``       | DEFAULT                       | The hostname verifier to provide for `HttpsURLConnections`. |br|                        |
  |                             |                               | Valid values are: |br|                                                                  |
  |                             |                               | `DEFAULT`, `STRICT`, `STRICT_IE6`, `DEFAULT_AND_LOCALHOST` and `ALLOW_ALL`              |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | ``keystores.factory.class`` | ``FileBasedKeyStoresFactory`` | The KeyStoresFactory implementation to use                                              |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | ``server.conf``             | ``ssl-server.xml``            | Resource file from which ssl server keystore information will be extracted. |br|        |
  |                             |                               | This file is looked up in the classpath, it should be in Hadoop ``conf/`` directory     |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | ``client.conf``             | ``ssl-client.xml``            | Resource file from which ssl server keystore information will be extracted. |br|        |
  |                             |                               | This file is looked up in the classpath, it should be in Hadoop ``conf/`` directory     |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+
  | ``enabled.protocols``       | TLSv1.2                       | The supported SSL protocols. The parameter will only be used from `DatanodeHttpServer`. |
  +-----------------------------+-------------------------------+-----------------------------------------------------------------------------------------+

.. note:: 
    * All these properties should be marked as final in the cluster configuration
      files including ``mapreduce.shuffle.ssl.enabled``.
    * The Linux container executor should be set to prevent job tasks from
      reading the server keystore information and gaining access to the shuffle
      server certificates.
    * Currently requiring client certificates should be set to false.
    * Refer to
      :ref:`the Client Certificates section <mapreduce_shuffle_client_certificates>`
      for details



.. _mapreduce_shuffle_keystore_settings:

Keystore and Truststore Settings
--------------------------------

Currently ``FileBasedKeyStoresFactory`` is the only ``KeyStoresFactory``
implementation. The ``FileBasedKeyStoresFactory`` implementation uses the
following properties (:numref:`table-mr-shuffle-ssl-server`)
in the ``ssl-server.xml`` and ``ssl-client.xml`` files,
to configure the keystores and truststores.


The mapred user should own the ``ssl-server.xml`` file and have exclusive read
access to it.


.. table:: `ssl-server.xml. Prefix ssl.server`
  :widths: auto
  :name: table-mr-shuffle-ssl-server

  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | Property                       | Default | Description                                                                                          |
  +================================+=========+======================================================================================================+
  | ``keystore.type``              | jks     | Keystore file type                                                                                   |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``keystore.location``          | NONE    | Keystore file location. The mapred user should own this file and have exclusive read access to it.   |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``keystore.password``          | NONE    | Keystore file password                                                                               |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``truststore.type``            | jks     | Truststore file type                                                                                 |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``truststore.location``        | NONE    | Truststore file location. The mapred user should own this file and have exclusive read access to it. |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``truststore.password``        | NONE    | Truststore file password                                                                             |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+
  | ``truststore.reload.interval`` | 10000   | Truststore reload interval, in milliseconds                                                          |
  +--------------------------------+---------+------------------------------------------------------------------------------------------------------+  


The mapred user should own the ``ssl-client.xml`` file and it should have default
permissions. The values in that file are the same as descrived in
:numref:`table-mr-shuffle-ssl-server`.

.. _mapreduce_shuffle_client_certificates:

Client Certificates
-------------------

Using Client Certificates does not fully ensure that the client is a reducer
task for the job. Currently, Client Certificates (their private key) keystore
files must be readable by all users submitting jobs to the cluster. |br|
This means that a rogue job could read those keystore files and use the client
certificates in them to establish a secure connection with a Shuffle server.
However, unless the rogue job has a proper `JobToken`, it won’t be able to
retrieve shuffle data from the Shuffle server. A job, using its own `JobToken`,
can only retrieve shuffle data that belongs to itself.

By default the truststores will reload their configuration every 10 seconds.
If a new truststore file is copied over the old one, it will be re-read,
and its certificates will replace the old ones. |br|
This mechanism is useful for adding or removing nodes from the cluster,
or for adding or removing trusted clients. |br|
In these cases, the client or NodeManager certificate is added to
(or removed from) all the truststore files in the system, and the new
configuration will be picked up without you having to restart the `NodeManager`
daemons.


.. _mapreduce_spill_encryption:

Encrypted Spill
=================

A `spill` is when a mapper's output exceeds the amount of memory which was
allocated for the MapReduce task. **Spilling** happens when there is not enough
memory to fit all the mapper output.

MapReduce v2 allows to encrypt intermediate files generated during encrypted
shuffle and in case of data spills during the map and reduce stages.

This can be enabled by setting the following properties in ``mapred-site.xml``.

.. table:: `Intermediate Data Spill configuration in MR. Prefixed mapreduce.job`
  :widths: auto
  :name: table-mr-spill-configs

  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | Configuration                                 | Type    | Description                                                                                                                    | Default |
  +===============================================+=========+================================================================================================================================+=========+
  | ``encrypted-intermediate-data``               | boolean | Enable or disable encrypt intermediate mapreduce spill files.                                                                  | False   |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``spill-encryption-keyprovider.class``        | string  | The class used as a spill key provider. This defines the encryption algorithm and key provider to get the client certificates. | ""      |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``encrypted-intermediate-data-key-size-bits`` | int     | The key length used to encrypt data spilled to disk.                                                                           | 128     |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``encrypted-intermediate-data.buffer.kb``     | int     | The buffer size in kb for stream written to disk after encryption                                                              | 128     |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+

Available implementation for ``spill-encryption-keyprovider.class``:

* ``org.apache.hadoop.mapreduce.NullSpillKeyProvider``: a null encryption provider 
* ``org.apache.hadoop.mapreduce.DefaultSpillKeyProvider``: an implementation that uses
  ``HmacSHA1`` algo for encryption with a key length defined as
  ``encrypted-intermediate-data-key-size-bits``.
* ``org.apache.hadoop.mapreduce.KMSSpillKeyProvider``: KMS is used to provide
  intermediate encryption. In that case, the configuration
  ``mapreduce.job.kms-encryption-key-name`` provides the kms-key.


.. _mapreduce_encryption_evaluation:

Performance Evaluation
======================

A `performance evaluation dated May 22nd 2019 <https://docs.google.com/spreadsheets/d/1dFdW3KrZD55rZo69oPaaZqcr1sr74SAsiNu5tSojCxk/edit#gid=2038478652>`_ of ``TeraSort``


Characterestics:
  * 11 GB and 20 GB shuffle 
  * To get the worst case scenario, the sort buffer size and factors were lowered

Results (the ones measured with smaller buffers):
  * 0.46%-9.3% (AVG: 2.45%) slow down post encryption feature;
  * Average Map times are in the 1.05-7% (AVG: 3.023%)
  * Average Reduce time change is around 0.85% -13% (AVG: 4.72%);
  * Total deterioration in shuffle and merge phase is ~ 4% in the worst case;
  * CPU times increases by 2.97%-10% (AVG: 4.43%)
  * CPU time degrades in the range of 0.7-2.26% (AVG: 1.55%).


Resources
=========

.. include:: /common/mapreduce/encryption-reading-resources.rst


..  _tez_encryption:

**********
Encryption
**********

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

.. warning::
   * Using ``DefaultSpillKeyProvider`` is **ONLY** meant for testing purposes.
   * Encrypting intermediate data (shuffle/spill) will incur a
     significant performance impact. |br|
     Users should profile this and potentially reserve 1 or more cores for
     encrypted spill.
   * Enabling encryption for intermediate data spills automatically restricts
     the number of attempts for a job to 1
     (a.k.a, ``TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS``).


Similar to :ref:`Mapreduce Encryption <mapreduce_encryption>`,  this capability
allows encryption of the intermediate files generated during the shuffle/spill.


Tez Shuffle
===========

.. sidebar:: More Details ...

   * :ref:`Mapreduce Encryption <mapreduce_shuffle_encryption>`
   * :ref:`What is Shuffling <mapreduce_encryption_definitions>`

Tez uses ``org.apache.hadoop.mapred.ShuffleHandler`` provided by MapReduce version
2.0 (MRv2) as an auxiliary service, which you can choose to configure via the
`hadoop/mapred-site.xml` file.

On a secured cluster, Tez shuffle, SSL encryption configuration is enabled in
`conf/tez-site.xml` by setting ``tez.runtime.shuffle.ssl.enable`` to true. 
Also, Tez shuffle for YARN must be configured by setting
``mapreduce.shuffle.ssl.enabled`` in `mapred-site.xml` file
(see :numref:`mapreduce_shuffle_encryption`).


Tez Spill
=========

It can be enabled by setting the `tez.am.encrypted-intermediate-data` job property
to true.

.. sidebar:: More Details ...

   * :ref:`What is Spilling <mapreduce_spill_encryption>`
   * :ref:`Shuffling Vs. Spilling <mapreduce_encryption_definitions>`
   * :ref:`What to expect with Encrypted Spilling <mapreduce_spill_encryption_expectations>`

.. _tez_spill_encryption_config:

Configuration
-------------

Intermediate encryption of a Tez job can be configured using the list of values
listed in :numref:`table-tez-spill-configs`.

.. _tez_spill_encryption:

.. table:: `TEZ Configuration AM level for Intermediate Data Encryption prefix 'tez.am'`
  :widths: auto
  :name: table-tez-spill-configs

  +---------------------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------------------------+
  | Configuration                                           | Type    | Description                                                                                                                    | Default                   |
  +=========================================================+=========+================================================================================================================================+===========================+
  | ``encrypted-intermediate`` |br| ``-data``               | boolean | Enable/Disable intermediate data encryption.                                                                                   | False                     |
  +---------------------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------------------------+
  | ``encrypted-intermediate`` |br| ``-data.class``         | string  | The class used as a spill key provider. This defines the encryption algorithm and key provider to get the client certificates. | `DefaultSpillKeyProvider` |
  +---------------------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------------------------+
  | ``encrypted-intermediate`` |br| ``-data-key-size-bits`` | int     | The key length used to encrypt data spilled to disk.                                                                           | 128                       |
  +---------------------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------------------------+
  | ``encrypted-intermediate`` |br| ``-data.buffer.kb``     | int     | The buffer size in kb for stream written to disk after encryption                                                              | 128                       |
  +---------------------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------------------------+

Available implementation for ``encrypted-intermediate-data.class``:

* ``org.apache.tez.client.NullSpillKeyProvider``: a null encryption provider 
* ``org.apache.tez.client.DefaultSpillKeyProvider``: an implementation that uses
  ``HmacSHA1`` algo for encryption with a key length defined as
  ``encrypted-intermediate-data-key-size-bits``.
* ``org.apache.tez.client.KMSSpillKeyProvider``: KMS is used to provide
  intermediate encryption. In that case, the configuration
  ``tez.am.kms-encryption-key-name`` provides the kms-key.


.. admonition:: Related...
   :class: readingbox

   Check :yahoo_jira:`YHADOOP-2500: [Umbrella] Spill Data Encryption for MapReduce and Tez <YHADOOP-2500>`



.. _tez_spill_encryption_examples:

Examples Using Encrypted Spill
------------------------------

The following two examples show command line to submit `orderedwordcount` job on AR.

Default Spill Encryption
^^^^^^^^^^^^^^^^^^^^^^^^

To run the job with default encryption key provider ``DefaultSpillKeyProvider``,
set ``encrypted-intermediate-data`` to true.

  .. code-block:: bash

     export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:$TEZ_CONF_DIR"
     hadoop jar $TEZ_HOME/tez-examples-*.jar \
            orderedwordcount \
            -Dtez.am.encrypted-intermediate-data=true \
            /tmp/wordcount-folder-input \
            /tmp/wordcount-folder-output

KMS Spill Encryption
^^^^^^^^^^^^^^^^^^^^

To run the job with KMS key provider, set the following parameters:

  * ``encrypted-intermediate-data``: true
  * ``spill-encryption-keyprovider.class``: ``KMSSpillKeyProvider``
  * ``kms-encryption-key-name``: `grid_us.EZ.spill_key`.

  The `grid_us.EZ.spill_key <https://ui.ckms.ouroath.com/prod/view-keygroup/grid_us.EZ/view-key/grid_us.EZ.spill_key>`_
  is created by gridops to serve as the key used to encrypt/decrypt spilled data to disk.

  .. code-block:: bash

     export HADOOP_CLASSPATH="$TEZ_HOME/*:$TEZ_HOME/lib/*:$TEZ_CONF_DIR"
     hadoop jar $TEZ_HOME/tez-examples-*.jar \
            orderedwordcount \
            -Dtez.am.encrypted-intermediate-data=true \
            -Dtez.am.encrypted-intermediate-data.class=org.apache.tez.client.KMSSpillKeyProvider \
            -Dtez.am.kms-encryption-key-name=grid_us.EZ.spill_key \
            /tmp/wordcount-folder-input \
            /tmp/wordcount-folder-output

.. _tez_spill_evaluation:

Performance Evaluation
======================

.. sidebar:: Tuning Spill Encryption ...

   See :ref:`mapreduce_encryption_evaluation_optimization`

A `performance evaluation dated May 22nd 2019 <https://docs.google.com/spreadsheets/d/1dFdW3KrZD55rZo69oPaaZqcr1sr74SAsiNu5tSojCxk/edit#gid=2038478652>`_ of ``OrderedWordCount``.


Characteristics:
  * `20` GB shuffle.
  * Configs:
    
    .. code-block:: bash

      tez.am.encrypted-intermediate-data.class=org.apache.tez.client.KMSSpillKeyProvider
      tez.am.kms-encryption-key-name=grid_us.EZ.spill_key

Results:
  * 2.2% - 4.12% (AVG: 3.175%) degradation with encryption turned on;
  * Average map times are in the 0.46% range;
  * Average reduce time change is around 2.5%;
  * Merge times are a contributor with a 1.06% degradation;
  * Shuffle times vary around 3%;
  * CPU time degrades in the range of 0.7-2.26% (AVG: 1.55%).

..  _tez_encryption:

******************************************
Encryption
******************************************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Similar to `Mapreduce data encryption <mapreduce_encryption>`,  this capability
allows encryption of the intermediate files generated during the shuffle/spill.

.. important::
   * Encrypting intermediate data (shuffle/spill) will incur in a
     significant performance impact. |br|
     Users should profile this and potentially reserve 1 or more cores for
     encrypted spill.


It can be enabled by setting the `tez.am.encrypted-intermediate-data` job property
to true.

.. todo::
   * is there a restriction on the number of the number of attempts for a job to
     1.
   * are those configs work for both spill and shuffle?


.. _tez_spill_encryption_config:

Configuration
=============

Intermediate encryption of a Tez job can be configued using the list of values
listed in :numref:`table-tez-spill-configs`.

.. _tez_spill_encryption:

.. table:: `TEZ Configuration AM level for Intermediate Data Encryption prefix tez.am`
  :widths: auto
  :name: table-tez-spill-configs

  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | Configuration                                 | Type    | Description                                                                                                                    | Default |
  +===============================================+=========+================================================================================================================================+=========+
  | ``encrypted-intermediate-data``               | boolean | Enable/Disable intermediate data encryption.                                                                                   | False   |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``encrypted-intermediate-data.class``         | string  | The class used as a spill key provider. This defines the encryption algorithm and key provider to get the client certificates. | ""      |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``encrypted-intermediate-data-key-size-bits`` | int     | The key length used to encrypt data spilled to disk.                                                                           | 128     |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+
  | ``encrypted-intermediate-data.buffer.kb``     | int     | The buffer size in kb for stream written to disk after encryption                                                              | 128     |
  +-----------------------------------------------+---------+--------------------------------------------------------------------------------------------------------------------------------+---------+

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



.. _tez_spill_evaluation:

Performance Evaluation
======================

A `performance evaluation dated May 22nd 2019 <https://docs.google.com/spreadsheets/d/1dFdW3KrZD55rZo69oPaaZqcr1sr74SAsiNu5tSojCxk/edit#gid=2038478652>`_ of ``OrderedWordCount``.


Characterestics:
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

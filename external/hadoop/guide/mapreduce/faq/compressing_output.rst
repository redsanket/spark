..  _mapreduce_compression_faq:

****************
Data Compression
****************

.. contents:: Table of Contents
  :local:
  :depth: 4

-----------

Make sure you go through :ref:`mapreduce_compression`.
The following sections show how to compress job output files and how to subsequently consume them through another Map/Reduce (or Map only) job.

How to Compress Job Output and Intermediate Map Output
======================================================

You can configure the compression for the entire cluster by setting the following properties in `mapred-site.xml`.

.. table:: `YARN compression properties for whole cluster by mapred.map.output`
  :widths: auto

  +---------------------+-------------------------------------------------------------------------------------------------------------------+
  |       Property      |                                                    Description                                                    |
  +=====================+===================================================================================================================+
  | ``compress``        | Should the outputs of the maps be compressed before being sent across the network. Uses SequenceFile compression. |
  +---------------------+-------------------------------------------------------------------------------------------------------------------+
  | ``compress.codec``  | If the map outputs are compressed, how should they be compressed?                                                 |
  +---------------------+-------------------------------------------------------------------------------------------------------------------+

To enable compression for MapReduce on a per-job basis, you need to use the following properties defined in the table. The following examples compress both the intermediate data and the output in MR2 (YARN).

.. table:: `YARN compression properties prefixed by mapreduce.output.fileoutputformat.compress`
  :widths: auto
  
  +--------------------+---------------------------------------------------------------------------------------------------------------------------+
  |      Property      |                                                        Description                                                        |
  +====================+===========================================================================================================================+
  | ``compress``       | Whether to compress the final job outputs (`true` or `false`)                                                             |
  +--------------------+---------------------------------------------------------------------------------------------------------------------------+
  | ``compress.codec`` | If the final job outputs are to be compressed, which codec should be used.                                                |
  +--------------------+---------------------------------------------------------------------------------------------------------------------------+
  | ``.compress.type`` | For SequenceFile outputs, what type of compression should be used (`NONE`, `RECORD`, or `BLOCK`). `BLOCK` is recommended. |
  +--------------------+---------------------------------------------------------------------------------------------------------------------------+


.. code-block:: bash
   
   hadoop jar hadoop-examples-.jar sort "-Dmapreduce.compress.map.output=true" \
          "-Dmapreduce.map.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec" \
          "-Dmapreduce.output.compress=true" \
          "-Dmapreduce.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec" \
          -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text \
          input output



How to Read the Compressed Data into Map/Reduce Job
===================================================

Check user guides for other Hadoop Product Family:
  
  * `Bdml-guide: setting-pig-properties <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#setting-pig-properties>`_
  * `Bdml-guide: Run Hive <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#run-hive>`_


How do I use bzip2 compression scheme on the Grid?
==================================================

Compressed input file in `bzip2` can be split across multiple maps and thus recommended for processing the data on the grid.

.. code-block:: bash

  mapreduce.output.fileoutputformat.compress=true
  mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec




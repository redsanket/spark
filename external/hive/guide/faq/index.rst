===
FAQ
===

This page answers some of the most common questions we get about Hive at Yahoo. For 
troubleshooting issues, see `Troubleshooting <../troubleshooting/>`_.

Questions
=========

* :ref:`Where are the log files created? <log_files>`  
* :ref:`What should I inspect before reporting a problem to @grid-solutions or @hive-dev? <report_problems>`  
* :ref:`What are the delimiters for data and how can I customize them? <delimiters>`  
* :ref:`How are the delimiters expected with multiple nested complex types? <nested>`  
* :ref:`Is it possible to create a Hive table for a data with two maps each separated by different delimiters? <maps_two_delimiters>`
* :ref:`I have structured data on HDFS (or any storage). Can I use Hive to run data processing queries on it? <use_hive>`
* :ref:`Are there any limitations with using external tables? <limitations_ext>`
* :ref:`I have data at multiple locations on HDFS all corresponding to the same schema. Can I map them to a single Hive table? <map_single>`
* :ref:`How long does it take to convert my data to a form Hive understands? <convert_data>`
* :ref:`What happens when I 'drop' an external table? <drop_table>`
* :ref:`Table joins are taking a long time. How can the time taken be reduced? <time_joins>`
* :ref:`Are there any known limitations/problems with bucketing? <bucketing_probs>`
* :ref:`My query will be creating large number of dynamic partitions. Can this be done? <dyn_partitions>`
* :ref:`Does Hive support functions that can be used in a query? <support_funcs>`
* :ref:`Where can I find documentation on how to write user defined functions? <doc_user_funcs>`
* :ref:`What are the input formats supported in Hive? <supported_input>`
* :ref:`Can I do custom transformations in the query and output the data? <custom_trans>`
* :ref:`What are the advantages of using "SORT BY" when creating tables? <sort_by>`
* :ref:`What are the known problems of using "ORDER BY"? <order_by>`
* :ref:`How do you reduce the number of mappers that are created for Hive queries? <reduce_mappers>`
* :ref:`What precautions should be taken when working on the gateways? <gateways>`
* :ref:`What is a scratch directory? <scratch>`
* :ref:`Can scratch directories be configured? <config_scratch>`
* :ref:`Can I control the Hive logging level? (I don't like too many hive log files under $HOME/hivelogs.) <log_levels>`
* :ref:`My Hive program fails because tasks run out of memory. How do I adjust memory settings for Hive jobs? <memory_tuning>`


Answers
=======

.. _log_files:
.. topic::  **Where are the log files created?**

   The Hive server log is located at ``/home/y/libexec/hive_server/logs/hive_server.log``. 
   The Hive CLI log is in ``$HADOOP_TOOLS_HOME/var/logs/hive_cli/${userid}/hive.log``.
    
.. _report_problems:
.. topic::  **What should I inspect before reporting a problem to @grid-solutions or @hive-dev?**

   - **Error on Hive CLI** - See if that matches any of the problems reported in `Troubleshooting <../troubleshooting>`_.
   - **Hive CLI log** - ``$HADOOP_TOOLS_HOME/var/logs/hive_cli/${userid}/hive.log``, typically ``/home/y/var/logs/hive_cli/${userid}/hive.log``
   - **Hive Server log (Dev/QE)** - ``/home/y/libexec/hive_server/logs/hive_server.log``
   - **Job and Task pages** - If the job fails, the job page in JobTracker will be displayed for all jobs that are launched.

.. _delimiters:
.. topic::  **What are the delimiters for data and how can I customize them?**

   The default delimiters are **^A** between fields, **^B** for first-level complex types 
   (array, separation between ``map:key`` entries in a map and between fields of a struct), 
   and **^C** for third-level complex types (separation between key and value in a map). 
   One can customize them by specifying the ``DELIMITED`` properties while creating the 
   table. See the |DDL|_  for more information.
   

.. _nested:
.. topic:: **How are the delimiters expected with multiple nested complex types?** 

   By nested complex types, we mean array of maps, struct containing arrays or maps 
   of maps, etc. By default, the first-level delimiter is **^A**, the next level is **^B**, and 
   further down, it follows the same pattern as **^C**, **^D**, **^E**, etc.

.. _maps_two_delimiters:
.. topic:: **Is it possible to create a Hive table for a data with two maps each separated by different delimiters?**

   No, only one delimiter character is possible.

.. _use_hive:
.. topic:: **I have structured data on HDFS (or any storage). Can I use Hive to run data processing queries on it?**

   Yes. Hive supports the concept of external tables. A table can be created based 
   on the data that already exists on HDFS. A table that exists in other storage 
   systems (HBase alone is supported so far) can also be represented in Hive using ``StorageHandlers``. 
   See |DDL|_ on how to create external tables.

.. _limitations_ext:
.. topic:: **Are there any limitations with using external tables?**

   The location on HDFS is expected to contain only files and not directories. 

.. _map_single:
.. topic:: **I have data at multiple locations on HDFS all corresponding to the same 
           schema. Can I map them to a single Hive table?**

   Yes, you can create the table with partitions. First, an external table has to 
   be created with an empty directory already created on HDFS. An additional column 
   should be added as a partition (this is just a dummy partition which will help 
   us to include multiple paths). Then add a partition to the table by specifying the 
   partition specification (some value which would distinguish the multiple paths, 
   say an hour or day, etc.) and include the location. Hive supports 
   ``alter table <table name> <partition spec> location <location on DFS>'``. 
   See the |DDL|_ for more information.


.. _convert_data:
.. topic:: **How long does it take to convert my data to a form Hive understands?**

   Hive understands a structured data that is delimited properly (also for complex 
   and nested types) and laid out well on HDFS (dir/files or dir/dir*/files). 
   Time taken for conversion depends on the external tool (Pig or MapReduce job or 
   streaming job).   

.. _drop_table:
.. topic:: **What happens when I 'drop' an external table?** 

   The table is removed from Hive but the directories and their files are NOT deleted from HDFS.

.. _time_joins:
.. topic:: **Table joins are taking a long time. How can the time taken be reduced?**

   One can use a map-join (support is in the process of being removed by the Hive 
   community) or bucketed join. See |BT|_ for more information. 
   Do note that if the data size of a table is very, very large (1 TB or so), 
   then creating a bucketed table will also take time.

.. _bucketing_probs:
.. topic:: **Are there any known limitations/problems with bucketing?** 

   Creating a bucketing table takes a long time if the data is skewed. There is 
   no known workarounds for this approach.


.. _dyn_partitions:
.. topic:: **My query will be creating large number of dynamic partitions. Can this be done?**

   Yes. By default, the dynamic partitions are created in the mapper. If lots of 
   partitions (this is not a general scenario) are created, then one can use a 
   sub-query and post-pone the partition creation in the reducers by using a distribute 
   by. See `Bugzilla Ticket 4016030 - Dynamic partition - errors and limits <http://bug.corp.yahoo.com/show_bug.cgi?id=4016030>`_  
   to learn how this was done internally.



.. _support_funcs:
.. topic:: **Does Hive support functions that can be used in a query?** 

   Yes. Hive supports UDF (User Defined Functions) that operate at the column(s) level, 
   UDAF (User Defined Aggregate Functions that operate at multiple rows and produces 
   one output), and UDTF (User Defined Transformation Functions), which accepts 
   one-column value and throws out multiple rows-columns.

.. _doc_user_funcs:
.. topic:: **Where can I find documentation on how to write user defined functions?**

   - `UDF <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF>`_ - custom scalar functions
   - `UDAF <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inAggregateFunctions(UDAF)>`_ - aggregations
   - `UDTF <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-Built-inTable-GeneratingFunctions(UDTF)>`_ - table functions

.. _supported_input:
.. topic:: **What are the input formats supported in Hive?**

   - TextFile input format (default).
   - SequenceFileInput format and RCFile input format (column based).
   -  `Optimized Row Columnar (ORC) <http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.0.2/ds_Hive/orcfile.html>`_ (groups of row data called **stripes**).

   Please note, you must set ``hive.input.format`` to the appropriate java class.

.. _custom_trans:
.. topic:: **Can I do custom transformations in the query and output the data?**

   Yes, look at the `Apache Transform documentation <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Transform>`_.

.. _sort_by:
.. topic:: **What are the advantages of using "SORT BY" when creating tables?**

   With RCFile, a better compression can be achieved as related data is close by.

.. _order_by:
.. topic:: **What are the known problems of using "ORDER BY"?**

   Jobs that use ``ORDER BY`` will have a single reducer. All rows of the output must 
   pass through it, which can take a long time.

.. _reduce_mappers:
.. topic:: **How do you reduce the number of mappers that are created for Hive queries?**

   By using CombineHiveInputFormat, we can control the number of maps that are created 
   for Hive jobs. Following are the parameters to be set:

   ::

       set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
       set hive.hadoop.supports.splittable.combineinputformat=true";

   The parameter ``'hive.hadoop.supports.splittable.combineinputformat'`` is a Hive 
   implementation tweak, which should be set by default on all installations. If not, 
   set it. ``CombineHiveInputFormat`` depends on the parameters ``mapred.max.split.size``, 
   ``mapred.min.split.size``, ``mapred.min.split.size.per.node``, and ``mapred.min.split.size.per.rack``. 
   Set them accordingly to control the number of MAPs.



.. _gateways:
.. topic:: **What precautions should be taken when working on the gateways?** 

   Gateways are shared user resources. Large queries with lots of files can consume 
   memory at the client end (MR split calculation) which can impact other users ability 
   to use the gateways to invoke hive or pig CLI.

   Be a good gateway citizen.

.. _scratch:
.. topic:: **What is a scratch directory?**

   The ``scratch`` directory is configured by a SE and will be used as a temporary space for all the Hive jobs.

.. _config_scratch:
.. topic:: **Can scratch directories be configured?**

   Yes. The property ``'hive.exec.scratchdir'`` can be overridden. As long as the user has permission 
   and quota in that directory, it will be used.


.. _log_levels:
.. topic:: **Can I control the Hive logging level? (I don't like too many hive log files under $HOME/hivelogs.)** 

   Yes. By default the logging level is ``INFO``. You can change it to WARN by using 
   the option ``-hiveconf`` from the command line. 

   For example::

       # hive -hiveconf hive.root.logger=WARN,DRFA

.. _memory_tuning:
.. topic:: **My Hive program fails because tasks run out of memory. How do I adjust memory settings for Hive jobs?**

   You may tune the memory allocation for your Hive tasks in MapReduce/Tez using/adjusting the following settings::

        -- Container sizes.
        set mapreduce.map.memory.mb=2048;
        set mapreduce.reduce.memory.mb=2048;

        -- Heap sizes.
        set hive.tez.java.opts=-Xmx1536m;
        set mapred.child.java.opts=-Xmx1536m;

        -- Tez Application master settings:
        set tez.am.resource.memory.mb=3072;
        set tez.am.launch.cmd-opts=-Xmx2560m;

   Please note the following::

      1. Container parameters should be tuned with ``mapreduce.*.memory.mb``, instead of ``hive.tez.container.size``, because this allows control over map/reduce tasks separately.
      2. Ensure that the container sizes exceed the Xmx settings by 512MB. This is the JVM tax.
      3. Please be careful about how much you bump the container sizes. These resources are shared by others on your queue/cluster.


.. |DDL| replace:: Hive Language Manual
.. _DDL: https://cwiki.apache.org/confluence/display/Hive/LanguageManual 
.. |BT| replace:: LanguageManual DDL Bucketed Tables
.. _BT: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL+BucketedTables

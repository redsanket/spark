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
* :ref:`How do I make my tables/partitions readable by someone else? <hdfs_permissions>`
* :ref:`I have (only) read-access to ``hdfs:///x/y/z``. Why am I not allowed to create EXTERNAL tables pointing to those locations? <external_tables_to_readable_data>`
* :ref:`Are there any limitations with using external tables? <limitations_ext>`
* :ref:`I have data at multiple locations on HDFS all corresponding to the same schema. Can I map them to a single Hive table? <map_single>`
* :ref:`How long does it take to convert my data to a form Hive understands? <convert_data>`
* :ref:`What happens when I 'drop' an external table? <drop_table>`
* :ref:`Table joins are taking a long time. How can the time taken be reduced? <time_joins>`
* :ref:`How does one filter input data for an outer join? <outer_join_filtering>`
* :ref:`Are there any known limitations/problems with bucketing? <bucketing_probs>`
* :ref:`My query will be creating large number of dynamic partitions. Can this be done? <dyn_partitions>`
* :ref:`Does Hive support functions that can be used in a query? <support_funcs>`
* :ref:`Where can I find documentation on how to write user defined functions? <doc_user_funcs>`
* :ref:`What are the input formats supported in Hive? <supported_input>`
* :ref:`Why is CREATE TABLE failing for Avro tables? <avro_create_table>`
* :ref:`Can I do custom transformations in the query and output the data? <custom_trans>`
* :ref:`What are the advantages of using "SORT BY" when creating tables? <sort_by>`
* :ref:`What are the known problems of using "ORDER BY"? <order_by>`
* :ref:`How do you reduce the number of mappers that are created for Hive queries? <reduce_mappers>`
* :ref:`What precautions should be taken when working on the gateways? <gateways>`
* :ref:`What is a scratch directory? <scratch>`
* :ref:`Can scratch directories be configured? <config_scratch>`
* :ref:`Can I control the Hive logging level? (I don't like too many hive log files under $HOME/hivelogs.) <log_levels>`
* :ref:`My Hive program fails because tasks run out of memory. How do I adjust memory settings for Hive jobs? <memory_tuning>`
* :ref:`Why does selecting a simple projection from a table take so long? <fetch_task_conversion>`


Answers
=======

.. _log_files:
.. topic::  **Where are the log files created?**

   The Hive server log is located at ``/home/y/libexec/hive_server/logs/hive_server.log``. 
   The Hive CLI log is in ``$$HOME/hivelogs/hive.log.<pid>@<hostname>``. The latest file in the directory will correspond to the latest session.
    
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

.. _hdfs_permissions:
.. topic:: **How do I make my tables/partitions readable by someone else?**

    Permissions to Hive's databases/tables/partitions are currently governed by the HDFS permissions on their
    corresponding HDFS paths. E.g. To read a table's partitions, a user would need to be granted read-permissions
    on the partitions' data paths.

    It is recommended that production-tables have their data stored under ``hdfs:///projects/<project_name>``, owned
    by a suitable production headless account.
    User databases may be stored under ``hdfs:///user/<userid>/<database_name>``.

    **Note**:
        1. *DO NOT point databases directly to ``hdfs:///user/<userid>``.* Dropping your database will obliterate
           your home directory, replacing it with a smoking crater.
        2. Do not store data in ``hdfs:///tmp`` unless you're comfortable with losing said data.

    The recommended way to manage permissions is as follows:

        1. Ensure the data directories have 750 permissions.
        2. Only the owner (user or headless account) has write permissions. Only this user can write to,
           add/drop tables/partitions.
        3. Grant read permissions to an appropriate group. Users requesting read permission will need to be
           added to said group.

.. _external_tables_to_readable_data:
.. topic:: **I have (only) read-access to ``hdfs:///x/y/z``. Why am I not allowed to create EXTERNAL tables pointing to those locations?**

    To create a Hive object (database/table/partition) pointing to a directory, the user would need to be the HDFS
    owner of said directory. It is not enough to have read-permissions. This is by design. Were this not the case:

        1. When the actual owner deletes the HDFS data, the Hive objects would be dangling pointers to non-existent
           data paths.
        2. Any new partitions added to a table will not be available on the replica table, unless added explicitly.
        3. Any schema changes made on the source table would render the replica table unreadable.

    Rather than to create (possibly duplicate) Hive objects for data that a user doesn't own, it is recommended that
    the user work with the data-owner to create Hive definitions for the data.

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

.. _outer_join_filtering:
.. topic:: **How does one filter input data for an outer join?**

    To reduce input-data to an outer join, filter-predicates may be introduced in either/both of the ``JOIN`` predicates, and the ``WHERE`` clause. If done correctly,
    this will reduce the amount of data scanned, *before* the ``JOIN`` is processed.

    One may peruse the `Apache Hive documentation for the behaviour of Outer Joins <https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior#OuterJoinBehavior-PredicatePushdownRules>`_,
    for a detailed explanation.

    If:

    1. ``Preserved Row Table``  is the table that returns all rows in the outer join (E.g. the LHS relation in a ``LEFT OUTER JOIN``.)
    2. ``Null Supplying Table`` is the table whose columns are returned as nulls, for unmatched rows,

    then, the ``OUTER JOIN`` behaviour may be summarized as below:

        +-----------------+-----------------------+------------------------+
        |                 | Preserved Row Table   | Null Supplying Table   |
        +=================+=======================+========================+
        | Join Predicate  |     Not Pushed        |        Pushed          |
        +-----------------+-----------------------+------------------------+
        | Where Predicate |       Pushed          |      Not Pushed        |
        +-----------------+-----------------------+------------------------+

    For inner joins, any filter-predicates specified in either the ``WHERE`` or ``JOIN`` clauses may be pushed down into either of the participating tables/relations, as applicable.

    Outer joins differ, in that *all rows from the ``Preserved Row Table`` are returned in the result, unless explicitly filtered in the ``WHERE`` clause*. Therefore,

    1. Filter predicates in the ``WHERE`` clause may be applied to the ``Preserved Row Table`` *before* processing the join. This is especially useful if the input table is large, and the join is explosive.
    2. Filter predicates in the ``JOIN`` clause may be applied to the ``Null Supplying Table`` *before* processing the join, because rows that do not satisfy the predicate don't participate in the join anyway.

    Attempting the reverse would produce incorrect results. i.e.

    1. The ``Preserved Row Table`` cannot be pre-filtered by the ``JOIN`` predicates, because the ``JOIN`` clause defines the *match* condition. Rows from the ``Preserved Row Table`` that do not match the ``JOIN`` clause should still appear in the join-results.
    2. Similarly, the ``Null Supplying Table`` cannot be pre-filtered by the ``WHERE`` predicates. Pre-filtering will erroneously produce NULL-filled records from the ``Preserved Row Table``, instead of removing the row entirely from the join-results.

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

.. _avro_create_table:
.. topic:: **Why is CREATE TABLE failing for Avro tables?**

   Avro tables require special handling with YGrid Hive. Unlike with other data formats, Avro schemas are stored outside the table's column schema in the Hive metastore.
   This may be stored in one of two ways:
   - Avro schema JSON string, stored in the "``avro.schema.literal``" table property
   - Avro schema JSON string stored on HDFS, and pointed to by the "``avro.schema.url``" table property

   The contents of Avro schema-strings tend to be too large to be stored as an "``avro.schema.literal``"; the use of "``avro.schema.url``" is preferred, i.e.
   ::

    CREATE TABLE my_avro_table STORED AS AVROFILE TBLPROPERTIES( 'avro.schema.url' = 'hdfs:///path/to/schema.avsc' );

   If the contents of the "``schema.avsc``" exceeds 4K characters, it is possible that this table creation might fail. One can work around this by splitting up
   the table creation as follows:
   ::

    CREATE TABLE my_avro_table ( first_column <type> ) STORED AS AVROFILE;

    ALTER TABLE my_avro_table SET TBLPROPERTIES( 'avro.schema.url' = 'hdfs:///path/to/schema.avsc' );

   If an Avro table's schema needs to be modified, it must be done in the "``schema.avsc``", instead of using an ``ALTER TABLE CHANGE COLUMNS`` command.

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

   In Hive 1.2.x, split combination is done in the Tez Application Master, by default. Please use the following settings:

   ::

       set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
       set tez.grouping.min-size=16777216; -- 16 MB min split
       set tez.grouping.max-size=1073741824; -- 1 GB max split

   Increase min and max split size to reduce the number of mappers.
   Please note that the ``org.apache.hadoop.hive.ql.io.CombineHiveInputFormat`` is no longer supported with Hive 1.2, and should not be used.

.. _reducer_parallelism:
.. topic:: **How do you increase reducer parallelism for Hive queries?**

   By default, the number of reducers in a Hive query stage is derived from the input data-size. Roughly,

   ::

       num-reducers = max( (hive.exec.reducers.max), (input-data-size-in-bytes) / (hive.exec.reducers.bytes.per.reducer))

   By default, the ``hive.exec.reducers.bytes.per.reducer)`` is 256MB. Thus, for a 1GB input, there are 4 reducers.
   The reducer count can be increased by decreasing the denominator, up to a max of `hive.exec.reducers.max` (1009, by default). To increase further, please increase `hive.exec.reducers.max`.

   ::

       set hive.exec.reducers.bytes.per.reducer=64*1024*1024; -- 16MB
       set hive.exec.reducers.max=2009;

   Alternatively, the reducer-count can be hard-coded via the following setting:

   ::

       set mapreduce.job.reduces=2009;
       set hive.exec.reducers.max=2009;

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
        set mapreduce.map.java.opts=-Xmx1536m;
        set mapreduce.reduce.java.opts=-Xmx1536m;

        -- Tez Application Master settings:
        set tez.am.resource.memory.mb=3072;
        set tez.am.launch.cmd-opts=-Xmx2560m;

   Please note the following::

      1. Container parameters should be tuned with ``mapreduce.*.memory.mb``, instead of ``hive.tez.container.size``, because this allows control over map/reduce tasks separately.
      2. "The JVM Tax": Ensure that the heap-size (Xmx setting) is at 80% of the container size (or at least 512MB less than container size). This covers the JVM overhead.
      3. In case more memory is required in a mapper/reducer, please try bumping the corresponding container sizes by 512MB at a time, and adjust the heap sizes according to above.
      4. Please be careful about how much you bump the container sizes. These resources are shared by others on your queue/cluster.

.. _fetch_task_conversion:
.. topic:: **Why does selecting a simple projection from a table take so long?**

    For speeding up queries that select simple projections from tables, consider using the Hive "fetch-task conversion" optimization by setting ``hive.fetch.task.conversion=more``.
    Hive then attempts to run queries (with simple projections, limit clauses, and without group-by) within the Hive client, instead of submitting a cluster-job.
    This is particularly useful for exploratory queries to sample table-contents.

    While this approach is usually faster for queries with non-existent or easily satisfied selection predicates, it has potential to be *much* slower, if the predicates are rarely satisfied.
    For instance, a query with a predicate like "``WHERE userid IS NULL``" will run much more slowly, if the data rarely has ``NULL`` values for ``userid``.
    (This is because the data-files will be scanned linearly on the Hive client, looking for the elusive record.)
    For cases like this, it would be better to ``set hive.fetch.task.conversion=minimal``, and have Hive launch a cluster job to scan the data in parallel.

    For clarity, the permitted values of ``hive.fetch.task.conversion`` are:
        1. ``none``:    No fetch-task-conversion optimization is performed. All queries are executed via cluster jobs.
        2. ``minimal``: Queries are converted to run on the Hive client, if they contain only simple projections and limits, with no UDFs, aggregations, or predicates.
        3. ``more``   : Queries are converted to run on the Hive client, if they contain only simple projections and limits, with no UDFs, or aggregations. Simple predicates are permitted.

.. |DDL| replace:: Hive Language Manual
.. _DDL: https://cwiki.apache.org/confluence/display/Hive/LanguageManual 
.. |BT| replace:: LanguageManual DDL Bucketed Tables
.. _BT: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL+BucketedTables

===============
Troubleshooting
===============


Issues
======

* :ref:`Authorization failed:org.apache.hadoop.security.AccessControlException: action WRITE not permitted on path <auth_no_write>`
* :ref:`What logs should I look into when a problem is encountered and what information should I have before reporting a bug? <examine_logs>`
* :ref:`Hive CLI exits after throwing a FileNotFoundException, does not launch a CLI session. <not_found>`
* :ref:`Hive CLI throws MetaException when any of the queries are run. <meta_exception>`
* :ref:`Hive CLI fails with garbage collection (GC) memory limit exceeded exception. <memory_limit>`
* :ref:`Drop table fails with quota exceeded exception. <quota_exceeded>`
* :ref:`Hive Hadoop Job fails with "total number of created files exceeds" exception. <number_files_exceeds>`
* :ref:`Job fails with "Fatal error was thrown due to exceeding number of dynamic partitions" error. <dyn_partitions_exceeded>`
* :ref:`User does not have permission to SUBMIT_JOB to the queue 'default' or the JobTracker queue 'default' does not exist. <submit_permission>`
* :ref:`Read-timeout errors when sampling data from tables. <timeout_errors>`
* :ref:`Cannot view the Task (Map/Reduce) information or the Job configuration of the Hive jobs that are submitted. <view_mr_job>`
* :ref:`Hive Job is running for a long time, and very few reducers are running for a long time. <hive_job_running_long_time>`
* :ref:`The Map or Reducer tasks are failing with heap error or "cannot create thread" exceptions. <heap_error>`
* :ref:`The Hive queries generate lots of map tasks denying CPU resources to other jobs. <starving_jobs>`

Solutions
=========

.. _auth_no_write:
.. topic:: **Authorization failed:org.apache.hadoop.security.AccessControlException: action WRITE not permitted on path**

   You should see the following error during table creation under the 'default' database.

   ::

       hive> create table test (name string) location '/tmp/1';
       Authorization failed:org.apache.hadoop.security.AccessControlException: action WRITE not permitted on path hdfs://axoniteblue-nn1.blue.ygrid.yahoo.com:8020/projects/hcatalog-warehouse for user thiruvel. Use show grant to get more details.


   Hive needs write permission on the database directory to create a table under the 
   'default' database. This is discouraged at Yahoo as providing global write 
   permission to ``/projects/hcatalog-warehouse`` would make it an alternative to ``/tmp``. 
   Hive users at Yahoo are requested to create their own database with their own 
   user/project space as the location.  See `Getting Started <../getting_started/>`_.

.. _examine_logs:
.. topic:: **What logs should I look at when a problem is encountered and what information should 
           I have before reporting a bug?**

   Logs to look at:

   - **Error on Hive CLI** - See if that matches any of the problems reported in 
     this troubleshooting chapter.
   - **Hive CLI log** - '$HOME/hivelogs'. There will be a ``hive.log.<pattern>``i, which 
     will contain the Hive CLI's log. There will be one file log file per session.
   - **Job and Task pages** - if the job fails. The Job page in JobTracker will be 
     displayed for all jobs that are launched.
   - Errors should be covered in all the logs above. If no exception/error is present 
     in the above list, talk to Hive solutions and support with the above information, 
     tables used, schema, and the type of query launched.


.. _not_found:
.. topic:: **Hive CLI exits after throwing the error FileNotFoundException and does not 
           launch a CLI session.**

   ::

       bash-3.2$ hive
       Hive history file=/home/y/libexec/hive_cli/logs/hadoopqa/hive_job_log_hadoopqa_201010062007_184624718.txt
       Exception in thread "main" java.io.FileNotFoundException: /homes/hadoopqa/.hivehistory (No such file or directory)
       The $HOME directory /homes/{userid} will not be mounted for the user on the node 
       and hence the hive history file cannot be created. If its not possible to mount 
       the home directory on the node, override user.home configuration to a different 
       directory which is private to the user.

   Start Hive again, specifying the user home::

       $ hive -hiveconf user.home=/grid/0/tmp/hadoopqa

.. _meta_exception:
.. topic:: **Hive CLI throws MetaException when any of the queries are run.**

   ::

       FAILED: Error in metadata: MetaException (message:Could not connect to meta store using any of the URIs provided)
       FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask

   a. It's possible that ``'kinit'`` was not done before launching Hive client.
   b. The HCat Server is not running or is not configured correctly. Talk to Hive support if 'a' is done.


.. _memory_limit:
.. topic:: **Hive CLI fails with garbage collection (GC) memory limit exceeded exception.** 

   This exception can occur before the launch of a MapReduce job or after completion too (during post-job activities).
   Exception in thread "main" java.lang.OutOfMemoryError: GC overhead limit exceeded.
   Restart Hive CLI session after tuning 'HADOOP_CLIENT_OPTS' environment variable to increase the heap size.

   ::

       # export HADOOP_CLIENT? _OPTS="-Xmx512m $HADOOP_CLIENT_OPTS" # An example where max heap is set to 512 MB.

.. _quota_exceeded:
.. topic:: **Drop table fails with quota exceeded exception.** 

   Hive calls HCat to drop the table's directory to Trash (which is typically 
   ``/user/<user id>/.Trash``) from the warehouse directory, which could have different 
   quota restrictions. So, if a large table is dropped or multiple tables were dropped 
   before the HDFS Trash directory is cleaned, one would get this exception. There 
   are two paths to resolve this:

   a. If the data of dropped tables have to stay in ``.Trash`` until its cleaned up, 
      talk to Grid Ops to see if increasing the quota on a user's home directory is possible.
   b. If the data of dropped tables can be purged immediately, then remove the table's 
      directory by hand.

   Try the following as well:

   #. Get the location of the table from ``desc formatted <table name>`` or ``desc formatted <table> partition <partition spec>``.
   #. Remove the data by hand ``hadoop fs -rmr -skipTrash <table location>``
   #. Drop the table.

.. _number_files_exceeds:
.. topic:: **Hive Hadoop Job fails with "total number of created files exceeds" exception.**

   This typically happens with dynamic partitions wherein multiple files are created. 
   Hive has a limit of 100000 for files created. To avoid this limitation, one can tune the 
   configuration "hive.exec.max.created.files" to a higher value and restart the query. 
   This is an indication, however, that the query or the data model 'might' need optimization.

.. _dyn_partitions_exceeded:
.. topic:: **Job fails with "Fatal error was thrown due to exceeding number of dynamic partitions" error.**

   There is a limit of 100 partitions created per node (task tracker). Increase the 
   configuration ``"hive.exec.max.dynamic.partitions.pernode"`` according to the input 
   data, i.e., if there are only 250 different properties (say Yahoo property IDs), 
   then one can set this to 250 or 260.


.. _submit_permission:
.. topic:: **User does not have permission to ``SUBMIT_JOB`` to the queue 'default' or the 
           ``JobTracker`` queue 'default' does not exist.**

   The MR configuration ``'mapred.job.queue.name'`` should be set to the JobTracker, which 
   the user is authorized to use. It can be set on the Hive CLI or command arguments.

   ::

       > set mapred.job.queue.name=<>;


.. _timeout_errors:
.. topic:: **Read-timeout errors when sampling data from tables.**

   This typically happens if a partitioned table in HCatalog has a large number 
   (read "hundreds of thousands") of partitions and is queried without specifying 
   any partition filters. This will result in HCatalog having to needlessly process 
   a large number of files/directories on HDFS.

   ::

       hive (fetl)> select * from abf_hourly limit 10;

       FAILED: SemanticException? org.apache.thrift.transport.TTransportException: java.net.SocketTimeoutException: Read timed out
       hive (fetl)>

   To speed up your query for partitioned tables, it is recommended to specify as 
   many partition-filters as are feasible in your query. For instance, 
   the sampling above can be improved thus:

   ::

       [mithunr@gwta6002 ~]$ hive --database fetl -e "show partitions abf_hourly" | tail -5 # Sample the partitions.
       Logging initialized using configuration in file:/home/y/libexec/hive/conf/hive-log4j.properties
       OK
       Time taken: 2.211 seconds
       OK
       Time taken: 3.231 seconds
       dt=201403112000/timezone_dt=201403120600p10/page=PAGE/isvalid=Valid
       dt=201403112000/timezone_dt=201403120800p12/page=INTERACTION/isvalid=Valid
       dt=201403112000/timezone_dt=201403120800p12/page=NONPAGE/isvalid=Valid
       dt=201403112000/timezone_dt=201403120800p12/page=PAGE/isvalid=Invalid
       dt=201403112000/timezone_dt=201403120800p12/page=PAGE/isvalid=Valid
       [mithunr@gwta6002 ~]$ hive --database fetl -e "select * from abf_hourly where dt='201403112000' and timezone_dt='201403120800p12' and page='PAGE' and isvalid='Valid' limit 10;" # Sample from a specific partition ...

   Specifying partition filters helps Hive trim down the data scan-range. Relational 
   operators are supported in the WHERE clause.

.. _view_mr_job:
.. topic:: **Cannot view the Task (Map/Reduce) information or the Job configuration of the Hive jobs that are submitted.**

   Hadoop Security has two parameters which control which user's can view and modify 
   the job, ``mapreduce.job.acl-view-job`` and ``mapreduce.job.acl-modify-job`` appropriately. 
   Set them to ``"user1,[user2] group1,[group2]"``. Remember the **space** between user list and group list.

   Those can be set in Hive CLI prompt like ``"> set <param>=<value>"`` or when 
   invoking the CLI ``"hive -hiveconf <param>=<value>"``.


.. _hive_job_running_long_time: 
.. topic:: **Hive Job is running for a long time, and very few reducers are running for a long time.**

   This happens when the data is skewed and the reducers have to process lots of rows. 
   Happens with ``"SORT BY"`` or ``"ORDER BY"`` or with ``JOINS``, too,  when a single reducer 
   ends up getting many of the rows processed by multiple maps. We suggest
   trying out ``Bucketing`` or ``Mapjoin`` or ``Skewed`` join to see if that helps.

.. _heap_error:
.. topic:: **The Map or Reducer tasks are failing with heap error or "cannot create thread" exceptions.** 

   This happens when the Task is running short of memory. One can tune the Hadoop parameter 
   ``"mapred.child.java.opts"`` accordingly.

   ::

      hive -hiveconf mapred.child.java.opts="-server -Xmx1200m -Djava.io.tmpdir=/grid/0/tmp -Djava.net.preferIPv4Stack=true"

.. _starving_jobs:
.. topic:: **The Hive queries generate lots of map tasks denying CPU resources to  other jobs.**

   By default, one mapper is created for one split of the input. Use ``CombineFileInput``
   format to tune the number of maps based on the split size. 
   See the `FAQ <../faq>`_ for more information.


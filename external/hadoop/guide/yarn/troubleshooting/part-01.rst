.. _yarn_troubleshooting_slow_attempts:

Manage Slow or Stuck Yarn Application Attempts
==============================================

Hadoop applications periodically will have one or more attempts that are running slowly (or are stuck) and are slowing down the rest of the application.

In Hadoop `2.7.2.15` and later releases, there is a new feature that allows you to:

- Dump the `JStack` of a slow or stuck attempt so that you can see where it is taking its time
- Kill (either gracefully or forcefully) an attempt that is running slowly so that the attempt can be rescheduled on a faster node.

..  _merge_slow_attempt_dump_jstack:


Dump the JStack of a Slow Attempt
---------------------------------

In order to signal an application attempt to dump its stack, you will need to know its container ID.

See :numref:`yarn_troubleshooting_merge_slow_attempt_get_container_id` below for instructions on how to obtain the container ID of the desired attempt.

* Cause the container to dump its JStack

  .. code-block:: bash

    $ yarn container -signal container_1466534149943_0002_01_000007OUTPUT_THREAD_DUMP

* View the stdout container logs from the GUI:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/container.logs.stdout.jstack.jpg
  :height: 777px
  :scale: 100%
  :alt:
  :align: center

..  _yarn_troubleshooting_merge_slow_attempt_kill_attempt:

Kill an Application Attempt
---------------------------

In order to kill an application attempt by sending it a signal, you will need to know its container ID.

See (:numref:`yarn_troubleshooting_merge_slow_attempt_get_container_id`) below for instructions on how to obtain the container ID of the desired attempt.

* ``yarn container -signal container_1466534149943_0002_01_000007 GRACEFUL_SHUTDOWN``
* If the container won't die, use ``FORCEFUL_SHUTDOWN`` instead of ``GRACEFUL_SHUTDOWN``.

..  _yarn_troubleshooting_merge_slow_attempt_get_container_id:

Get the Container ID
^^^^^^^^^^^^^^^^^^^^

From the Map Reduce GUI
"""""""""""""""""""""""

* Select the list of running attempts from the MapReduce job overview page on the Job History Server. For example:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/get.container.id.1.jpg
  :height: 250px
  :width: 700px
  :scale: 85%
  :alt:
  :align: center

* From the job's running attempts page, select the "logs" link. For example:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/get.container.id.2.jpg
  :height: 200px
  :width: 700px
  :scale: 85%
  :alt:
  :align: center

* Use the container ID from the URL. For example:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/get.container.id.3.jpg
  :height: 250px
  :width: 700px
  :scale: 85%
  :alt:
  :align: center

From the TEZ GUI
""""""""""""""""

* Select "All TaskAttempts" from the "DAG Details" page of your application in the TEZ GUI. For example:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/get.container.id.4.jpg
  :height: 250px
  :width: 700px
  :scale: 85%
  :alt:
  :align: center

* Get the container ID of your attempt from the "Containers" column. For example:

.. image:: /images/yarn/troubleshooting/manage-slow-attempts/get.container.id.5.jpg
  :height: 250px
  :width: 700px
  :scale: 85%
  :alt:
  :align: center

From the Gateway
""""""""""""""""

* Get the application attempt ID:

  .. code-block:: bash

    $ yarn applicationattempt -list application_1466534149943_0002
    Total number of application attempts :1
    ApplicationAttempt-Id                   State      AM-Container-Id                           Tracking-URL
    appattempt_1466534149943_0002_000001    RUNNING    container_1466534149943_0002_01_000001    localhost:8088/proxy/application_1466534149943_0002/

* Get the ID of the slow or stuck container:

  .. code-block:: bash

    $ yarn container -list appattempt_1466534149943_0002_000001
    Container                               Start Time  Finish Time  State    Host Node       ...

    container_1466534149943_0002_01_000007  ...         N/A          RUNNING  localhost:4545  ...

.. _yarn_troubleshooting_metadata_size_exceeds:

Yarn job failed with Error: `Split metadata size exceeded 10000000`
===================================================================

.. rubric:: Root Cause

The error could come because there are too many splits generated for the map reduce job. The Metadata file which stores the split information has too much information(``> 10MB`` default value) possibly because of too many splits. 

.. rubric:: Solutions

* If there are too many mappers (in the order of thousands) and you actually don't need them (having way too many mappers is actually bad for performance), try reducing the number of total mapper tasks by having bigger splits by setting:
  ``-Dmapreduce.input.fileinputformat.split.minsize=536870912`` (`512M` or any higher value, default to block size which is `128M` on our grid. For ABF feeds `1G` or `2G` is good).

* If the file sizes are small, say `128MB`, setting ``split.minsize`` to a higher value like 1G does not help.
  In that case, you can try to combine splits (See :numref:`runtime-qa-part-02-number-of-mappers`).

* If first two options do not work for you and it is still hitting the same error, please try bumping up AM meta info size by setting ``-Dmapreduce.job.split.metainfo.maxsize=___`` to higher value (default is `10,000,000`). The latter is the maximum permissible size of the split metainfo file. The MapReduce `ApplicationMaster` won't attempt to read submitted split metainfo files bigger than this configured value. No limits if set to `-1`.


* If first two options does not work for you and if Application Master is hitting OOM due to too many tasks, please try bumping up heapsize of the application master by the options in the table below:
  
  .. include:: /common/yarn/memory/yarn-memory-appmaster-conf.rst
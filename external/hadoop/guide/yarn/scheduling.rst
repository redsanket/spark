.. _hadoop_guide_yarn_scheduling:

**********************
Scheduling and Queuing
**********************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------


The Capacity Scheduler
======================

.. admonition:: Reading...
   :class: readingbox

   Read about the Capacity Scheduler on Cloudera Blog `YARN – The Capacity Scheduler <https://blog.cloudera.com/yarn-capacity-scheduler>`_, or download the :download:`PDF version</resources/yarn-capacity-scheduler-cloudera-blog.pdf>`.

Application Priorities Within Capacity Scheduler Queue
------------------------------------------------------

This feature allows applications to be submitted and scheduled with different priorities. Higher integer value indicates higher priority for an application. Currently Application priority is supported only for FIFO ordering policy.
Application priority works only along with FIFO ordering policy. Default ordering policy is FIFO. Default priority for an application can be at cluster level and queue level.

.. glossary::

   Cluster-level priority
     Any application submitted with a priority greater than the cluster-max priority will have its priority reset to the cluster-max priority. ``$HADOOP_HOME/etc/hadoop/yarn-site.xml`` is the configuration file for cluster-max priority.
   
   Leaf Queue-level priority
     Each leaf queue provides default priority by the administrator. The queue's default priority will be used for any application submitted without a specified priority. ``$HADOOP_HOME/etc/hadoop/capacity-scheduler.xml`` is the configuration file for queue-level priority. *Note* that priority of an application will not be changed when application is moved to different queue.


+-------+-----------------------------------------------------+-------------------------------------------------------+
|  User |                    Configuration                    |                      Description                      |
+=======+=====================================================+=======================================================+
| Admin | ``yarn.cluster.max-application-priority``           | Defines maximum application priority in a cluster.    |
+-------+-----------------------------------------------------+-------------------------------------------------------+
| Admin | ``yarn.scheduler.capacity.root`` |br|               | Defines default application priority in a leaf queue. |
|       | ``.<leaf-queue-path>.default-application-priority`` |                                                       |
+-------+-----------------------------------------------------+-------------------------------------------------------+


Hadoop Preemption Overview
--------------------------

Hadoop preemption is a feature within the Hadoop capacity scheduler. This feature allows under-served queues to preempt tasks from queues that are operating over-capacity. The preemption feature will `NEVER` steal resources from a queue that is operating under its capacity setting.


.. glossary::

   Under-served queue
     is one that is consuming less than its allotted capacity and is requesting additional resources from the cluster.
   
   Over-capacity queue
     is a queue that is using more resources than its allotted capacity.

Whether or not resources can be preempted from a queue is configurable on a queue-by-queue basis. Initially, we are configuring clusters so that only the "default" queue is preemptable. All other queues will be marked as non-preemptable.

.. tip:: Queue owners can request their queue be marked preemptable. Since the system can take resources from a preemptable queue in order to meet demand, preemptable queues can have much higher `max-capacity` settings. In other words, when a queue is configured to be preemptable, its max-capacity setting will often increase at the same time giving those queues more resources to work with when the cluster has available resources.


:guilabel:`Example:`

+-------+--------------------------+--------------------+-------------------------------+--------------+
| Queue | Configured |br| Capacity | Resource |br| Used | Pending Resource |br| Request | Preemptable? |
+=======+==========================+====================+===============================+==============+
| A     | 100                      | `150`              | 100                           | Yes          |
+-------+--------------------------+--------------------+-------------------------------+--------------+
| B     | 100                      | 50                 | 100                           | Yes          |
+-------+--------------------------+--------------------+-------------------------------+--------------+
| C     | 10                       | `150`              | 0                             | Yes          |
+-------+--------------------------+--------------------+-------------------------------+--------------+


In this example, queues A and C are operating over-capacity and B is under-served. C is more over-served than A so in this case, 50 resources  will be preempted from queue C in order to satisfy B's requests. The result after the preemption process has completed will be (preemption does not happen all at once. It is iterative until it reaches its desired goal):


:token:`Result:`

+-------+--------------------------+---------------------+-------------------------------+--------------+
| Queue | Configured |br| Capacity | Resource |br| Used  | Pending Resource |br| Request | Preemptable? |
+=======+==========================+=====================+===============================+==============+
| A     | 100                      | 150                 | 100                           | Yes          |
+-------+--------------------------+---------------------+-------------------------------+--------------+
| B     | 100                      | |ss| 50 |se| `100`  | |ss| 100 |se| 50              | Yes          |
+-------+--------------------------+---------------------+-------------------------------+--------------+
| C     | 10                       | |ss| 150 |se| `100` | |ss| 0 |se| 50                | Yes          |
+-------+--------------------------+---------------------+-------------------------------+--------------+


.. note:: When a task is preempted, any work it has performed will be lost.

By design, the map-reduce and tez frameworks are fine with individual tasks failing and getting restarted (it is happening on our clusters all the time due to faulty nodes, slow nodes, overly busy nodes, etc.) However, the framework can't control the user-code which runs as part of the application and as a result we do occasionally run across applications that don't properly deal  with tasks being re-run (e.g. they hit an external REST service that is not idempotent).

To minimize the amount of work lost when preempting a task, the scheduler will preempt from youngest to oldest (i.e. tasks that have been running for a long time are much less likely to be preempted).

In hierarchical queue configurations, resources will not be preempted from a sub-tree that is running within capacity, even if a leaf queue is running significantly over capacity. In the example below, A/sub1 is over capacity and B is under-served, but A/sub1 will not be preempted because its parent (A) is still within capacity limits.


:guilabel:`Example:`

+--------+--------------------------+--------------------+-------------------------------+--------------+
| Queue  | Configured |br| Capacity | Resource |br| Used | Pending Resource |br| Request | Preemptable? |
+========+==========================+====================+===============================+==============+
| A      | 200                      | 150                | 100                           | Yes          |
+--------+--------------------------+--------------------+-------------------------------+--------------+
| A/sub1 | 100                      | `150`              | 100                           | Yes          |
+--------+--------------------------+--------------------+-------------------------------+--------------+
| A/sub2 | 100                      | 0                  | 0                             | Yes          |
+--------+--------------------------+--------------------+-------------------------------+--------------+
| B      | 100                      | 50                 | 50                            | Yes          |
+--------+--------------------------+--------------------+-------------------------------+--------------+


:guilabel:`Configurations:`

*Note:* All the configurations are prefixed by ``yarn.resourcemanager``.

The CapacityScheduler supports preemption of container from the queues whose resource usage is more than their guaranteed capacity. The following configuration parameters need to be enabled in ``yarn-site.xml`` for supporting preemption of application containers.

.. table:: `All configuration are prefixed by yarn.resourcemanager.scheduler.monitor`
  :widths: auto

  +--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |            Property            |                                                                                                                      Description                                                                                                                      |
  +================================+=======================================================================================================================================================================================================================================================+
  | ``enable``                     | Enable a set of periodic monitors (specified in ``scheduler.monitor.policies``) that affect the scheduler. Default value is ``false``.                                                                                                                |
  +--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``policies``                   | The list of ``SchedulingEditPolicy`` classes that interact with the scheduler. Configured policies need to be compatible with the scheduler. Default value is ``ProportionalCapacityPreemptionPolicy`` which is compatible with ``CapacityScheduler`` |
  +--------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


The following configuration parameters can be configured in ``yarn-site.xml`` to control the preemption of containers when ``ProportionalCapacityPreemptionPolicy`` class is configured for ``yarn.resourcemanager.scheduler.monitor.policies``

.. table:: `All configuration are prefixed by yarn.resourcemanager.monitor.capacity.preemption`
  :widths: auto

  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |            Property            |                                                                                                                                                                                                  Description                                                                                                                                                                                                 |
  +================================+==============================================================================================================================================================================================================================================================================================================================================================================================================+
  | ``observe_only``               | If true, run the policy but do not affect the cluster with preemption and kill events. Default value is false                                                                                                                                                                                                                                                                                                |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``monitoring_interval``        | Time in milliseconds between invocations of this ``ProportionalCapacityPreemptionPolicy`` policy. Default value is 3000                                                                                                                                                                                                                                                                                      |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``max_wait_before_kill``       | Time in milliseconds between requesting a preemption from an application and killing the container. Default value is 15000                                                                                                                                                                                                                                                                                   |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``total_preemption_per_round`` | Maximum percentage of resources preempted in a single round. By controlling this value one can throttle the pace at which containers are reclaimed from the cluster. After computing the total desired preemption, the policy scales it back within this limit. Default value is 0.1                                                                                                                         |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``max_ignored_over_capacity``  | Maximum amount of resources above the target capacity ignored for preemption. This defines a deadzone around the target capacity that helps prevent thrashing and oscillations around the computed target balance. High values would slow the time to capacity and (absent ``natural.completions``) it might prevent convergence to guaranteed capacity. Default value is 0.1                                |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``natural_termination_factor`` | Given a computed preemption target, account for containers naturally expiring and preempt only this percentage of the delta. This determines the rate of geometric convergence into the deadzone (``MAX_IGNORED_OVER_CAPACITY``). For example, a termination factor of 0.5 will reclaim almost 95% of resources within ``5 * #WAIT_TIME_BEFORE_KILL``, even absent natural termination. Default value is 0.2 |
  +--------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+


The ``CapacityScheduler`` supports the following configurations in ``capacity-scheduler.xml`` to control the preemption of application containers submitted to a queue.



.. table:: `All configuration are prefixed by yarn.scheduler.capacity.<queue-path>.`
  :widths: auto

  +-----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |                    Property                   |                                                                                                                                                                                                                                                                                  Description                                                                                                                                                                                                                                                                                  |
  +===============================================+===============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================+
  | ``disable_preemption``                        | This configuration can be set to `true` to selectively disable preemption of application containers submitted to a given queue. This property applies only when system wide preemption is enabled by configuring ``yarn.resourcemanager.scheduler.monitor.enable`` to `true` and ``yarn.resourcemanager.scheduler.monitor.policies`` to ``ProportionalCapacityPreemptionPolicy``. If this property is not set for a queue, then the property value is inherited from the queue’s parent. Default value is `false`.                                                            |
  +-----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``intra-queue-preemption.disable_preemption`` | This configuration can be set to `true` to selectively disable intra-queue preemption of application containers submitted to a given queue. This property applies only when system wide preemption is enabled by configuring ``yarn.resourcemanager.scheduler.monitor.enable`` to `true`, ``yarn.resourcemanager.scheduler.monitor.policies`` to ``ProportionalCapacityPreemptionPolicy``, and ``intra-queue-preemption.enabled`` to `true`. If this property is not set for a queue, then the property value is inherited from the queue's parent. Default value is `false`. |
  +-----------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Hadoop CPU Scheduling
=====================

.. _hadoop_guide_yarn_cpu_scheduling_overview:

The Hadoop YARN scheduler now supports two resources: Memory, and now CPU.
Applications now request some amount of both resource and the scheduler makes sure both resources are
available on the node it assigns the application's containers.
Further, YARN makes sure the containers stay within their Memory and CPU limits:

#. If a container exceeds its memory limits, it is killed.
#. If there is contention for the CPU, the container is held to its CPU allocation
   (i.e. if the node is busy enough and a container only allocated 1 CPU core, then it will be held to that limit using linux
   `cgroups <https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch01.html>`_).

If there is extra CPU available, then containers are allowed to exceed their CPU allocation.

The unit of CPU resource is a “vcore” or "virtual core". The term “virtual” is used because there is not necessarily a 1:1 mapping between a vcore and a physical core on a compute node. In our case, we use the approximation of 1 physical core = 10 vcores.

For Map-Reduce and Tez containers, we have set the default vcore requirement to 10 vcores (approximately 1 physical core). This seems to be a very reasonable default for most applications.

Now that there are two resources (memory and vcores) which the YARN scheduler must schedule, a few things are worth pointing out:

#. In order to schedule a container on a node, the node must have BOTH the required amount of free memory AND the required number of free vcores. Since there are now two resource constraints that must be met, it has become more difficult for the scheduler to find a place to run a container. What this means is that we have to be careful that overall cluster utilization does not decline. To avoid such a decline we will most likely be over-subscribing the CPU resource to some small degree. See FAQ below.
#. Some applications used to effectively reserve CPU cores by requesting lots of memory. This made sense where there was only a single resource. Now however, this ends up wasting the memory resource and probably on many newer machines won't even get them the CPU cores they want (because most likely they're using the default of 10 vcores).
#. The part of the Resource Manager UI that shows queue utilization is rolling up both resources into a single utilization number. Essentially it's: Max(memory_utilization, vcore_utilization).

.. _yarn_scheduling_capacity_scheduler:

******************
Capacity Scheduler
******************

.. admonition:: Reading...
   :class: readingbox

   Read about the Capacity Scheduler on Cloudera Blog `YARN – The Capacity Scheduler <https://blog.cloudera.com/yarn-capacity-scheduler>`_, or download the :download:`PDF version</resources/yarn-capacity-scheduler-cloudera-blog.pdf>`.

`CapacityScheduler`, a pluggable scheduler for Hadoop which allows for multiple-tenants to securely share a large cluster such that their applications are allocated resources in a timely manner under constraints of allocated capacities.

.. _yarn_scheduling_app_priorities:

Application Priorities Within Capacity Scheduler Queue
======================================================

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



.. _hadoop_guide_yarn_cpu_scheduling_overview:

CPU Scheduling
==============

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


User Weights: Some Users Are More Equal Than Others
===================================================

A user whose apps are running in a queue can be assigned a weight in that queue in order to regulate the amount of resources assigned to that user relative to other users. It is sometimes desirable in multi-tenant queues to allocate more resources to some users than to others. This can be used to dynamically allow some users more resources during peak times to complete catchup. Also, it can be used to dynamically reduce a user's resource allocation if that user is holding on to resources for long periods of time. When used in conjunction with the in-queue preemption feature (See :ref:`yarn_scheduling_preemption_intra-queue`), the user-weights feature can be used to adjust weights and preempt resources to allow higher-priority users to complete critical tasks more quickly.


.. glossary::

   Active User
     A user that has applications in a queue AND at least one of those applications is requesting resources.
   
   Configured Capacity
     This is a percentage of a parent queue's resources that are allocated for use by a child queue. For example, if this value is 10%, the child queue is "guaranteed" 10% of it's parents resources. (NOTE: "guaranteed" is in quotes because various conditions could temporarily cause the child queue to be starved of its resources.)

   Configured Max Capacity
     This is the maximum percentage of a parent queue's resources that a child queue can consume. For example, if a queue's configured capacity (see above) is 10% but its configured max capacity is 75%, the child queue can grow above it's guaranteed 10% up to 75% of the parent's total allocated resources. This growth can happen only if the queue's sibling queues are not using those resources.
   
   Configured Minimum User Limit Percent
     The minimum percentage of a queue's resources assigned to a single active user. During the resource assignment phase, the capacity scheduler will meet this requirement as best as it can. However, since this percentage is dependent on the number of active users and the number of active users can change frequently, this is not a guarantee.

   Configured User Limit Factor
     This number represents the amount of a queue that one user can consume. If the value is 0.5, applications of a single user cannot consume more than 50% of the queue's resources. If the value is 1.5, a single user's applications can consume 150% of a queue's resources. This factor is applied to all users in a specified queue.
   
   Inactive User
     A user whose applications are running in a queue and are using resources, but none of the applications are requesting more resources.

   Resources
     Shared resources allocated to a queue. As of now, resources are memory (measured in GB) and CPU (measured in virtual cores).


Capacity Scheduler GUI Provides Users' Resource Usage
-----------------------------------------------------

If you open the Capacity Scheduler GUI and click on the arrow in order to expand a queue, you will see something like this:

.. image:: /images/yarn/scheduling/user-weights/image-01.png
  :alt:
  :align: center

Active Users Info Section on the Capacity Scheduler GUI:

* The Active Users Info section describes the resources used by both active and inactive users (despite its name).
* Each user may have multiple applications running.

.. image:: /images/yarn/scheduling/user-weights/image-02.png
  :alt:
  :align: center

Active and Inactive Users:

* In the above image of the Active Users Info, the user named `hadoop5` is highlighted in yellow because it has apps that are asking for more resources. It is an active user.
* The users `hadoop3` and `hadoop4` have apps in the default queue that are using resources, but none of the apps need more resources. They are inactive users.  


Max Resource:

* The Max Resource column indicates how much of the cluster's resources should be allocated to a users.
* Increasing and decreasing a user's weight will cause this value to go up or down relative to other users.
* If the user's total `Used Resource` is less than this value, the capacity scheduler will assign resources to this user's apps.
* If the user's total `Used Resource` is equal to this value, the capacity scheduler will assign one more container to the next requesting app owned by this user.
* The Max Resource value is irrelevant for inactive users. This is because the Max Resource value is calculated based on the number of active users (see below).

.. _yarn_scheduling_how_does_user_weights_work:

How Do User Weights Work?
-------------------------

* Users with higher weights will be assigned more resources than users with lower weights within a queue.
* Although there are many additional variables to consider, the calculation for **Max Resource** basically boils down to :eq:`eq-user-weight-hadoop`
  
  * Let :math:`R` is the total resources consumed by active users, :math:`N` is the number of active users, :math:`C` is the cluster capacity, :math:`l` is the number of active users,  :math:`l` Configured Minimum User Limit Percent, and :math:`w` is the user's weight, then:
  
    .. math::
      :label: eq-user-weight-hadoop

       \text{Max-Resource} = 
      \begin{cases}
       w \times \textit{max} \left( \dfrac{C}{N} , \dfrac{C \times l}{100} \right) & \textit{for hadoop-2.8}\\
       w \times \textit{max} \left( \dfrac{R}{N} , \dfrac{R \times l}{100} \right) & \textit{for hadoop-2.9+}
       \end{cases}

  * A weight value of `0.0` will assign owned container to the first active application of a user. No further resources will be assigned to that user. This is because when a user's `Used Resource` is equal to the user's `Max Resource`, the capacity scheduler will assign the user one more container.

Configuring User Weights
------------------------

The user weight properties should be placed in the `capacity-scheduler.xml` (or something that is included by the `capacity-scheduler.xml`). It has the following format: ``yarn.scheduler.capacity.[QueuePath].user-settings.[UserName].weight`` |br|
User weights can be refreshed without restarting the resource manager by running ``yarn rmadmin -refreshQueues``

* A user's weight is queue-specific.
* User Weights are inherited from parent queues.
* A user's weight value can be a float between `0.0` and :math:`\frac{100.0}{l}`, where :math:`l` is the Configured Minimum User Limit Percent
* The weight value of less than `1.0` is valid.

Examples
--------

These examples assume the following queue hierarchy:

.. image:: /images/yarn/scheduling/user-weights/image-03.png
  :alt:
  :align: center

Also note that in these examples, for simplicity, we are only looking at memory.


Users' Weights Are Inherited From a Parent Queue And Overridden at The Child Queue Level
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

User weights are inherited from the parent queue, but can be overridden by a child queue. For example, if the user `hadoop1` should be considered half a user in all queues in the cluster except in the `glamdring` queue. In the glamdring queue, `hadoop1` should have twice the weight of other users. The following configuration properties would be created:

  .. code-block:: xml

    <property>
      <name>yarn.scheduler.capacity.root.user-settings.hadoop1.weight</name>
      <value>0.5</value>
    </property>
    <property>
      <name>yarn.scheduler.capacity.root.swords.glamdring.user-settings.hadoop1.weight</name>
      <value>2.0</value>
    </property>

When `hadoop1` runs apps in the `default` queue, it's Max Resource is half of other users:

.. image:: /images/yarn/scheduling/user-weights/image-04.png
  :alt:
  :align: center

When `hadoop1` runs apps in the `glamdring` queue, it's Max Resource is twice that of other users:

.. image:: /images/yarn/scheduling/user-weights/image-05.png
  :alt:
  :align: center


User's Weight Is a Multiplier for the Configured User Limit Factor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

+-----------------+----------------------------+--------------------------------+--------------------------------------------+------------------------+
| Queue |br| Name | Configured |br| `Capacity` | Configured |br| `Max-Capacity` | (Cluster `Total-Resources`) |br|           | User Limit |br| Factor |
|                 |                            |                                | * (`Configured-Capacity`)                  |                        |
+=================+============================+================================+============================================+========================+
|     default     |            10.0%           |             100.0%             | :math:`20 \text{GB} * 10.0\% = 2 \text{GB}`| 1.0                    |
+-----------------+----------------------------+--------------------------------+--------------------------------------------+------------------------+

In this example, a single user with weight 1.0 can only ever use 2GB (plus 1 container--See :numref:`yarn_scheduling_how_does_user_weights_work`) because the Configured User Limit Factor is `1.0` and the Configured Capacity is `10.0%`. In the following image, see that:

  .. math::
    \text{Max-Resource} = (\textit{Cluster-Total-Resources} \times \textit{Configured-Capacity} \times \textit{User-Weight})

  .. math::
    \text{Max-Resource} = (20480 \times 0.1 \times 1.0) = 2048 \text{MB} \\
    \text{Used-Resource} = \text{Max-Resource} + 1 \textit{container} = (20480 + 512) = 2560\text{MB}

.. image:: /images/yarn/scheduling/user-weights/image-06.png
  :alt:
  :align: center


If a user's weight is 0.5 in this queue configuration, that user would be allowed to use only 1GB (plus one container):


.. image:: /images/yarn/scheduling/user-weights/image-07.png
  :alt:
  :align: center

If a user's weight is 2.0 in this queue configuration, that user would be allowed to use 4GB (plus one container).


.. image:: /images/yarn/scheduling/user-weights/image-08.png
  :alt:
  :align: center

Sum of Active Users' Weights Is Less Than 1.0
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If `hadoop1` has a weight of 0.5 and `hadoop2` has a weight of 0.25, they will interact in a similar way as if hadoop1 had a weight of 2.0 and `hadoop2` had a weight of 1.0. The important thing is the relative weights of the users.


.. image:: /images/yarn/scheduling/user-weights/image-09.png
  :alt:
  :align: center

Note that in order for these user to consume up to the Configured Max Capacity, the Configured User Limit Factor had to be set to `20.0`.

Sum of Active Users' Weights Is More Than 1.0
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. image:: /images/yarn/scheduling/user-weights/image-10.png
  :alt:
  :align: center

Inactive Users' Max Resource Can Go Above Cluster Capacity
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following image shows that if `hadoop0` is inactive and has a `Max-Resource` of `40GB`, which is twice that of the actual capacity of the entire cluster:


.. image:: /images/yarn/scheduling/user-weights/image-11.png
  :alt:
  :align: center

This is because the sum of weights for all active users is 0.5, and when Max Resource is calculated for the active user, it comes out correctly. Remember that the Max Resource for inactive users is not relevant.

User's Weight Set to 0.0
^^^^^^^^^^^^^^^^^^^^^^^^

When a user's weight is set to 0, it will be assigned 1 container. That is because 

  .. math::
    \text{Used-Resource} = \text{Max-Resource} + 1 \ \textit{container} = (0.0 + 512\text{MB})

512MB is the size of 1 container in this example.

.. image:: /images/yarn/scheduling/user-weights/image-12.png
  :alt:
  :align: center
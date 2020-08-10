..  _yarn_scheduling_preemption_intra-queue:

Intra-queue (in-queue) Preemption
=================================

.. admonition:: Reading...
   :class: readingbox

   * Review :ref:`yarn_scheduling_preemption_inter-queue`
   * Review section :ref:`yarn_scheduling_capacity_scheduler`.
   * Hadoop Resource Manager's Capacity Scheduler, especially Queue Properties at Apache Hadoop :hadoop_rel_doc:`Capacity Scheduler <hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html#Queue_Properties>`

.. glossary::

   userLimit
     Each user is given at least this percentage of a queue's resources. It is calculated using the configuration ``yarn.scheduler.capacity.<queue-path>.minimum-user-limit-percent``. The dfault value is `100`, which means `no-limits`. Note that ff this value is `25` and there are `4` or more users with submitted jobs, each user will be given at least `25%`` of the resources when their jobs run. This also means that if users always use their max allotment, only 4 users (in this example) can have running jobs at a time. There could be more if each user doesn't need the full `25%`.

Configuration Properties
------------------------

*Note:* All the configurations are prefixed by ``yarn.resourcemanager.monitor.capacity.preemption.intra-queue-preemption.``.


.. table:: `Properties for Intra-Queue preemption`
  :widths: auto
  :name: table-intra-queue-configs

  +-----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |           Property          |                                                                                                                  Description                                                                                                                  |
  +=============================+===============================================================================================================================================================================================================================================+
  | ``enabled``                 | Specifies whether intra-queue preemption is enabled or disabled for queues.                                                                                                                                                                   |
  |                             | The default value is `true`.                                                                                                                                                                                                                  |
  +-----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``preemption-order-policy`` | Specifies the order in which a queue can preempt resources. Based on your requirements, you can configure this property to either of the following values: |br|                                                                               |
  |                             |                                                                                                                                                                                                                                               |
  |                             | #. `userlimit-first`, to initiate intra-queue preemption based on configured user limits. This is the default value. When a cluster is configured with this property, application priority is ignored when considering containers to preempt. |
  |                             | #. `priority-first`, to initiate intra-queue preemption based on application priorities.                                                                                                                                                      |
  +-----------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

What problem is in-queue preemption trying to solve?
----------------------------------------------------

In the Capacity Scheduler, resource utilization (memory and CPU) is regulated by a set of properties for each queue. These properties define rules that ensure each application and each user in a queue get their share of resources. |br|
However, prior to intra-queue preemption, there are situations in which applications can be starved out of their rightful resources.

Priority Inversion
^^^^^^^^^^^^^^^^^^

As described in :ref:`yarn_scheduling_app_priorities`, applications can be given priorities within a queue so that when resources become available to that queue, they will be assigned to applications in priority order. In a full queue, the expectation is that containers for lower-priority applications will quickly complete and then be re-assigned to the higher-priority applications.

In some cases, however, resources will not become available because a lower priority application's containers are long-running and not releasing the resources. This causes priority inversion.


:token:`Use Case`

Let's assume we have:

* Queue Name: `default`
* Queue Guaranteed Resources: 100G
* Queue Max Resources: 100G

.. table:: `. Step-1. The user named hadoop1 submits a low priority app and consumes all of the default queue. The app runs 1 task attempt per container, and each task runs for hours:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 100 x 1G Containers        | 0                              |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

.. table:: `. Step-2. User hadoop1 launches a high priority app, but gets no resources because app1 is holding on to them:`
  :widths: auto
  
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 100 x 1G Containers        | 0                              |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app2                           | hadoop1           | 2                    | 0                          | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

In this case, app2 is starving for resources because app1 won't release them, resulting in a priority inversion.



User-limit Inversion
^^^^^^^^^^^^^^^^^^^^

As documented in Apache Hadoop :hadoop_rel_doc:`Capacity Scheduler <hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html#Queue_Properties>`, a queue's minimum user limit percent property defines the minimum percentage of each queue's resources that will be allocated to applications of a single user. The easiest way to describe this is through an example:


+-----------------+---------------------------------+--------------------------+---------------------------------+
| Queue |br| Name | Queue Guaranteed |br| Resources | Queue Max |br| Resources | Minimum User |br| Limit Percent |
+=================+=================================+==========================+=================================+
| default         | 100G                            | 100G                     | 50                              |
+-----------------+---------------------------------+--------------------------+---------------------------------+

.. table:: `. Step-1. User hadoop1 submits app1 that needs 150 1G containers. The app runs 1 task attempt per container, and each task runs for 1 second. Since there are no other users in the default queue, app1 consumes all resources in the default queue, but still needs 50 more 1G containers:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 100 x 1G Containers        | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

.. table:: `. Step-2. User hadoop2 submits app2 that needs 150 1G containers. The tasks for app1 are short-lived, so as they complete, these resources are given to app2. Since there are 2 users in the default queue, each user will quiesce to consuming 50% of the resources in the default queue:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 50 x 1G Containers         | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app2                           | hadoop2           | 1                    | 50 x 1G Containers         | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+


.. table:: `. Step-3. User hadoop3 submits app3 that needs 150 1G containers. Tasks from app1 and app2 are short-lived, but the first 2 apps are still requesting more resources. Since the minimum user limit percent for the default queue is 50%, app3 will not be given any resources until one of the first 2 users stops asking for more:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 50 x 1G Containers         | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app2                           | hadoop2           | 1                    | 50 x 1G Containers         | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app3                           | hadoop3           | 1                    | 0                          | 150 x 1G Containers            |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

.. table:: `. Step-4. When tasks from app1 and app2 have completed and they no longer need more, app3 will then be given resources from the default queue:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 40 x 1G Containers         | 0                              |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app2                           | hadoop2           | 1                    | 45 x 1G Containers         | 0                              |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app3                           | hadoop3           | 1                    | 15 x 1G Containers         | 135 x 1G Containers            |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

.. table:: `This use case works well as long as all of the tasks are short-lived. However, if the tasks for app1, for example, run for hours, step 2 would look like this:`
  :widths: auto

  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | App Name in |br| default Queue | Owner of |br| App | Priority |br| of App | Resources Used |br| By App | Resources Pending |br| for App |
  +================================+===================+======================+============================+================================+
  | app1                           | hadoop1           | 1                    | 100 x 1G Containers        | 50 x 1G Containers             |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+
  | app2                           | hadoop2           | 1                    | 0 x 1G Containers          | 150 x 1G Containers            |
  +--------------------------------+-------------------+----------------------+----------------------------+--------------------------------+

This would result in a user-limit inversion.


How does in-queue preemption prevent inversion?
-----------------------------------------------

The Capacity Scheduler runs a configurable monitor that periodically checks for queue anomolies that could indicate an inversion. In-queue preemption is a feature of this monitor. When enabled, in-queue preemption notices inversion and kills containers from inverted applications.

Each queue must be configured to preempt for either priority or user limit inversion. The property ``intra-queue-preemption.preemption-order-policy`` is a cluster-wide value, so all queues in the cluster will take on the value of this property. See :numref:`table-intra-queue-configs` for details on the values that can be passed to the property.

Preventing Priority Inversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the preemption monitor detects a priority inversion with a queue:

* Containers from the lowest priority applications are preempted first.
* If all lower priority applications have the same priority, the youngest containers will be selected first for preemption.
* Priority preemption will not preempt AM containers.

Preventing User-limit Inversion
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
When the preemption monitor detects a user-limit inversion with a queue:

* In order to fill user limit requirements for under-served users, the in-queue preemption monitor will preempt containers from users that are over their user limit.
* Containers from the most over-served users will be preempted first.
* If a user is at or under their user limit, their containers will not be preempted.
* Priority preemption will not preempt AM containers.

Use Cases
^^^^^^^^^

See :download:`Intra Queue Preemption Use Cases </resources/yarn/scheduling/IntraQueuePreemptionUseCases-v1.pdf>`.
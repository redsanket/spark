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

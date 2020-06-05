..  _hadoop_team_yarn_scheduling:

################################
Scheduling and Queuing
################################

*********************
Hadoop CPU Scheduling
*********************

..  _hadoop_team_yarn_cpu_scheduling_overview:

Overview
--------

The Hadoop YARN scheduler now supports two resources: Memory, and now CPU.
Applications now request some amount of both resource and the scheduler makes sure both resources are
available on the node it assigns the application's containers.
Further, YARN makes sure the containers stay within their Memory and CPU limits:

1. If a container exceeds its memory limits, it is killed.
2. If there is contention for the CPU, the container is held to its CPU allocation
   (i.e. if the node is busy enough and a container only allocated 1 CPU core, then it will be held to that limit using linux
   `cgroups <https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/ch01.html>`_).

If there is extra CPU available, then containers are allowed to exceed their CPU allocation.

The unit of CPU resource is a “vcore” or "virtual core". The term “virtual” is used because there is not necessarily a 1:1 mapping between a vcore and a physical core on a compute node. In our case, we use the approximation of 1 physical core = 10 vcores.

For Map-Reduce and Tez containers, we have set the default vcore requirement to 10 vcores (approximately 1 physical core). This seems to be a very reasonable default for most applications.

Now that there are two resources (memory and vcores) which the YARN scheduler must schedule, a few things are worth pointing out:

1. In order to schedule a container on a node, the node must have BOTH the required amount of free memory AND the required number of free vcores. Since there are now two resource constraints that must be met, it has become more difficult for the scheduler to find a place to run a container. What this means is that we have to be careful that overall cluster utilization does not decline. To avoid such a decline we will most likely be over-subscribing the CPU resource to some small degree. See FAQ below.
2. Some applications used to effectively reserve CPU cores by requesting lots of memory. This made sense where there was only a single resource. Now however, this ends up wasting the memory resource and probably on many newer machines won't even get them the CPU cores they want (because most likely they're using the default of 10 vcores).
3. The part of the Resource Manager UI that shows queue utilization is rolling up both resources into a single utilization number. Essentially it's: Max(memory_utilization, vcore_utilization).


FAQ
---

What if my application is not Map-reduce or Tez? (example: Spark)
#################################################################

- For Spark tools (e.g. spark-shell, spark-submit, pyspark) this is controlled by the ``--driver-cores`` and ``--executor-cores`` options which specify the ``#`` of physical cores. Internally Spark converts this parameter to vcores.
- For specific applications not mentioned above, you’ll need to check the application’s documentation.


What configuration options do I use to request more/less vcores?
################################################################

- For Map-Reduce and Tez applications, the following properties all default to 10 vcores:

  * ``mapreduce.map.cpu.vcores``
  * ``mapreduce.reduce.cpu.vcores``
  * ``yarn.app.mapreduce.am.resource.cpu-vcores``

- For other application types, refer to the app-specific documentation

How do I figure out if the 10 vcore default is right for my application?
########################################################################

- If your application is a Map-Reduce application, there are counters that help with this. The following example illustrates the basic principles:

  * The CPU time spent (sec) counter for the maps is 1100 CPU-seconds
  * The Avg Map time was 10 seconds and there were 100 maps
  * In this case, the avg cpu utilization for the maps was ``10 seconds * 100 maps = 1000`` CPU-seconds which is close to the 1100 CPU-seconds measured in the counter - So default of 10 vcores is REASONABLE.
  * **Important Note:** It's possible this job was limited by the vcore setting itself, it would be good to verify by setting vcores=20 and then verifying the CPU utilization was still close to 1000.
  * Simply stated, the idea is to get the following two ratios to be approximately equal: ``CPU_time_spent / Wall_Clock_time ~= vcores / 10`` (in our example ``1100 / 1000 ~= 10 / 10``)
  * An application that is very I/O bound (e.g. distcp), might have a ratio of 10 / 50, in which case a vcore setting of 2 would be appropriate.
  * An application that is heavily threaded and CPU bound, might have a ratio of 200 / 100, in which case a vcore setting of 20 would be appropriate.

- If your application is not a Map-Reduce application then refer to that application type's documentation.
- Basically, it's all about figuring out how many vcores the application's containers will need on average. Requesting too many vcores will mean YARN will not be able to schedule as many containers simultaneously (just like memory, vcores is a finite resource tracked by the RM). Requesting too few vcores can cause your job not to perform well when the cluster is busy (vcores is only truly enforced when there is significant demand for CPU resource).

If I request 10 vcores, am I guaranteed that an allocated container will get 1 full physical CPU core?
######################################################################################################

- No. Like Memory, we will probably be over-subscribing the CPU resource to some degree. We do this because it is unusual for all containers on a node to all be using their full vcore allotment all of the time. Normally a handful of containers are waiting on I/O, or are otherwise temporarily blocked. In order to keep our utilization at reasonable levels we want to take advantage of these idle periods by over-subscribing CPU.

- It's not clear at this point how much we will over-subscribe the CPU resource, probably somewhere in the 10-25% range.

How are Hyperthreads accounted for?
###################################

Hyperthreads are counted as physical cores. It's true that 2 hyperthreads is not the same as 2 dedicated cores, but it's certainly greater than 1 dedicated core. So, as with the previous question, we don't guarantee that 10 vcores == a physical core.

Is there a way for me to test my application at its allotted vcore usage so as to make sure I’ll still meet SLA even when system is busy?
#########################################################################################################################################

As describe above, the system can give my application more than it's allotted vcores if there are free CPU resources available. This makes it deterkine if the number of vcores I've allocated is sufficient to meet SLAs even when the system is under load.
This capability is coming. See Jira: `YARN-810 <https://issues.apache.org/jira/browse/YARN-810>`_

Is there a limit to the number of vcores an application can request?
####################################################################

Yes. This setting is cluster-specific. To see the value for the cluster you are interested in, go to the Resource Manager front page (example: `AxoniteRed <http://axonitered-jt1.red.ygrid.yahoo.com:8088/cluster/scheduler>`_), under ``Tools`` select ``Configuration``, then search for ``yarn.scheduler.maximum-allocation-vcores``.

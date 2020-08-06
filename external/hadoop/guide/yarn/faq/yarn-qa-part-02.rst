Hadoop CPU Scheduling
=====================

What if my application is not Map-reduce or Tez? (example: Spark)
-----------------------------------------------------------------

* For Spark tools (e.g. spark-shell, spark-submit, pyspark) this is controlled by the ``--driver-cores`` and ``--executor-cores`` options which specify the ``#`` of physical cores. Internally Spark converts this parameter to vcores.
* For specific applications not mentioned above, you’ll need to check the application’s documentation.


What configuration options do I use to request more/less vcores?
-----------------------------------------------------------------

* For Map-Reduce and Tez applications, the following properties all default to 10 vcores:

+---------------------------------------------------------+---------------------------------------------------------------------------------+
|                      Configuration                      |                                   Description                                   |
+=========================================================+=================================================================================+
| ``mapreduce.map.cpu.vcores``                            | The number of virtual cores to request from the scheduler for each map task.    |
+---------------------------------------------------------+---------------------------------------------------------------------------------+
| ``mapreduce.reduce.cpu.vcores``                         | The number of virtual cores to request from the scheduler for each reduce task. |
+---------------------------------------------------------+---------------------------------------------------------------------------------+
| ``yarn.app.mapreduce`` |br| ``.am.resource.cpu-vcores`` | The number of virtual CPU cores the MR AppMaster needs.                         |
+---------------------------------------------------------+---------------------------------------------------------------------------------+

* For other application types, refer to the app-specific documentation

How do I figure out if the 10 vcore default is right for my application?
------------------------------------------------------------------------

* If your application is a Map-Reduce application, there are counters that help with this. The following example illustrates the basic principles:

  * The CPU time spent (sec) counter for the maps is 1100 `CPU-seconds`
  * The Avg Map time was 10 seconds and there were 100 maps
  * In this case, the avg cpu utilization for the maps was ``10 seconds * 100 maps = 1000`` `CPU-seconds` which is close to the 1100 `CPU-seconds` measured in the counter - So default of 10 vcores is *REASONABLE*.
  * **Important Note:** It's possible this job was limited by the vcore setting itself, it would be good to verify by setting ``vcores=20`` and then verifying the CPU utilization was still close to 1000.
  * Simply stated, the idea is to get the following two ratios to be approximately equal: ``CPU_time_spent / Wall_Clock_time ~= vcores / 10`` (in our example ``1100 / 1000 ~= 10 / 10``)
  * An application that is very I/O bound (e.g. ``distcp``), might have a ratio of 10 / 50, in which case a vcore setting of 2 would be appropriate.
  * An application that is heavily threaded and CPU bound, might have a ratio of 200 / 100, in which case a vcore setting of 20 would be appropriate.

* If your application is not a Map-Reduce application then refer to that application type's documentation.
* Basically, it's all about figuring out how many vcores the application's containers will need on average. Requesting too many vcores will mean YARN will not be able to schedule as many containers simultaneously (just like memory, vcores is a finite resource tracked by the RM). Requesting too few vcores can cause your job not to perform well when the cluster is busy (vcores is only truly enforced when there is significant demand for CPU resource).


If I request 10 vcores, am I guaranteed that an allocated container will get 1 full physical CPU core?
------------------------------------------------------------------------------------------------------

* *No*. Like Memory, we will probably be over-subscribing the CPU resource to some degree. We do this because it is unusual for all containers on a node to all be using their full vcore allotment all of the time. Normally a handful of containers are waiting on I/O, or are otherwise temporarily blocked. In order to keep our utilization at reasonable levels we want to take advantage of these idle periods by over-subscribing CPU.
* It's not clear at this point how much we will over-subscribe the CPU resource, probably somewhere in the 10-25% range.

How are Hyperthreads accounted for?
-----------------------------------

Hyperthreads are counted as physical cores. It's true that 2 hyperthreads is not the same as 2 dedicated cores, but it's certainly greater than 1 dedicated core. So, as with the previous question, we don't guarantee that 10 vcores == a physical core.

Is there a way for me to test my application at its allotted vcore usage so as to make sure I’ll still meet ``SLA`` even when system is busy?
---------------------------------------------------------------------------------------------------------------------------------------------

As describe above, the system can give my application more than it's allotted vcores if there are free CPU resources available. This makes it deterkine if the number of vcores I've allocated is sufficient to meet SLAs even when the system is under load.
This capability is coming. See Jira: `YARN-810 <https://issues.apache.org/jira/browse/YARN-810>`_

Is there a limit to the number of vcores an application can request?
--------------------------------------------------------------------

*Yes*. This setting is cluster-specific. To see the value for the cluster you are interested in, go to the Resource Manager front page (example: `AxoniteRed <http://axonitered-jt1.red.ygrid.yahoo.com:8088/cluster/scheduler>`_), under ``Tools`` select ``Configuration``, then search for ``yarn.scheduler.maximum-allocation-vcores``.

Queueing and Scheduling
=======================

Will a higher priority job preempt a lower priority job in the same queue?
--------------------------------------------------------------------------
No. Higher priority jobs are offered resources ahead of lower priority jobs within the same queue. Resources which are already allocated to lower priority jobs are not currently taken away. Preemption of lower priority resources may become an option in the future.

Are priorities respected between queues?
----------------------------------------
No. Priorities are queue-specific.

Why preemption is needed?
-------------------------

*Q*: As a queue owner, why would I ever want to enable preemption for my queue. It sounds like resources will be taken from my queue when some other queue is in need.

*Ans*: If you enable preemption on your queue, it allows us to safely increase your max_capacity, allowing you to use a much higher percentage of idle cluster resources. For example, if your max_capacity is currently at 20% of the cluster and you agree to enable preemption for your queue, it's likely we would increase the max_capacity of your queue to a much higher value (75%+). So, rather than always being limited to 20%, you can use up to 75% of the cluster with the caveat that  when the cluster gets very busy, some of those resources are likely to be taken back so that other queues get their guaranteed capacity.


How quickly will the scheduler preempt resources from over-capacity queues to satisfy my under-capacity queue?
--------------------------------------------------------------------------------------------------------------

It's not as quickly as if the resources were completely idle. This is intentional because there is a cost associated with preempting containers. Overall, the cluster is better off if containers complete naturally. So, there are some delays in place to try and allow the system to meet the capacity demands naturally instead of immediately killing containers every time there is a request for resources. See the configuration parameters table below.

Will my job's counters be affected if tasks are preempted?
----------------------------------------------------------

No. Counters, logs, and other task-specific artifacts are not considered. However, it is still considered an "attempt" so it will show up in the task_attempts portion of the UI and it will be available in starling.

Is there a way I can determine if my job had tasks preempted?
-------------------------------------------------------------

Yes. Follow the link to the job history from the resource manager UI. Select the list of Killed map or reducer attempts. The reason for these tasks being killed will indicate it was preempted.

Is it possible for my job to never complete because it's getting preempted?
---------------------------------------------------------------------------
Yes. Unlikely, but it is possible that a cluster is so busy that your job never makes any real progress because it keeps getting preempted. If you have long running tasks, you are more susceptible because work is not preserved when tasks are preempted. So, one thing you can do to mitigate this possibility is to reduce your task running time (2 to 30 minutes is reasonable).

Is there a way to designate a job as non-preemptable?
-----------------------------------------------------
No. Only queues can be configured as preemptable/non-preemptable

Will the scheduler preempt things within a queue to satisfy a higher priority job within that queue?
----------------------------------------------------------------------------------------------------
No. There is no concept of priority within a queue (although that feature is coming shortly, stay tuned).

Which clusters will have preemption enabled?
---------------------------------------------

Clusters must be running at least Hadoop 2.6.0.10 before preemption can be enabled. Normally, shortly after moving to this release, the default queue is changed to be preemptable.

Sometimes my job runs really fast, and sometimes it seems to take forever.
---------------------------------------------------------------------------

When the cluster is busy, the default queue does not have a lot of capacity associated with it so jobs may appear to run slowly. When the cluster is mostly idle, the default queue can make use of these idle resources and therefore jobs can run orders of magnitude faster.

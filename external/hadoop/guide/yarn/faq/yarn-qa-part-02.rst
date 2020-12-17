.. _yarn-faq-cpu-scheduling:

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
  * In this case, the avg cpu utilization for the maps was:
    
    .. math::
      
      10 \, \textit{seconds} \times 100 \, \textit{maps} = 1000 \, \textit{cpu-seconds}

    which is close to the 1100 `CPU-seconds` measured in the counter - So default of 10 :math:`\textit{vcores}` is *REASONABLE*.
  * **Important Note:** It's possible this job was limited by the vcore setting itself, it would be good to verify by setting :math:`\textit{vcores}=20` and then verifying the CPU-utilization was still close to 1000.
  * Simply stated, the idea is to get the following two ratios to be approximately equal: 
    
    .. math::
      
      \dfrac{\textit{cpu_time_spent}}{\textit{wall_clock_time}} \approx \dfrac{\textit{vcores}}{10}

    For our example:  

    .. math::
      
      \dfrac{\textit{vcores}}{10} \approx \dfrac{1100}{1000} \approx \dfrac{11}{10}


  * An application that is very I/O bound (e.g. ``distcp``), might have a ratio of :math:`\dfrac{10}{50}`, in which case a vcore setting of 2 would be appropriate.
  * An application that is heavily threaded and CPU bound, might have a ratio of :math:`\dfrac{200}{100}`, in which case a :math:`\textit{vcore}` setting of 20 would be appropriate.

* If your application is not a Map-Reduce application then refer to that application type's documentation.
* Basically, it's all about figuring out how many vcores the application's containers will need on average. Requesting too many vcores will mean YARN will not be able to schedule as many containers simultaneously (just like memory, vcores is a finite resource tracked by the RM). Requesting too few vcores can cause your job not to perform well when the cluster is busy (:math:`\textit{vcores}` is only truly enforced when there is significant demand for CPU resource).


If I request 10 vcores, am I guaranteed that an allocated container will get 1 full physical CPU core?
------------------------------------------------------------------------------------------------------

* *No*. Like Memory, we will probably be over-subscribing the CPU resource to some degree. We do this because it is unusual for all containers on a node to all be using their full vcore allotment all of the time. Normally a handful of containers are waiting on I/O, or are otherwise temporarily blocked. In order to keep our utilization at reasonable levels we want to take advantage of these idle periods by over-subscribing CPU.
* It's not clear at this point how much we will over-subscribe the CPU resource, probably somewhere in the 10-25% range.

How are Hyperthreads accounted for?
-----------------------------------

Hyperthreads are counted as physical cores. It's true that 2 hyperthreads is not the same as 2 dedicated cores, but it's certainly greater than 1 dedicated core. So, as with the previous question, we don't guarantee that :math:`10 \, \textit{vcores} == \textit{a physical core}`.

Is there a way for me to test my application at its allotted vcore usage so as to make sure I’ll still meet ``SLA`` even when system is busy?
---------------------------------------------------------------------------------------------------------------------------------------------

As describe above, the system can give my application more than it's allotted vcores if there are free CPU resources available. This makes it determine if the number of vcores I've allocated is sufficient to meet SLAs even when the system is under load.
This capability is coming. See Jira: `YARN-810 <https://issues.apache.org/jira/browse/YARN-810>`_

Is there a limit to the number of vcores an application can request?
--------------------------------------------------------------------

*Yes*. This setting is cluster-specific. To see the value for the cluster you are interested in, go to the Resource Manager front page (example: `AxoniteRed <http://axonitered-jt1.red.ygrid.yahoo.com:50505/cluster/scheduler>`_), under ``Tools`` select ``Configuration``, then search for ``yarn.scheduler.maximum-allocation-vcores``.

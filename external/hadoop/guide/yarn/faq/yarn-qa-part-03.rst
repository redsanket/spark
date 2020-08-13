
Scheduling and Queuing
======================

Will a higher priority job preempt a lower priority job in the same queue?
--------------------------------------------------------------------------

No.  Preemption of lower priority resources can be configured on, but by default it is configured off. Higher priority jobs are offered resources ahead of lower priority jobs within the same queue, but resources which are already allocated to lower priority jobs are not currently taken away.

Are priorities respected between queues?
----------------------------------------

No. Priorities are queue-specific.


Setting Application Priority When Submitting MapReduce Jobs
-----------------------------------------------------------

Configure cluster's max application priority in `yarn-site.xml`

  .. code-block:: xml

    <property>
      <name>yarn.cluster.max-application-priority</name>
      <value>10</value>
    </property>

Set the ``mapreduce.job.priority`` when submitting MapReduce jobs. In the following example, from the command line submit the first job, wait until it is running, then submit the second job:

  .. code-block:: bash

    >$ hadoop jar hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}-tests.jar \
              sleep \
              -Dmapreduce.job.queuename=default \
              -Dmapreduce.job.priority=5 \
              -m 40 -r 1 -mt 15000 -rt 15000

    ## Submit second job  
    >$ hadoop jar hadoop-mapreduce-client-jobclient-${HADOOP_VERSION}-tests.jar \
              sleep \
              -Dmapreduce.job.queuename=default \
              -Dmapreduce.job.priority=8 \
              -m 40 -r 1 -mt 1 -rt 1

Lower priority job begins to run, then higher priority job is given all resources. Once the first job (`0001`) starts running, it consumes all of the resources allocated to the queue even though it has a lower priority. This is because the higher priority queue is still in the `ACCEPTED` state:

  .. image:: /images/yarn/scheduling/submit-app-priorities/image-01.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-02.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-03.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-04.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-05.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-06.jpg
    :alt:
    :align: center


  .. image:: /images/yarn/scheduling/submit-app-priorities/image-07.jpg
    :alt:
    :align: center


Changing Priority While Application Is Running
----------------------------------------------

You can change the priorities of running applications by executing the commands in this section. The result will be that the highest priority applications will get assigned the first available resources, just as seen in the section above.

  .. code-block:: bash

    $HADOOP_PREFIX/bin/yarn application -updatePriority 2 -appId AppID

Why preemption is needed?
-------------------------

*Q*: As a queue owner, why would I ever want to enable preemption for my queue. It sounds like resources will be taken from my queue when some other queue is in need.

*Ans*: If you enable preemption on your queue, it allows us to safely increase your `max_capacity`, allowing you to use a much higher percentage of idle cluster resources. For example, if your `max_capacity` is currently at 20% of the cluster and you agree to enable preemption for your queue, it's likely we would increase the `max_capacity` of your queue to a much higher value (75%+). So, rather than always being limited to 20%, you can use up to 75% of the cluster with the caveat that  when the cluster gets very busy, some of those resources are likely to be taken back so that other queues get their guaranteed capacity.


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

Can I designate a job as non-preemptable?
-----------------------------------------

No. Only queues can be configured as preemptable/non-preemptable

Will the scheduler preempt things within a queue to satisfy a higher priority job within that queue?
----------------------------------------------------------------------------------------------------

No.  Preemption of lower priority resources can be configured on, but by default it is configured off. Higher priority jobs are offered resources ahead of lower priority jobs within the same queue, but resources which are already allocated to lower priority jobs are not currently taken away.


Which clusters will have preemption enabled?
---------------------------------------------

Preemption is enabled in the default queue in VerizonMedia clusters and can be used by any user. However, there are *no* SLA guarantees for jobs running in the default queue.
All queues in VCG have preemption enabled except the GPU queues.

Sometimes my job in the default queue runs really fast, and sometimes it seems to take forever
-----------------------------------------------------------------------------------------------

When the cluster is busy, the default queue does not have a lot of capacity associated with it so jobs may appear to run slowly. When the cluster is mostly idle, the default queue can make use of these idle resources and therefore jobs can run orders of magnitude faster.

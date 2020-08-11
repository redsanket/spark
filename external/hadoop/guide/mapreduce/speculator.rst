..  _mapreduce_speculation:

*********************
Speculative Execution
*********************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Hadoop aims at reducing the completion time of each individual job by
splitting it into tasks that run in parallel. It is evident that such an
execution model is sensitive to slow tasks, dubbed “*stragglers*”, which
impede the overall execution time of a job. This leads to a degradation
of resource allocations; hence the loss of value of data and revenue. To
reduce the impact of stragglers, Hadoop deploys a submodule,
“*Speculator*”, that periodically detects a straggler and launches a new
“*speculative*” task that competes with the straggler to reach
completion. The speculator relies on predefined thresholds to identify
tasks that are exhibiting low progression compared to the average
successful tasks.

Is It Beneficial?
=================

Hadoop MapReduce Speculative execution is beneficial in some cases because in a Hadoop cluster with 100s of nodes, problems like hardware failure or network congestion are common and running parallel or duplicate task would be better since we won’t be waiting for the task in the problem to complete.
But if two duplicate tasks are launched at about same time, it will be a wastage of cluster resources.

:token:`The Straggler Problem`

A MapReduce job is not completed until map and reduce
tasks are all done. Slow running tasks (`stragglers`) can have
significant impact in deteriorating the execution time of the
job. The straggler phenomenon became the norm in large
scale distributed systems. The straggler tasks add a tailing
shape that exceeds 10 times longer compared to average task
duration.

A straggler task occurs for multiple reasons:

* hardware;
* networking;
* resource contention; or
* skewed data input.


Hadoop has no built-in capabilities to analyze the bottlenecks.
Instead, Hadoop detects slow tasks and launches
speculative task that compete with the original task. When
either task completes, it kills the other one. The process of
running speculative tasks is an optimistic approach based
on the intuition that a new speculative task will likely run
on a different node that does not exhibit hardware failure or
network congestion.

.. _mapreduce_speculation_how_it_works:

How does it work?
=================

Mapreduce Speculation
---------------------

Before we explain how the speculative execution works, we need to understand the task progress
calculation.

Hadoop makes several assumptions about the progress rate
of a job including:

* cluster nodes are homogenous and perform at same rate,
* tasks within the same category (i.e., map or reduce) require the same amount of work, and
* tasks have constant progress rate.

Each task has "progress score (:abbr:`PS (progress score)`)" between 0 and 1.
For map tasks, :abbr:`PS (progress score)` is the fraction of the input processed (:math:`L`).
For reduce tasks, the progress is divided equally between three phases:

* *copy*, when map outputs are fetched;
* *sort*, when map outputs are sorted by key; and
* *reduce*, when reduce function is applied.


Hence, :abbr:`PS (progress score)` in Eq. :eq:`eq-task-progres` is calculated as the number of finished phases
(:math:`P`) and the fraction of processed input in the current phase.
With each phase characterized by different workload (CPU intensity, I/O
and memory), it is unlikely that copying and sorting invest the same
time compared to applying the reduce function. :numref:`fig-reduce-progress` shows how the progress
rate of a reduce task typically changes throughout the execution time
based on the workload of the relevant phase. Assuming equal weighting of
different phases reduces the accuracy of the speculative decision as
mentioned in .

.. math::
   :label: eq-task-progres
    
    \textit{PS} =
    \begin{cases}
    \dfrac{L}{R} & \textit{for map tasks}\\
    (P + \dfrac{L}{R}) \times \dfrac{1}{3} & \textit{for reduce tasks}\\
    \end{cases}

The speculator looks at tasks that run for more than a minute and their
estimated finishing time exceeds the average of their category
(map/reduce). 

.. _fig-reduce-progress:

.. figure:: /images/mapreduce/speculator/reduce-progress.jpg
  :alt: progress rate of a reduce task
  :width: 70%
  :align: center

  Relation between the progress rate of a reduce task (y-axis) in each different phase vs. the execution time (x-axis)

Progress rate of each task is :math:`\dfrac{\textit{PS}}{T}`, where :math:`T` is
the amount of time the task has been running for, and then estimate the
time to completion. Note that this calculation is based on the
assumption that all tasks progress at a constant rate.
The speculator looks at tasks that run for more than a minute and their estimated finishing time
exceeds the average of their category (map/reduce).
the task with farthest finish time into the future is
labeled *straggler* and a new task is speculated. The intuition is that
the straggler task provides greater opportunity for a speculative backup
to overtake the original task.

Tez Speculation
---------------

Tez allows users to have more fine grained control of the data plan by
modeling computation into a structure of a data processing workflow
(Directed-Acyclic-Graph, or simply *DAG*) :cite:`Saha:2015`. Each
*vertex* represents a logical step of transforming and processing the
data. In distributed processing, the work represented by a single vertex
is physically executed as a set of *tasks* running on potentially
multiple machines of the cluster. The direction of an *edge* represents
movement of data between producers and consumers.

Tez deploys a similar technique to mitigate the impact of stragglers.
Unlike MapReduce, Tez monitors task progress and tries to detect
straggler tasks that may be running much slower than other tasks within
the *same vertex*. Once a vertex starts execution, it launches its own
speculator service. The speculator scans only the tasks within its
parent vertex looking for tasks with :abbr:`ECT (Estimated Completion Time)` exceeding the average
completion time of the vertex’s tasks. Upon detecting such a task, a
speculative attempt may be launched that runs in parallel with the
original task and races it to completion. If the speculative attempt
finishes first then it is successful in improving the completion time.

This model has its own challenge in speculation as it adds another dimension to the task progress.
It is unclear how Dependency between vertex is accounted for in the task progress calculation.
This is very similar to the way reduce tasks in Hadoop are inaccurate due to the fetch phase.
Since Tez DAGs are deeper than MapReduce, this effect is even more pronounced, and even has
cascading effects.

Another major difference between Tez and Hadoop is that Tez avoids scheduling the speculative
task on the node running the original task. This improves the success rate of the speculative tasks
as the new launched tasks avoid running on nodes that may be exhibiting some performance issues.

.. note:: We found that many speculative tasks share the same nodes as the original tasks which defies the purpose of speculative tasks. There is a work in-progress Mapreduce to address this issue. See `MAPREDUCE-7169 <https://issues.apache.org/jira/browse/MAPREDUCE-7169>`_.

Algorithms
==========

The :abbr:`ECT (Estimated Completion Time)` of tasks are calculated using a "*Static formula*" as described in :numref:`mapreduce_speculation_how_it_works`. Some tasks may exhibit different behavior throughout the runtime
composed of a sequence of phases of execution. Therefore, the speculator that assumes linear proportion between time and progress will not be accurate. This limitation is emphasized in :numref:`fig-reduce-progress-limitations` where the ECT  his algorithm is provided by the runtime estimator class
:hadoop_rel_doc:`LegacyTaskRuntimeEstimator <hadoop-mapreduce-client/hadoop-mapreduce-client-app/apidocs/org/apache/hadoop/mapreduce/v2/app/speculate/LegacyTaskRuntimeEstimator.html>`

Another alternative to measure :abbr:`ECT (Estimated Completion Time)` is to switch to "*Exponential Smoothing*" which is a technique that takes into considerations all
past readings and smoothes time series data using the exponential
window function assigning decreasing weights to previous data over time. This can be achieved by using Class  - :hadoop_rel_doc:`SimpleExponentialTaskRuntimeEstimator <hadoop-mapreduce-client/hadoop-mapreduce-client-app/apidocs/org/apache/hadoop/mapreduce/v2/app/speculate/SimpleExponentialTaskRuntimeEstimator.html>`.

.. admonition:: Reading...
   :class: readingbox

   Read more about the Exponentially Smooth Estimator in :download:`TechPulse-282'19 </resources/mapreduce/speculator/techpulse19-282.pdf>` and a quick overview to its :download:`poster </resources/mapreduce/speculator/techpulse19-282-poster.pdf>`.

.. _fig-reduce-progress-limitations:

.. figure:: /images/mapreduce/speculator/reduce-progress-task-problem.jpg
  :alt: downward progress rate of a reduce task
  :width: 70%
  :align: center

  Limitation of using linear estimator (`LegacyEstimator`) to calculate :abbr:`ECT (Estimated Completion Time)`. Relation between the progress rate of a reduce task (y-axis) in each different phase vs. the execution time (x-axis). 

Configuration
=============

The main work of speculative execution is to reduce the job execution time; however, the clustering efficiency is affected due to duplicate tasks. Since in speculative execution redundant tasks are being executed, thus this can reduce overall throughput. For this reason, some cluster administrators prefer to turn off the speculative execution in Hadoop. :numref:`table-speculative-properties` lists the knobs that controls the speculative execution. |br|
Turning On/Off speculative execution is done using the two following properties:

* ``mapreduce.map.speculative`` – If true, then multiple instances of some map tasks may be executed in parallel. Default is true.
* ``mapreduce.reduce.speculative`` – If true, then multiple instances of some reduce tasks may be executed in parallel. Default is true.


.. table:: `Configuration for Speculative execution: Prefixed by "mapreduce.job.speculative."`
  :widths: auto
  :name: table-speculative-properties

  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |              Property             | Default |                                                                             Description                                                                             |
  +===================================+=========+=====================================================================================================================================================================+
  | ``speculative-cap-running-tasks`` | 0.1     | The max percent (0-1) of running tasks that can be speculatively re-executed at any time.                                                                           |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``speculative-cap-total-tasks``   | 0.01    | The max percent (0-1) of all tasks that can be speculatively re-executed at any time.                                                                               |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``minimum-allowed-tasks``         | 10      | The minimum allowed tasks that can be speculatively re-executed at any time.                                                                                        |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``retry-after-no-speculate``      | 1000    | The waiting time(ms) to do next round of speculation if there is no task speculated in this round.                                                                  |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``retry-after-speculate``         | 15000   | The waiting time(ms) to do next round of speculation if there are tasks speculated in this round.                                                                   |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``slowtaskthreshold``             | 1.0     | The number of standard deviations by which a task's ave progress-rates must be lower than the average of all running tasks' for the task to be considered too slow. |
  +-----------------------------------+---------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------+

The property ``yarn.app.mapreduce.am.job.task.estimator.class`` accepts either one of the following options:

* ``org.apache.hadoop.mapreduce.v2.app.speculate.SimpleExponentialTaskRuntimeEstimator``: which is the default value for hadoop running on verizon clusters. 
* ``org.apache.hadoop.mapreduce.v2.app.speculate.LegacyTaskRuntimeEstimator``: which is the default for Tez.


When the Job is configured to use ``SimpleExponentialTaskRuntimeEstimator``, the performance of the speculator by tuning the properties of the smooth estimator. The list in :numref:`table-exponential-smoothing-properties` describes those properties which are per-job specific:


.. table:: `Exponetial-Estimator Forecast Parameters: Prefixed by "yarn.app.mapreduce.am.job.task.estimator.simple.exponential.smooth.".`
  :widths: auto
  :name: table-exponential-smoothing-properties

  +-------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+
  |      Property     | Default |                                                                         Description                                                                         |
  +===================+=========+=============================================================================================================================================================+
  | ``lambda-ms``     | 120,000 | The lambda value in the smoothing function of the task estimator.                                                                                           |
  +-------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``stagnated-ms``  | 360,000 | The window length in the simple exponential smoothing that considers the task attempt is stagnated.                                                         |
  +-------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+
  | ``skip-initials`` | 24      | The number of initial readings that the estimator ignores before giving a prediction. At the beginning the smooth estimator won't be accurate in prediction |
  +-------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------+

.. important:: Make sure you read :download:`TechPulse-282'19 paper </resources/mapreduce/speculator/techpulse19-282.pdf>` before you change those properties.

:numref:`fig-graph-smooth-behavior` illustrates how the `Exponential-Estimator`'s behavior is controled by changing the value of "`lambda-ms`". Compared to :numref:`fig-reduce-progress-limitations`, we clearly see that `Exponential-Estimator` has an edge of the `LegacyEstimator` because it gives the option to tune the :abbr:`ECT (Estimated Completion Time)` calculation. The smoothed statistic :math:`s_t` is the weighted average of the previous smoothed statistic :math:`s_{t-1}` and the current reading
:math:`x_t`:

.. math::
   :label: eq-smooth-forecast
    
   s_{t} = \alpha \cdot x_{t} + (1 - \alpha) \cdot s_{t - 1} = s_{t-1} + \alpha \cdot (x_{t} - s_{t-1})

where :math:`\alpha` is the smoothing factor :math:`0 < \alpha < 1`.
Larger values of :math:`\alpha` gives higher weight to recent changes in
data which in turn reduces the smoothing level, eventually giving the
full weight to the current reading with :math:`\alpha = 1`. However, a
desirable accuracy of the output will not be achieved until several
readings have been sampled.

.. _fig-graph-smooth-behavior:

.. figure:: /images/mapreduce/speculator/graph-smooth-behavior.png
  :alt: downward progress rate of a reduce task
  :width: 80%
  :align: center

  Relation between :abbr:`ECT (Estimated Completion Time)` and the `Exponential-Estimator` Forecast Parameters (`lambda-ms)`. The progress rate of a reduce task (y-axis) in each different phase vs. the execution time (x-axis)


Resources
=========

.. include:: /common/mapreduce/speculator-reading-resources.rst


..  _hadoop_team_yarn_speculator:

##########
Speculator
##########


By default, MR uses LegacyTaskRuntimeEstimator to get an estimate of the runtime.  The estimator does not adjust dynamically to the progress rate of the tasks. On the other hand, the existing alternative "ExponentiallySmoothedTaskRuntimeEstimator" behavior in unpredictable.



There are several dimensions to improve the exponential implementation:

* Exponential shooting needs a warmup period. Otherwise, the estimate will be affected by the initial values.

* Using a single smoothing factor (Lambda) does not work well for all the tasks. To increase the level of smoothing across the majority of tasks, we need to give a range of flexibility to dynamically adjust the smoothing factor based on the history of the task progress.

* Design wise, it is better to separate between the statistical model and the MR interface. We need to have a way to evaluate estimators statistically, without the need to run MR. For example, an estimator can be evaluated as a black box by using a stream of raw data as input and testing the accuracy of the generated stream of estimates.

* The exponential estimator speculates frequently and fails to detect slowing tasks. It does not detect slowing tasks. As a result, a taskAttempt that does not do any progress won't trigger a new speculation.

**Resources:**

TechPulse19-282
  A Comprehensive Review of Speculation for Hadoop Ecosystem

   - :download:`PDF  </resources/techpulse19-282.pdf>`
   - :download:`Poster  </resources/techpulse19-282-poster.pdf>`

.. _hadoop_team_yarn_algorithms:

**********
Algorithms
**********

Legacy Runtime Estimator
========================

.. todo:: Explain the legacy runtime estimator here (start to end)

Exponentially Smoothed Implementation in Hadoop
===============================================

**Example of DAG in MapReduce:**

Tez UI for the Dilithium Blue cluster:
https://dilithiumblue-jt1.blue.ygrid.yahoo.com:50508/tez/ Sample job
with multiple vertices:
https://dilithiumblue-jt1.blue.ygrid.yahoo.com:50508/tez/#/dag/dag_1542216896066_17804406_1

.. code-block:: java

   // source:
   // hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/
   // src/main/java/org/apache/hadoop/mapreduce/v2/app/speculate/
   // ExponentiallySmoothedTaskRuntimeEstimator.java

   EstimateVector incorporate(float newProgress, long newAtTime) {
     if (newAtTime <= atTime || newProgress < basedOnProgress) {
       return this;
     }

     double oldWeighting
         = value < 0.0
             ? 0.0 : Math.exp(((double) (newAtTime - atTime)) / lambda);

     double newRead = (newProgress - basedOnProgress) / (newAtTime - atTime);

     if (smoothedValue == SmoothedValue.TIME_PER_UNIT_PROGRESS) {
       newRead = 1.0 / newRead;
     }

     return new EstimateVector
         (value * oldWeighting + newRead * (1.0 - oldWeighting),
          newProgress, newAtTime);
   }

How Basic Exponential Smooth works?
-----------------------------------

**Smoothing Factor, and estimated Value:**

The raw data sequence is often represented by beginning at time
:math:`t = 0`, and the output of the exponential smoothing algorithm is
commonly written as :math:`s_t`, which may be regarded as a best estimate of what
the next value of will be. When the sequence of observations begins at
time :math:`t = 0`, the simplest form of exponential smoothing is given by the
formulas:

.. math::

  s_0 = x_0

.. math::

  s_t = \alpha x_t + (1 - \alpha) s_{t-1} = s_{t-1} + \alpha (x_t - s_{t-1}), t > 0

Where α is the smooting factor, and :math:`0 < \alpha < 1`

larger values of α actually reduce the level of smoothing, and in the
limiting case with ``α = 1`` the output series is just the current
observation. Values of α close to one have less of a smoothing effect
and give greater weight to recent changes in the data, while values of α
closer to zero have a greater smoothing effect and are less responsive
to recent changes. There are some techniques to optimize the value of α
(least squares).

A “good average” will not be achieved until several samples have been
averaged together; for example, a constant signal will take
approximately ``3 / α`` stages to reach ``95%`` of the actual value. To
accurately reconstruct the original signal without information loss all
stages of the exponential moving average must also be available, because
older samples decay in weight exponentially. This is in contrast to a
simple moving average, in which some samples can be skipped without as
much loss of information due to the constant weighting of samples within
the average. If a known number of samples will be missed, one can adjust
a weighted average for this as well, by giving equal weight to the new
sample and all those to be skipped.

**Time Constant:**

The time constant of an exponential moving average is the amount of time
for the smoothed response of a unit set function to reach :math:`1 - 1/e \approx 63` of the
original signal. The relationship between this time constant, :math:`\tau` , and the
smoothing factor, α, is given by the formula:

.. math::

  \alpha = 1 - e^{\frac{-\Delta\text{T}}{\tau}}

Where :math:`\Delta\text{T}` is the sampling time interval of the discrete time implementation.
If the sampling time is fast compared to the time constant :math:`\Delta T \ll \tau` then

.. math::

  \alpha \approx \frac{\Delta\text{T}}{\tau}

How Double Exponential Smooth works?
------------------------------------

The basic idea behind double exponential smoothing is to introduce a
term to take into account the possibility of a series exhibiting some
form of trend. This slope component is itself updated via exponential
smoothing.

*Holt-Winters double exponential smoothing* works as follows:

The raw data sequence is often represented by :math:`x_t` beginning at time :math:`t=0`, to
represent the smoothed value for time :math:`t`, and :math:`b_t`is our best estimate of the
trend at time :math:`t`. The output of the algorithm is now written as :math:`F_{t+m}``, an
estimate of the value of :math:`x` at time :math:`t+m, \ \text{for} \ m > 0` based on the raw data up
to time :math:`t`. The double exponential smoothing is:

  .. math::

    s_1 = x_1

    b_1 = x_1 - x_0,\ \text{and}

:math:`\text{for}\ t > 1`

  .. math::

    s_t = \alpha x_t + (1-\alpha)(s_{t-1} + b_{t-1})

    b_t = \beta (s_t + s_{t-1}) + (1-\beta)\;b_{t-1}

where α is the data smoothing factor, :math:`0 < \alpha < 1`, and :math:`\beta` is the trend smoothing
factor, :math:`0 < \beta < 1`

* The first smoothing equation adjusts :math:`s_t` directly for the trend of the previous period, :math:`b_{t-1}`, by adding it to the last smoothed value, :math:`s_{t-1}`.
   This helps to eliminate the lag and brings :math:`s_t` to the appropriate base of the current value.
* The second smoothing equation then updates the trend, which is expressed as the difference between the last two values.
  The equation is similar to the basic form of single smoothing, but here applied to the updating of the trend.
* The values for α and :math:`\beta` can be obtained via non-linear optimization techniques, such as the Marquardt Algorithm.

To forecast beyond :math:`x_t`

.. math::
  F_{t+m} = s_t + mb_t

Setting the initial value :math:`b_0` is a matter of preference. An option other
than the one listed above is :math:`\frac{(x_n - x_0)}{n}\, \text{for some}\ n > 1`.

Also see `Brown’s double exponential
smoothing <http://www.spiderfinancial.com/support/documentation/numxl/reference-manual/smoothing/lesmth>`_.
It has only one factor which may be easier to configure compared to
having two different factors.


..  _hadoop_team_yarn_speculator_implementation:

Implementation
==============


.. figure:: /images/speculator/speculator-chart-service.jpg
   :alt:  Fig 02-27-A: Speculative Poll Service

   Fig 02-27-A: Speculative Poll Service

----------

.. figure:: /images/speculator/speculator-chart-computeSpeculations.jpg
   :alt:  Fig 02-27-B: Compute Speculation

   Fig 02-27-B: Compute Speculation

----------

.. figure:: /images/speculator/speculator-chart-speculationValue.jpg
   :alt:  Fig 02-27-C: Speculation Value

   Fig 02-27-C: Speculation Value.

----------

.. figure:: /images/speculator/speculator-chart-startEnd-updateAttempt.jpg
   :alt:  Fig 02-27-D: StartEndEstimator.UpdateAttempt()

   Fig 02-27-D: StartEndEstimator.UpdateAttempt()

----------

.. figure::  /images/speculator/speculator-chart-legacy-updateAttempt.jpg
   :alt:  Fig 02-27-E: LegacyEstimator.UpdateAttempt()

   Fig 02-27-E: LegacyEstimator.UpdateAttempt()

----------

.. figure:: /images/speculator/speculator-chart-exponential-updateAttempt.jpg
   :alt:  Fig 02-27-F: ExponentialEstimator.UpdateAttempt()

   Fig 02-27-F: ExponentialEstimator.UpdateAttempt()

----------

.. figure:: /images/speculator/speculator-chart-exponential-estimatedruntime.jpg
   :alt:  Fig 02-27-G: ExponentialEstimator.EstimatedRuntime()

   Fig 02-27-G: ExponentialEstimator.EstimatedRuntime()

..  _speculator_runtime_testing:

Runtime Testing
===============

.. todo:: list the starling query and how to check the versions



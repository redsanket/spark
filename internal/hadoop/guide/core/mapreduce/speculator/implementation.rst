..  _hadoop_team_yarn_speculator_implementation:

**************
Implementation
**************


.. figure:: /images/speculator/speculator-chart-service.jpg
   :alt:  Fig 02-27-A: Speculative Poll Service
   :width: 100%
   :align: center

   Speculative Poll Service

----------

.. figure:: /images/speculator/speculator-chart-computeSpeculations.jpg
   :alt:  Fig 02-27-B: Compute Speculation
   :width: 100%
   :align: center

   Compute Speculation

----------

.. figure:: /images/speculator/speculator-chart-speculationValue.jpg
   :alt:  Fig 02-27-C: Speculation Value
   :width: 100%
   :align: center
   
   Speculation Value.

----------

.. figure:: /images/speculator/speculator-chart-startEnd-updateAttempt.jpg
   :alt:  Fig 02-27-D: StartEndEstimator.UpdateAttempt()
   :width: 100%
   :align: center

   StartEndEstimator.UpdateAttempt()

----------

.. figure::  /images/speculator/speculator-chart-legacy-updateAttempt.jpg
   :alt:  Fig 02-27-E: LegacyEstimator.UpdateAttempt()
   :width: 100%
   :align: center

   LegacyEstimator.UpdateAttempt()

----------

.. figure:: /images/speculator/speculator-chart-exponential-updateAttempt.jpg
   :alt:  Fig 02-27-F: ExponentialEstimator.UpdateAttempt()
   :width: 100%
   :align: center

   ExponentialEstimator.UpdateAttempt()

----------

.. figure:: /images/speculator/speculator-chart-exponential-estimatedruntime.jpg
   :alt:  Fig 02-27-G: ExponentialEstimator.EstimatedRuntime()
   :width: 100%
   :align: center

   ExponentialEstimator.EstimatedRuntime()
..  _hadoop_team_yarn_speculator_implementation:

**************
Implementation
**************


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
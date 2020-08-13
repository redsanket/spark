.. table:: `Yarn NodeManager: Configuration Parameters for External Health Script prefixed by yarn.nodemanager.disk-health-checker.`
  :widths: auto

  +-----------------------+---------------------+---------------------------------------------------------------------------------------------------------+
  |          Name         | Allowed |br| Values |                                               Description                                               |
  +=======================+=====================+=========================================================================================================+
  | ``interval-ms``       | Postive integer     | The interval, in milliseconds, at which health checker service runs; the default value is `10` minutes. |
  +-----------------------+---------------------+---------------------------------------------------------------------------------------------------------+
  | ``script.timeout-ms`` | Postive integer     | The timeout for the health script that’s executed; the default value is `20` minutes.                   |
  +-----------------------+---------------------+---------------------------------------------------------------------------------------------------------+
  | ``script.path``       | String              | Absolute path to the health check script to be run.                                                     |
  +-----------------------+---------------------+---------------------------------------------------------------------------------------------------------+
  | ``script.opts``       | String              | Arguments to be passed to the script when the script is executed.                                       |
  +-----------------------+---------------------+---------------------------------------------------------------------------------------------------------+

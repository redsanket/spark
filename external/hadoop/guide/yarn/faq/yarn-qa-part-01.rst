How to run Hadoop Programs on the Gateway Machines?
===================================================

Running mapred jobs on the gateways, is now an expert option. (This decision was made because several users had unintentionally run their full-fledged jobs on the gateway machines in the past, when they really wanted to run the jobs on the grid instead.)

Here is how you can test your jobs (on a much smaller dataset, please) on the gateway machine itself you need to ssh into  cluster gateway, pkinit, and finally run your hadoop program.
Please, refer to Bdml-guide instructions:

* `Onboarding ... Gateways and Launchers <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/Onboarding_..._Gateways_and_Launchers/>`_
* `Accessing Gateways <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/Grid_Components_(formerly_Overview_of_Yahoo_Grid)/#accessing-gateways>`_ and `SSH to a Gateway <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#ssh-to-a-gateway>`_
* `Enable Authentication Using PKINIT <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#enable-authentication-using-pkinit>`_
* `Copy files to HDFS <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#explore-hdfs>`_
* Finally, run your hadoop program. See the example in "`Run MapReduce Jobs <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#run-mapreduce-jobs>`_".

How to Compute Hadoop Job Cost?
===============================

.. include:: /common/yarn/yarn-compute-job-cost.rst

Yarn Commands
=============

How to Run a jar file?
----------------------

Users can bundle their YARN code in a `jar` file and execute it using this command.

  .. code-block:: bash

    yarn jar <jar> [mainClass] args...

How to get logs on Yarn?
------------------------

As described in Sec. :ref:`yarn-commands`, Yarn provides a set of commands useful for users, including:


  .. include:: /common/yarn/yarn-commands-logs.rst

How to Use a library for Yarn Job?
----------------------------------

  .. code-block:: bash

    yarn classpath [--glob |--jar <path> |-h |--help]

Specifies the classpath that should be used by the containers (processes) that are launched by YARN. This classpath references all of the required Hadoop jar files with their locations on the cluster nodes.

  .. table:: 
    :widths: auto

    +--------------------+-----------------------------------------------+
    |       Options      |                  Description                  |
    +====================+===============================================+
    | ``--glob``         | expand wildcards                              |
    +--------------------+-----------------------------------------------+
    | ``--jar`` path     | write classpath as manifest in jar named path |
    +--------------------+-----------------------------------------------+
    | ``-h``, ``--help`` | print help                                    |
    +--------------------+-----------------------------------------------+

Prints the class path needed to get the Hadoop jar and the required libraries. If called without arguments, then prints the classpath set up by the command scripts, which is likely to contain wildcards in the classpath entries. Additional options print the classpath after wildcard expansion or write the classpath into the manifest of a jar file. The latter is useful in environments where wildcards cannot be used and the expanded classpath exceeds the maximum supported command line length.

How To Check the Environment Variables?
---------------------------------------

Display computed Hadoop environment variables using the command:

  .. code-block:: bash

     yarn envvars


How To Submit my Own Node Health Script?
----------------------------------------

As described in :numref:`yarn-nm-health-scripts`, users may specify their own health checker script that will be invoked by the health checker service. Users may specify a `timeout` as well as options to be passed to the script. If the script exits with a non-zero exit code, times out or results in an exception being thrown, the node is marked as `unhealthy`. 

Please note:

* if the script cannot be executed due to permissions or an incorrect path, etc, then it counts as a failure and the node will be reported as `unhealthy`.
* specifying a health check script is not mandatory. If no script is specified, only the disk checker status will be used to determine the health of the node.

  .. include:: /common/yarn/yarn-nm-health-conf.rst

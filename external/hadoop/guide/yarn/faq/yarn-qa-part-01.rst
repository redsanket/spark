How to run hadoop programs on the gateway machines
==================================================

Running mapred jobs on the gateways, is now an expert option. (This decision was made because several users had unintentionally run their full-fledged jobs on the gateway machines in the past, when they really wanted to run the jobs on the grid instead.)

Here is how you can test your jobs (on a much smaller dataset, please) on the gateway machine itself you need to ssh into  cluster gateway, pkinit, and finally run your hadoop program.
Please, refer to Bdml-guide instructions:

* `Onboarding ... Gateways and Launchers <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/Onboarding_..._Gateways_and_Launchers/>`_
* `Accessing Gateways <https://git.vzbuilders.com/pages/developer/Bdml-guide/migrated-pages/Grid_Components_(formerly_Overview_of_Yahoo_Grid)/#accessing-gateways>`_ and `SSH to a Gateway <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#ssh-to-a-gateway>`_
* `Enable Authentication Using PKINIT <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#enable-authentication-using-pkinit>`_
* `Copy files to HDFS <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#explore-hdfs>`_
* Finally, run your hadoop program. See the example in "`Run MapReduce Jobs <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#run-mapreduce-jobs>`_".



https://dzone.com/articles/configuring-memory-for-mapreduce-running-on-yarn

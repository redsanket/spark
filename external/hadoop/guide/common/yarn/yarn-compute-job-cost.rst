Doppler computes job costs in a few places outside of the project pages:

`Hadoop Apps <http://yo/hadoop-apps>`_
  Job level costs with handy links to the grid UI for the job.  Fastest updates. 
`Doppler Downsizer <http://yo/downsizer>`_
  Useful tool for evaluating memory usage.  Container data takes more time to
  gather, usually a few days behind. 
`TCO Dashboard <http://yo/grid-tco>`_
  Monthly rollup of cost by user, business unit, etc.  Slowest updates (monthly)

.. admonition:: Reading...
   :class: readingbox

    .. include:: /common/yarn/yarn-tco-reading-resources.rst


.. rubric:: Using Hadoop Apps

.. figure:: /images/yarn/tco/hadoop-apps-ui.png
  :alt: UI of the Hadoop-Apps
  :width: 100%
  :align: center

  UI of the Hadoop-Apps


`Pig` jobs have names that start with "`PigLatin:`".
`Hive` jobs have names that start with "`Hive:`".



.. figure:: /images/yarn/tco/hadoop-apps-table.png
  :alt: UI of the Hadoop-Apps table
  :width: 100%
  :align: center

  The values at the right are the Memory in GB-Hrs and vCores in (vCore hrs).  


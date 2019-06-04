Presto Clusters
###############

Currently, there is one Presto cluster each in gq1 and ne1. By end of Q3 2019, we will have one staging
and production cluster per colo.

**Note:**
  Presto production clusters are only for production BI analytics, and
  not for general use. The ``default`` queue is currently available for users to
  benchmark and request capacity if Presto suits their use case. If you came to
  know of Presto through someone and is using it as a better performing alternative to
  `Hive <https://git.ouroath.com/pages/hadoop/docs/hive/index.html>`_, please expect the
  access to be revoked soon. Only customers who paid for capacity will allowed to use Presto.
  `Hive <https://git.ouroath.com/pages/hadoop/docs/hive/index.html>`_ should be used
  by everyone else.

Below is the list of available Presto cluster deployments and the hive catalogs in each.
Each hive catalog by name corresponds to the Hive Metastore of the Hadoop
clusters by the same name.

.. _ygrid_presto_clusters:

YGRID
*****
+------+-------------+-----------------------------------------------------+---------------+
| colo | environment | Presto cluster                                      | Hive Catalogs |
+======+=============+=====================================================+===============+
| gq1  | production  | https://xandarblue-presto.blue.ygrid.yahoo.com:4443 | jetblue       |
|      |             |                                                     |               |
|      |             |                                                     | uraniumblue   |
|      |             |                                                     |               |
|      |             |                                                     | oxiumblue     |
|      |             |                                                     |               |
|      |             |                                                     | bassniumblue  |
|      |             |                                                     |               |
|      |             |                                                     | phazonblue    |
|      |             |                                                     |               |
|      |             |                                                     | dilithiumblue |
+------+-------------+-----------------------------------------------------+---------------+
| ne1  | production  | https://xandartan-presto.blue.ygrid.yahoo.com:4443  | tiberiumtan   |
|      |             |                                                     |               |
|      |             |                                                     | uraniumtan    |
|      |             |                                                     |               |
|      |             |                                                     | zaniumtan     |
|      |             |                                                     |               |
|      |             |                                                     | bassniumtan   |
|      |             |                                                     |               |
|      |             |                                                     | phazontan     |
+------+-------------+-----------------------------------------------------+---------------+

.. _vcg_presto_clusters:

VCG
***
+------+-------------+---------------------------------------------+---------------+
| colo | environment | Presto cluster                              | Hive Catalogs |
+======+=============+=============================================+===============+
| gq   | staging     | https://hothgq-presto.gq.vcg.yahoo.com:4443 | kessel        |
|      |             |                                             |               |
|      |             |                                             | polaris       |
+------+-------------+---------------------------------------------+---------------+

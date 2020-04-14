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
  access to be revoked soon. Only customers who paid for capacity will be allowed to use Presto.
  `Hive <https://git.ouroath.com/pages/hadoop/docs/hive/index.html>`_ should be used
  by everyone else.

Below is the list of available Presto cluster deployments and the hive catalogs in each.
Each hive catalog by name corresponds to the Hive Metastore of the Hadoop
clusters by the same name.

.. _ygrid_presto_clusters:

YGRID
*****
+------+-------------+-----------+-----------------------------------------------------------+---------------+
| colo | environment | shortname | Presto Cluster CLI and JDBC URLs                          | Hive Catalogs |
+======+=============+===========+===========================================================+===============+
| gq1  | staging     | YB        | https://yodablue-presto.blue.ygrid.yahoo.com:4443         | axoniteblue   |
|      |             |           |                                                           |               |
|      |             |           | jdbc:presto://yodablue-presto.blue.ygrid.yahoo.com:4443   | jetblue       |
|      |             |           |                                                           |               |
|      |             |           |                                                           | uraniumblue   |
|      |             |           |                                                           |               |
|      |             |           |                                                           | oxiumblue     |
|      |             |           |                                                           |               |
|      |             |           |                                                           | bassniumblue  |
|      |             |           |                                                           |               |
|      |             |           |                                                           | phazonblue    |
|      |             |           |                                                           |               |
|      |             |           |                                                           | dilithiumblue |
+------+-------------+-----------+-----------------------------------------------------------+---------------+
| ne1  | staging     | YT        | https://yodatan-presto.tan.ygrid.yahoo.com:4443           | tiberiumtan   |
|      |             |           |                                                           |               |
|      |             |           | jdbc:presto://yodatan-presto.tan.ygrid.yahoo.com:4443     | uraniumtan    |
|      |             |           |                                                           |               |
|      |             |           |                                                           | zaniumtan     |
|      |             |           |                                                           |               |
|      |             |           |                                                           | bassniumtan   |
|      |             |           |                                                           |               |
|      |             |           |                                                           | phazontan     |
+------+-------------+-----------+-----------------------------------------------------------+---------------+
| bf1  | staging     | YR        | https://yodared-presto.red.ygrid.yahoo.com:4443           | axonitered    |
|      |             |           |                                                           |               |
|      |             |           | jdbc:presto://yodared-presto.red.ygrid.yahoo.com:4443     | mithrilred    |
|      |             |           |                                                           |               |
|      |             |           |                                                           | dilithiumred  |
|      |             |           |                                                           |               |
|      |             |           |                                                           | bassniumred   |
|      |             |           |                                                           |               |
|      |             |           |                                                           | phazonred     |
+------+-------------+-----------+-----------------------------------------------------------+---------------+
| gq1  | production  | XB        | https://xandarblue-presto.blue.ygrid.yahoo.com:4443       | jetblue       |
|      |             |           |                                                           |               |
|      |             |           | jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443 | uraniumblue   |
|      |             |           |                                                           |               |
|      |             |           |                                                           | oxiumblue     |
|      |             |           |                                                           |               |
|      |             |           |                                                           | bassniumblue  |
|      |             |           |                                                           |               |
|      |             |           |                                                           | phazonblue    |
|      |             |           |                                                           |               |
|      |             |           |                                                           | dilithiumblue |
+------+-------------+-----------+-----------------------------------------------------------+---------------+
| ne1  | production  | XT        | https://xandartan-presto.tan.ygrid.yahoo.com:4443         | tiberiumtan   |
|      |             |           |                                                           |               |
|      |             |           | jdbc:presto://xandartan-presto.tan.ygrid.yahoo.com:4443   | uraniumtan    |
|      |             |           |                                                           |               |
|      |             |           |                                                           | zaniumtan     |
|      |             |           |                                                           |               |
|      |             |           |                                                           | bassniumtan   |
|      |             |           |                                                           |               |
|      |             |           |                                                           | phazontan     |
+------+-------------+-----------+-----------------------------------------------------------+---------------+

.. _vcg_presto_clusters:

VCG
***

If you are accessing the VCG clusters directly from Verizon network instead of VMG (Verizon Media) network,
please use the Verizon specific URLs (-dhd) as the connection has to be routed through ATS proxies.

+------+-------------+-----------+-------------------------------------------------------+---------------+
| colo | environment | shortname | Presto Cluster CLI and JDBC URLs                      | Hive Catalogs |
+======+=============+===========+=======================================================+===============+
| gq2  | staging     | HGQ       | From VMG:                                             | kessel        |
|      |             |           |                                                       |               |
|      |             |           | https://hothgq-presto.gq.vcg.yahoo.com:4443           | polaris       |
|      |             |           |                                                       |               |
|      |             |           | jdbc:presto://hothgq-presto.gq.vcg.yahoo.com:4443     |               |
|      |             |           |                                                       |               |
|      |             |           | From Verizon:                                         |               |
|      |             |           |                                                       |               |
|      |             |           | https://hothgq-presto-dhd.gq.vcg.yahoo.com:4443       |               |
|      |             |           |                                                       |               |
|      |             |           | jdbc:presto://hothgq-presto-dhd.gq.vcg.yahoo.com:4443 |               |
+------+-------------+-----------+-------------------------------------------------------+---------------+
| gq2  | production  | LGQ       | From VMG:                                             | kessel        |
|      |             |           |                                                       |               |
|      |             |           | https://legogq-presto.gq.vcg.yahoo.com:4443           | polaris       |
|      |             |           |                                                       |               |
|      |             |           | jdbc:presto://legogq-presto.gq.vcg.yahoo.com:4443     |               |
|      |             |           |                                                       |               |
|      |             |           | From Verizon:                                         |               |
|      |             |           |                                                       |               |
|      |             |           | https://legogq-presto-dhd.gq.vcg.yahoo.com:4443       |               |
|      |             |           |                                                       |               |
|      |             |           | jdbc:presto://legogq-presto-dhd.gq.vcg.yahoo.com:4443 |               |
+------+-------------+-----------+-------------------------------------------------------+---------------+

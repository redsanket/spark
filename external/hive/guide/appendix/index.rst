========
Appendix
========

This chapter includes reference information to help you use Hive at Yahoo. 

.. toctree::
   :maxdepth: 2
   :hidden:

   ddl

.. _gs_appendix-grid_vips:

Grid VIP URLs/Ports
===================

The following table provides the URIs and ports to the Hive Server 2, HCatalog
as well as the Business Intelligence (BI) server information on each cluster.

For more information about the software installed on the different clusters,
see `Grid Versions <http://twiki.corp.yahoo.com/view/Grid/GridVersions>`_.


+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| **Grid** | **BI Server**      | **VIP Host**                                        | **Port** | **Principal**                                            | **HCatalog VIP**                                              | **Comments**       |
+==========+====================+=====================================================+==========+==========================================================+===============================================================+====================+
| AR       | Microstrategy/JDBC | ``axonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com``    | 50515    | ``hive/axonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com``    | ``thrift://axonitered-hcat.ygrid.vip.bf1.yahoo.com:50513``    | Hadoop 2.x sandbox |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``axonitered-hs2.ygrid.vip.bf1.yahoo.com``          | 50514    | ``hive/axonitered-hs2.ygrid.vip.bf1.yahoo.com``          |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| BR       | Microstrategy/JDBC | ``bassniumred-hs2-noenc.ygrid.vip.bf1.yahoo.com``   | 50515    | ``hive/bassniumred-hs2-noenc.ygrid.vip.bf1.yahoo.com``   | ``thrift://bassniumred-hcat.ygrid.vip.bf1.yahoo.com:50513``   |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``bassniumred-hs2.ygrid.vip.bf1.yahoo.com``         | 50514    | ``hive/bassniumred-hs2.ygrid.vip.bf1.yahoo.com``         |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| BT       | Microstrategy/JDBC | ``bassniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``   | 50515    | ``hive/bassniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``   | ``thrift://bassniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``   |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``bassniumtan-hs2.ygrid.vip.bf1.yahoo.com``         | 50514    | ``hive/bassniumtan-hs2.ygrid.vip.bf1.yahoo.com``         |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| CB       | Microstrategy/JDBC | ``cobaltblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``    | 50515    | ``hive/cobaltblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``    | ``thrift://cobaltblue-hcat.ygrid.vip.gq1.yahoo.com:50513``    |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``cobaltblue-hs2.ygrid.vip.gq1.yahoo.com``          | 50514    | ``hive/cobaltblue-hs2.ygrid.vip.gq1.yahoo.com``          |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| DB       | Microstrategy/JDBC | ``dilithiumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com`` | 50515    | ``hive/dilithiumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com`` | ``thrift://dilithiumblue-hcat.ygrid.vip.gq1.yahoo.com:50513`` |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com``       | 50514    | ``hive/dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com``       |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| DR       | Microstrategy/JDBC | ``dilithiumred-hs2-noenc.ygrid.vip.bf1.yahoo.com``  | 50515    | ``hive/dilithiumred-hs2-noenc.ygrid.vip.bf1.yahoo.com``  | ``thrift://dilithiumred-hcat.ygrid.vip.bf1.yahoo.com:50513``  |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``dilithiumred-hs2.ygrid.vip.bf1.yahoo.com```       | 50514    | ``hive/dilithiumred-hs2.ygrid.vip.bf1.yahoo.com``        |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| KR       | Microstrategy/JDBC | ``kryptonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com`` | 50515    | ``hive/kryptonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com`` | ``thrift://kryptonitered-hcat.ygrid.vip.bf1.yahoo.com:50513`` | Hadoop sandbox     |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``       | 50514    | ``hive/kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``       |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| MB       | Microstrategy/JDBC | ``mithrilblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``   | 50515    | ``hive/mithrilblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``   | ``thrift://mithrilblue-hcat.ygrid.vip.gq1.yahoo.com:50513``   |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``mithrilblue-hs2.ygrid.vip.gq1.yahoo.com``         | 50514    | ``hive/mithrilblue-hs2.ygrid.vip.gq1.yahoo.com``         |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| MR       | Microstrategy/JDBC | ``mithrilred-hs2-noenc.ygrid.vip.bf1.yahoo.com``    | 50515    | ``hive/mithrilred-hs2-noenc.ygrid.vip.bf1.yahoo.com``    | ``thrift://mithrilred-hcat.ygrid.vip.bf1.yahoo.com:50513``    |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``mithrilred-hs2.ygrid.vip.bf1.yahoo.com``          | 50514    | ``hive/mithrilred-hs2.ygrid.vip.bf1.yahoo.com``          |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| NB       | Microstrategy/JDBC | ``nitroblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``     | 50515    | ``hive/nitroblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``     | ``thrift://nitroblue-hcat.ygrid.vip.gq1.yahoo.com:50513``     |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``nitroblue-hs2.ygrid.vip.gq1.yahoo.com``           | 50514    | ``hive/nitroblue-hs2.ygrid.vip.gq1.yahoo.com``           |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| OB       | Microstrategy/JDBC | ``oxiumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``     | 50515    | ``hive/oxiumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``     | ``thrift://oxiumblue-hcat.ygrid.vip.gq1.yahoo.com:50513``     |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``oxiumblue-hs2.ygrid.vip.gq1.yahoo.com``           | 50514    | ``hive/oxiumblue-hs2.ygrid.vip.gq1.yahoo.com``           |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| PT       | Microstrategy/JDBC | ``phazontan-hs2-noenc.ygrid.vip.ne1.yahoo.com``     | 50515    | ``hive/phazontan-hs2-noenc.ygrid.vip.ne1.yahoo.com``     | ``thrift://phazontan-hcat.ygrid.vip.ne1.yahoo.com:50513``     |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``phazontan-hs2.ygrid.vip.ne1.yahoo.com``           | 50514    | ``hive/phazontan-hs2.ygrid.vip.ne1.yahoo.com``           |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| TT       | Microstrategy/JDBC | ``tiberiumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``   | 50515    | ``hive/tiberiumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``   | ``thrift://tiberiumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``   |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``tiberiumtan-hs2.ygrid.vip.ne1.yahoo.com``         | 50514    | ``hive/tiberiumtan-hs2.ygrid.vip.ne1.yahoo.com``         |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| UB       | Microstrategy/JDBC | ``uraniumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``   | 50515    | ``hive/uraniumblue-hs2-noenc.ygrid.vip.gq1.yahoo.com``   | ``thrift://uraniumblue-hcat.ygrid.vip.gq1.yahoo.com:50513``   |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``uraniumblue-hs2.ygrid.vip.gq1.yahoo.com``         | 50514    | ``hive/raniumblue-hs2.ygrid.vip.gq1.yahoo.com``          |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| UT       | Microstrategy/JDBC | ``uraniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``    | 50515    | ``hive/uraniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``    | ``thrift://uraniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``    |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``uraniumtan-hs2.ygrid.vip.ne1.yahoo.com``          | 50514    | ``hive/uraniumtan-hs2.ygrid.vip.ne1.yahoo.com``          |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+
| ZT       | Microstrategy/JDBC | ``zaniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``     | 50515    | ``hive/zaniumtan-hs2-noenc.ygrid.vip.ne1.yahoo.com``     | ``thrift://zaniumtan-hcat.ygrid.vip.ne1.yahoo.com:50513``     |                    |
+          +--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+                                                               +                    +
|          | Tableau            | ``zaniumtan-hs2.ygrid.vip.ne1.yahoo.com``           | 50514    | ``hive/zaniumtan-hs2.ygrid.vip.ne1.yahoo.com``           |                                                               |                    |
+----------+--------------------+-----------------------------------------------------+----------+----------------------------------------------------------+---------------------------------------------------------------+--------------------+



.. _gs_appendix-space_quotes:

Space Quotas for Projects
=========================


Grids running Hadoop 0.20.2xx (aka Fred) have Capacity Scheduler Limits:

- HDFS namespace (i.e., the number of files) that are in **Pending + Running** will be set to a maximum of
  - Per Job: 100k
  - Per User: 100k
  - Per Queue: 200k

  .. note:: Jobs with more than 100k files need to be split into 100k chunks.
- Top-level HDFS directory's file space will be set to 4TB.
- New *project* directories will be set to 4TB, unless the ticket requests a higher allocation and is approved by the Business Unit Grid POC.
- NFS-mounted ``home`` directories will be assigned 5GB.

For more information, see `HDFS Quotas Guide <http://twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fhadoop.apache.org%2Fcore%2Fdocs%2Fr0.20.0%2F%2Fhdfs_quota_admin_guide.html&SIG=12bpd0f84>`_.



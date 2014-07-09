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

+-----------+-------------------------------------------------+------------+-----------------+
| **Grid**  | **VIP**                                         | **Port**   | **Comments**    |
+===========+=================================================+============+=================+
| BR        | ``bassniumred-hs2.ygrid.vip.bf1.yahoo.com``     | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| BT        | ``bassniumtan-hs2.ygrid.vip.ne1.yahoo.com``     | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| CB        | ``cobaltblue-hs2.ygrid.vip.gq1.yahoo.com``      | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| DB        | ``dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com``   | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| DR        | ``dilithiumred-hs2.ygrid.vip.bf1.yahoo.com``    | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| KR        | ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``   | 50514      | Sandbox cluster |
+-----------+-------------------------------------------------+------------+-----------------+
| MB        | ``mithrilblue-hs2.ygrid.vip.gq1.yahoo.com``     | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| MR        | ``mithrilred-hs2.ygrid.vip.bf1.yahoo.com``      | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| NB        | ``nitroblue-hs2.ygrid.vip.gq1.yahoo.com``       | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| OB        | ``oxiumblue-hs2.ygrid.vip.gq1.yahoo.com``       | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| PT        | ``phazontan-hs2.ygrid.vip.ne1.yahoo.com``       | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| TT        | ``tiberiumtan-hs2.ygrid.vip.ne1.yahoo.com``     | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| UB        | ``uraniumblue-hs2.ygrid.vip.gq1.yahoo.com``     | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| UT        | ``uraniumtan-hs2.ygrid.vip.ne1.yahoo.com``      | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+
| ZT        | ``zaniumtan-hs2.ygrid.vip.ne1.yahoo.com``       | 50514      |                 |
+-----------+-------------------------------------------------+------------+-----------------+

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



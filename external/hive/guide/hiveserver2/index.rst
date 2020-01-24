HiveServer2
###########

.. toctree::
   :maxdepth: 2
   :hidden:

   tableau
   looker
   beeline
   jdbc
   dbvis

:ref:`HiveServer2 (HS2) <hive_intro-HiveServer2>` enables JDBC/ODBC clients to run HiveQL queries.

**Connectivity:**

There are multiple ways and tools to connect to a Hive server 2 and run queries.
Below is the supported set of tools and classification based on whether they are hosted by
the Grid team or to be installed by the users.
Click on the individual tool to see details about connectivity through that tool.

+----------------------------------+--------+---------------+----------+
| Tool                             | Type   | Hosting model | Location |
+==================================+========+===============+==========+
| CLI                              | Client | hosted        | Gateway  |
+----------------------------------+--------+---------------+----------+
|                                  |        | self-hosting  | Launcher |
+----------------------------------+--------+---------------+----------+
| Hue                              | Server | hosted        |          |
+----------------------------------+--------+---------------+----------+
| :doc:`Looker <looker>`           | Server | hosted        |          |
+----------------------------------+--------+---------------+----------+
| Tableau Server                   | Server | self-hosting  |          |
+----------------------------------+--------+---------------+----------+
| :doc:`Tableau Desktop <tableau>` | Client | self-hosting  | Laptop   |
+----------------------------------+--------+---------------+----------+
| :doc:`Beeline <beeline>`         | Client | self-hosting  |          |
+----------------------------------+--------+---------------+----------+
| :doc:`Direct JDBC <jdbc>`        | Client | self-hosting  |          |
+----------------------------------+--------+---------------+----------+
| :doc:`DbVisualizer <dbvis>`      | Client | self-hosting  |          |
+----------------------------------+--------+---------------+----------+


.. _hiveserver2_servers:

HiveServer2 Servers
*******************

The following table provides the URIs and ports for connecting to the HiveServer2 instances for each Hadoop cluster.
These instances only support Thrift/HTTPS. Thrift/SASL is disabled as it does not meet our encryption strength standards.

+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| **Grid** | **Hostname**                               | **Port** | **Kerberos Principal**                                          | **Type**       |
+==========+============================================+==========+=================================================================+================+
| AR       | ``axonitered-hs2.red.ygrid.yahoo.com``     | 4443     | ``hive/axonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``     | Sandbox        |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| BB       | ``bassniumblue-hs2.blue.ygrid.yahoo.com``  | 4443     | ``hive/bassniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``  | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| BR       | ``bassnniumred-hs2.red.ygrid.yahoo.com``   | 4443     | ``hive/bassnniumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``   | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| BT       | ``bassniumtan-hs2.tan.ygrid.yahoo.com``    | 4443     | ``hive/bassniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``    | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| DB       | ``dilithiumblue-hs2.blue.ygrid.yahoo.com`` | 4443     | ``hive/dilithiumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM`` | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| DR       | ``dilithiumred-hs2.red.ygrid.yahoo.com``   | 4443     | ``hive/dilithiumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``   | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| JB       | ``jetblue-hs2.blue.ygrid.yahoo.com``       | 4443     | ``hive/jetblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``       | Research       |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| KR       | ``kryptonitered-hs2.red.ygrid.yahoo.com``  | 4443     | ``hive/kryptonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``  | Sandbox        |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| MR       | ``mithrilred-hs2.red.ygrid.yahoo.com``     | 4443     | ``hive/mithrilred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``     | Research       |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| OB       | ``oxiumblue-hs2.blue.ygrid.yahoo.com``     | 4443     | ``hive/oxiumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``     | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| PB       | ``phazonblue-hs2.blue.ygrid.yahoo.com``    | 4443     | ``hive/phazonblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``    | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| PR       | ``phazonred-hs2.red.ygrid.yahoo.com``      | 4443     | ``hive/phazonred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``      | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| PT       | ``phazontan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/phazontan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``      | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| TT       | ``tiberiumtan-hs2.tan.ygrid.yahoo.com``    | 4443     | ``hive/tiberiumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``    | Research       |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| UB       | ``uraniumblue-hs2.blue.ygrid.yahoo.com``   | 4443     | ``hive/uraniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``   | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| UT       | ``uraniumtan-hs2.tan.ygrid.yahoo.com``     | 4443     | ``hive/uraniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``     | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| ZT       | ``zaniumtan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/zaniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``      | Production     |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| KGQ      | ``kesselgq-hs.gq.vcg.yahoo.com``           | 4443     | ``HTTP/kesselgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``           | VCG Sandbox    |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+
| PGQ      | ``polarisgq-hs.gq.vcg.yahoo.com``          | 4443     | ``HTTP/polarisgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``          | VCG Production |
+----------+--------------------------------------------+----------+-----------------------------------------------------------------+----------------+

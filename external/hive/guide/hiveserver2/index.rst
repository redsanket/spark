============
HiveServer2
============

Hiveserver2 (HS2) enables to clients to fetch data from HDFS by executing queries against Hive.
It is successor to HiveServer1 which is deprecated. Details can be found at :ref:`HiveServer2 <hive_intro-HiveServer2>`.

The following table provides the URIs and ports to the Hive Server 2 on each cluster.

HiveServer2 Servers (Thrift/HTTPS)
-----------------------------------
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| **Grid** | **Hostname**                                 | **Port** | **Kerberos Principal**                                               | **Comments**       |
+==========+==============================================+==========+======================================================================+====================+
| AR       | ``axonitered-hs2.red.ygrid.yahoo.com``       | 4443     | ``hive/axonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``          | Hadoop 2.x sandbox |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BB       | ``bassniumblue-hs2.blue.ygrid.yahoo.com``    | 4443     | ``hive/bassniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``       |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BR       | ``bassnniumred-hs2.red.ygrid.yahoo.com``     | 4443     | ``bassnniumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``             |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| BT       | ``bassniumtan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/bassniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DB       | ``dilithiumblue-hs2.blue.ygrid.yahoo.com``   | 4443     | ``hive/dilithiumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``      |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| DR       | ``dilithiumred-hs2.red.ygrid.yahoo.com``     | 4443     | ``hive/dilithiumred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``        |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| JB       | ``jetblue-hs2.blue.ygrid.yahoo.com``         | 4443     | ``hive/jetblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``            |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| KR       | ``kryptonitered-hs2.red.ygrid.yahoo.com``    | 4443     | ``hive/kryptonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``       | Hadoop sandbox     |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| MR       | ``mithrilred-hs2.red.ygrid.yahoo.com``       | 4443     | ``hive/mithrilred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| OB       | ``oxiumblue-hs2.blue.ygrid.yahoo.com``       | 4443     | ``hive/oxiumblue-hs2.blue.ygrid.yahoo.com``                          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PB       | ``phazonblue-hs2.blue.ygrid.yahoo.com``      | 4443     | ``hive/phazonblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PR       | ``phazonred-hs2.red.ygrid.yahoo.com``        | 4443     | ``hive/phazonred-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| PT       | ``phazontan-hs2.tan.ygrid.yahoo.com``        | 4443     | ``hive/phazontan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| TT       | ``tiberiumtan-hs2.tan.ygrid.yahoo.com``      | 4443     | ``hive/tiberiumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``         |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UB       | ``uraniumblue-hs2.blue.ygrid.yahoo.com``     | 4443     | ``hive/uraniumblue-hs2.blue.ygrid.yahoo.com@YGRID.YAHOO.COM``        |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| UT       | ``uraniumtan-hs2.tan.ygrid.yahoo.com``       | 4443     | ``hive/uraniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``          |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| ZT       | ``zaniumtan-hs2.tan.ygrid.yahoo.com``        | 4443     | ``hive/zaniumtan-hs2.tan.ygrid.yahoo.com@YGRID.YAHOO.COM``           |                    |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Kessel   | ``kesselgq-hs.gq.vcg.yahoo.com``             | 4443      | ``HTTP/kesselgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``               | VCG                |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+
| Polaris  | ``polarisgq-hs.gq.vcg.yahoo.com``            | 4443     | ``HTTP/polarisgq-hs.gq.vcg.yahoo.com@VCG.OUROATH.COM``               | VCG                |
+----------+----------------------------------------------+----------+----------------------------------------------------------------------+--------------------+


Connectivity
------------

There are multiple ways and tools to connect to a Hive server 2 and run queries.
Below is the supported set of tools and classification based on whether they are hosted by
the Grid team or to be installed by the users.
Click on the individual tool to see details about connectivity through that tool.

+---------------------------------------------------+--------+------------------+----------+
| Tool                                              | Type   | Hosting model    | Location |
+===================================================+========+==================+==========+
| CLI                                               | Client | hosted           | Gateway  |
+---------------------------------------------------+--------+------------------+----------+
|                                                   |        | self-hosting     | Launcher |
+---------------------------------------------------+--------+------------------+----------+
| Hue                                               | Server | hosted           |          |
+---------------------------------------------------+--------+------------------+----------+
|                                                   |        | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| Looker                                            | Server | hosted           |          |
+---------------------------------------------------+--------+------------------+----------+
| Tableau Server                                    | Server | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| :ref:`Tableau Desktop <Tableau_Desktop_Connectivity>`| Client | self-hosting  | Laptop   |
+---------------------------------------------------+--------+------------------+----------+
| Direct JDBC                                       | Client | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| Python Client                                     | Client | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
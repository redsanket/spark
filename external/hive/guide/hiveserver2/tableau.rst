Tableau Desktop
###############

.. _Tableau_Desktop_Connectivity:

This document contains instructions to connect to HiveSever2 hosted on **YGRID** from Tableau Desktop.

macOS
*****

You can either connect using `Hortonworks Hadoop Hive Tableau Connector <https://help.tableau.com/current/pro/desktop/en-us/examples_hortonworkshadoop.htm>`_
or the `Other ODBC Connector <https://help.tableau.com/current/pro/desktop/en-us/odbc_tableau.htm>`_ using the Simba Apache Hive ODBC Driver.

For both the connectors, there is
   - a onetime setup to install Athenz CLI utilities used to fetch certificates for authentication.
   - a daily setup step to fetch the role certificates.

This setup is same for Presto as well and so it is required to do only once for either Presto or HiveServer2.

Onetime Setup
=============

- Download the Athenz utilities following the steps in `macOS Onetime Setup <https://git.ouroath.com/pages/hadoop/docs/presto/authentication.html#mac-onetime>`_.


Daily Setup
===========

- Download and run the script to fetch Athenz role certificates following steps in `macOS Daily Setup <https://git.ouroath.com/pages/hadoop/docs/presto/authentication.html#mac-daily>`_.



Hortonworks Hadoop Hive Connector
=================================

The Hortonworks Hive ODBC driver is just a rebranded driver of Simba Apache Hive ODBC Driver but
natively supported in Tableau. The issue is that the Tableau UI does not have options for mTLS
authentication. With Y.CORP.YAHOO.COM being decommissioned and `pkinit <http://yo/pkinit>`_ not
being support in macOS, mTLS with Athenz role certificates is the only option supported. We can still
get it working with the Hortonworks Hadoop Hive Connector with some addition configuration.

Steps:

1. Edit the ``/Library/hortonworks/hive/lib/universal/hortonworks.hiveodbc.ini`` file to add the following.
   Replace all occurrences of ``<username>`` with your username.

   .. code-block:: text

      [Driver]
      ErrorMessagesPath=/Library/hortonworks/hive/ErrorMessages/
      LogLevel=0
      LogPath=
      SwapFilePath=/tmp
      # Add below settings to make mTLS connection default for
      # Tableau Hortonworks Hadoop Hive Connection
      # Replace <username> with your name
      HiveServerType = 2
      ThriftTransport = 2
      SSL = 1
      TwoWaySSL = 1
      AuthMech = 0
      HTTPPath = cliservice
      CAIssuedCertNamesMismatch = 0
      ClientCert = /Users/<username>/.athenz/griduser.uid.<username>.cert.pem
      ClientPrivateKey = /Users/<username>/.athenz/griduser.uid.<username>.key.pem

2. Create a new connection similar to the following example. Please do replace
   ``jetblue-hs2.blue.ygrid.yahoo.com`` with the HiveServer2 instance you want to
   connect to from the :ref:`list of HiveServer2 servers <hiveserver2_urls>`.

   .. code-block:: text

      Server :           jetblue-hs2.blue.ygrid.yahoo.com
      Port   :           4443
      Authentication :   No Authentication
      Transport   :      HTTP
      HTTP Path   :      cliservice
      Require SSL        [✓]  (Select the checkbox)

   .. image:: images/tableau_hortonworkshive_new_connection.png
     :height: 350px
     :width: 400px
     :scale: 100%
     :alt:
     :align: left

|
|
|
|
|
|
|
|
|
|
|
|
|
|
|

Even though the mTLS options are not specified in the Tableau Connection dialog, they are
picked from ``/Library/hortonworks/hive/lib/universal/hortonworks.hiveodbc.ini``.

Migrating from Kerberos
-----------------------

You might already have existing workbooks configured with Kerberos Authentication. To migrate those workbooks
you can either open the workbook and edit the connection details or modify the workbook file directly if it is
in the ``.twb`` xml format instead of ``.twbx`` binary format.

Edit Connection
^^^^^^^^^^^^^^^

1. Open the workbook and in the ``Datasource`` tab, from the list of ``Connections``
   click on ``Edit Connection`` for the connection to be modified .

  .. image:: images/tableau_hortonworkshive_edit_connection.png
     :height: 350px
     :width: 400px
     :scale: 100%
     :alt:
     :align: left

|
|
|
|
|
|
|
|
|
|
|
|
|
|
|

2. Modify all the fields to be similar to a new connection. Remember to change
   ``Server`` and ``Port`` as well as they are different from the Thrift/SASL Kerberos authentication
   we had before.

   .. code-block:: text

      Server :           jetblue-hs2.blue.ygrid.yahoo.com
      Port   :           4443
      Authentication :   No Authentication
      Transport   :      HTTP
      HTTP Path   :      cliservice
      Require SSL        [✓]  (Select the checkbox)

   .. image:: images/tableau_hortonworkshive_new_connection.png
     :height: 300px
     :width: 400px
     :scale: 100%
     :alt:
     :align: left

|
|
|
|
|
|
|
|
|
|
|
|
|

Repeat this for all the connections still using Kerberos and older HiveServer2 servers.

Modify Workbook File
^^^^^^^^^^^^^^^^^^^^

If you have saved your workbook in the ``.twb`` format, it can be directly edited instead.

Here is an example with old value and changed new values for the ``<connection>`` section in the file.

**Kerberos authentication:**

.. code-block:: text

   <connection authentication='yes' authentication-type='1' class='hortonworkshadoophive'
   connection-type='2' dbname=''
   kerberos-host='jetblue-hs2.ygrid.vip.gq1.yahoo.com' kerberos-realm='YGRID.YAHOO.COM' kerberos-service='hive'
   odbc-connect-string-extras='' one-time-sql=''
   port='50514' schema='benzene' server='jetblue-hs2.ygrid.vip.gq1.yahoo.com'
   sslcert='' sslmode='' transport-type='1' username=''>

**mTLS authentication:**

.. code-block:: text

   <connection authentication='no' authentication-type='0' class='hortonworkshadoophive'
   connection-type='2' dbname=''
   http-path='cliservice'
   odbc-connect-string-extras='' one-time-sql=''
   port='4443' schema='benzene' server='jetblue-hs2.ygrid.vip.gq1.yahoo.com'
   sslcert='' sslmode='require' transport-type='2' username=''>

+----------------------+-------------------------------------------+----------------------------------------+
| Attribute name       | Old Value                                 | New Value                              |
+======================+===========================================+========================================+
| kerberos-host        | 'jetblue-hs2.ygrid.vip.gq1.yahoo.com'     |                                        |
+----------------------+-------------------------------------------+----------------------------------------+
| kerberos-realm       | YGRID.YAHOO.COM                           |                                        |
+----------------------+-------------------------------------------+----------------------------------------+
| kerberos-service     | hive                                      |                                        |
+----------------------+-------------------------------------------+----------------------------------------+
| authentication-realm | yes                                       | no                                     |
+----------------------+-------------------------------------------+----------------------------------------+
| authentication-type  | 1                                         | 0                                      |
+----------------------+-------------------------------------------+----------------------------------------+
| server               | dilithiumblue-hs2.ygrid.vip.gq1.yahoo.com | dilithiumblue-hs2.blue.ygrid.yahoo.com |
+----------------------+-------------------------------------------+----------------------------------------+
| port                 | 50514 or 50515                            | 4443                                   |
+----------------------+-------------------------------------------+----------------------------------------+
| sslmode              |                                           | require                                |
+----------------------+-------------------------------------------+----------------------------------------+
| transport-type       | 1                                         | 2                                      |
+----------------------+-------------------------------------------+----------------------------------------+

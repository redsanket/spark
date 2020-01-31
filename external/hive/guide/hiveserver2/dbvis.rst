DbVisualizer
############

DbVisualizer is a client side database management and analysis tool that can be used to
run Hive queries using its JDBC driver.

*Note:* DBVisualizer is popular among some users for non-production adhoc usecases
and this document is to make it easier for them. But do note that the
Hive team does not provide production support for DbVisualizer as a BI Tool
unlike Tableau, Looker or Superset. We support it just as another JDBC client to Hive
and will only help with Hive JDBC driver specific issues.

Download
********

**Free Edition:**

You can `download <https://www.dbvis.com/download/>`_ the free version from their site
which is good enough for simple use.

**Professional Edition:**

You can also request for DBVisualizer Professional Edition from GSD if you need it.

  1. Go to `yo/gsd <http://yo/gsd>`_
  2. Click on ``Request Software`` which will take you to another page in `Street <https://thestreet.vzbuilders.com/thestreet/software-requests>`_.
  3. Click on ``Other Software`` under ``Desktop Software``. It will take you to a ServiceNow request form.
  4. Select ``DBVisualizer`` from the dropdown for ``Please select the software`` field.
  5. Fill the rest of the fields and submit the request.

macOS
*****

Onetime Setup
=============

.. _dbvis_hive_jar_setup:

1. Download the latest Hive standalone jdbc jar to ``${HOME}/.dbvis`` directory.

.. code-block:: text

  rm -f ~/.dbvis/jdbc/hive-jdbc*.jar && curl -v -o ~/.dbvis/jdbc/hive-jdbc-standalone.jar "https://artifactory.ouroath.com/artifactory/maven-release/org/apache/hive/hive-jdbc/\[RELEASE\]/hive-jdbc-\[RELEASE\]-standalone.jar"

2. Download the Athenz utilities following the steps in `macOS Onetime Setup <https://git.ouroath.com/pages/hadoop/docs/presto/authentication.html#mac-onetime>`_.


Daily Setup
===========

1. Download and run the script to fetch Athenz role certificates following steps in `macOS Daily Setup <https://git.ouroath.com/pages/hadoop/docs/presto/authentication.html#mac-daily>`_.
   This script has to be run once daily before connecting to HiveServer2 or Presto.

New Connection
==============

You can create a new connection, by either running the provided script or by directly adding it in the UI.

Script
------

1. Close the DBVisualizer.
2. Please :download:`download <files/macOS_hive_dbvis_add_connection.py>`
   the script that automates adding connection. It does it by modifying ``${HOME}/.dbvis/config70/dbvis.xml`` directly.
   It also creates a backup of the file before running in the same directory and if you run into any issues,
   you can revert to the backed up older copy of ``dbvis.xml``.
3. After downloading, ``cd`` to the directory you downloaded the script to,
   give it execute permissions and then invoke it. The arguments to the script are:

   - short name of the HiveServer2 cluster
   - name of the schema (hive database name). This is optional

   You can refer to :ref:`list of HiveServer2 instances <hiveserver2_servers>` for the valid values for cluster and catalog.
   Below example will create a new connection to ``Xandar Blue`` cluster which has the default catalog and database as
   ``dilithiumblue`` and ``benzene`` respectively.

.. code-block:: text

  python macOS_hive_dbvis_add_connection.py JB benzene


4. Restart the DBVisualizer after running the script.


UI
--

1. Restart the DBVisualizer, if you have not already done after downloading the Hive JDBC jar.
2. Click on ``Create new database connection`` in the ``Databases`` tool bar on the left.

  .. image:: images/dbvis_new_connection.png
     :height: 200px
     :width: 200px
     :scale: 80%
     :alt:
     :align: left

|
|
|
|
|
|
|

3. Select ``No Wizard`` in the ``Use Connection Wizard?`` dialog.

4. In the ``Connection Tab``, change the ``Settings Format`` field under ``Database`` from ``Server Info`` to ``Database URL``.
   That will replace ``Database Server``, ``Database Port`` and ``Database`` fields with
   a single ``Database URL`` field.

5. Configure the following connection details in the ``Connection`` tab. In the example below
   please be sure to

   - replace ``jetblue-hs2.blue.ygrid.yahoo.com`` with the HiveServer2 instance
     you want to connect to from the :ref:`list of HiveServer2 instances <hiveserver2_servers>`.
   - replace all occurrences of ``<username>`` with your username.
   - replace ``default`` in ``tez.queue.name=default`` with the Hadoop queue name, if you want to run the queries in a different queue.

   .. code-block:: text

      Database Type      Hive
      Driver (JDBC)      Hive
      Database URL       jdbc:hive2://jetblue-hs2.blue.ygrid.yahoo.com:4443/;transportMode=http;httpPath=cliservice;ssl=true;sslTrustStore=/Users/<username>/.athenz/yahoo_certificate_bundle.jks;twoWay=true;sslKeyStore=/Users/<username>/.athenz/griduser.role.uid.<username>.jks;keyStorePassword=changeit?tez.queue.name=default

   .. image:: images/dbvis_configure_connection.png
     :height: 516px
     :width: 1000px
     :scale: 80%
     :alt:
     :align: left
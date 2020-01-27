DbVisualizer
############

DbVisualizer is a client side database management and analysis tool that can be used to
run Presto queries using its JDBC driver.

*Note:* DBVisualizer is popular among some users for non-production adhoc usecases
and this document is to make it easier for them. But do note that the
Presto team does not provide production support for DbVisualizer as a BI Tool
unlike Tableau, Looker or Superset. We support it just as another JDBC client to Presto
and will only help with Presto JDBC driver specific issues.

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

.. _dbvis_presto_jar_setup:

1. Download the latest Presto jdbc jar to ``${HOME}/.dbvis`` directory.

.. code-block:: text

  rm -f ~/.dbvis/jdbc/presto-jdbc*.jar && curl -v -o ~/.dbvis/jdbc/presto-jdbc.jar "https://artifactory.ouroath.com/artifactory/maven-release/com/facebook/presto/presto-jdbc/\[RELEASE\]/presto-jdbc-\[RELEASE\].jar"

2. Download the Athenz utilities following the steps in :ref:`macOS Onetime Setup <mac_onetime>`.


Daily Setup
===========

1. Download and run the script to fetch Athenz role certificates following steps in :ref:`macOS Daily Setup <mac_daily>`.


New Connection
==============

You can create a new connection, by either running the provided script or by directly adding it in the UI.

Script
------

1. Close the DBVisualizer.
2. Please :download:`download <files/macOS_presto_dbvis_add_connection.py>`
   the script that automates adding connection. It does it by modifying ``${HOME}/.dbvis/config70/dbvis.xml`` directly.
   It also creates a backup of the file before running in the same directory and if you run into any issues,
   you can revert to the backed up older copy of ``dbvis.xml``.
3. After downloading, ``cd`` to the directory you downloaded the script to,
   give it execute permissions and then invoke it. The arguments to the script are:

   - short name of the Presto cluster
   - name of the catalog
   - name of the schema (hive database name). This is optional

   You can refer to :ref:`list of clusters and catalogs <ygrid_presto_clusters>` for the valid values for cluster and catalog.
   Below example will create a new connection to ``Xandar Blue`` cluster which has the default catalog and database as
   ``dilithiumblue`` and ``benzene`` respectively.

.. code-block:: text

  python macOS_presto_dbvis_add_connection.py XB dilithiumblue benzene


4. Restart the DBVisualizer after running the script.


UI
--

1. Restart the DBVisualizer, if you have not already done after downloading the Presto JDBC jar.
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

4. Configure the connection details for the Presto Driver. In the example below
   please be sure to replace ``xandarblue-presto.blue.ygrid.yahoo.com`` and
   ``dilithiumblue`` with the Presto cluster and Hive catalog you want to connect to
   from the :ref:`list of clusters and catalogs <ygrid_presto_clusters>`. Replace
   ``<databasename>`` with the name of the Hive database and ``username`` with your
   Okta short id.

   .. code-block:: text

      Database Type      Presto
      Driver (JDBC)      Presto
      Database Server    xandarblue-presto.blue.ygrid.yahoo.com
      Database Port      4443
      Catalog            dilithiumblue
      Schema             <databasename>
      Database Userid    <username>

   .. image:: images/dbvis_configure_connection.png
     :height: 516px
     :width: 700px
     :scale: 100%
     :alt:
     :align: left


5. Configure the connection properties by clicking on the ``+`` sign and adding the below
   properties one by one. Replace ``username`` with your Okta short id.
   Click on ``Apply`` after adding all the properties.

   .. code-block:: text

    SSL=true
    SSLCertificatePath=/Users/<username>/.athenz/griduser.uid.<username>.cert.pem
    SSLKeyStorePath=/Users/<username>/.athenz/griduser.uid.<username>.key.pem
    SSLTrustStorePath=/Users/<username>/.athenz/yahoo_certificate_bundle.pem
    SessionProperties=query_max_execution_time=15m

  You can set the ``SessionProperties`` to ``query_max_execution_time=15m;distributed_join=false`` for better
  performance if all your join queries join with small dimension tables (<100MB) with the smaller table on the right side.
  If you have join between two large tables, do not use that option as it will cause high memory overload
  and can even take out the workers.

  .. image:: images/dbvis_configure_connection_properties.png
     :height: 516px
     :width: 836px
     :scale: 80%
     :alt:
     :align: left


  If you run into ``Unrecognized connection property`` error, ensure there is no typo
  or leading/trailing whitespaces in the property name and that you have downloaded
  the :ref:`latest Presto jdbc driver`.


Looker
######

Prerequisites
*************
1. You already have a Looker setup with TLS (https).
2. Looker is configured with `SAML Authentication <https://docs.looker.com/admin-options/security/saml-auth>`_ and OKTA as Identity Provider.

Setup
*****

Custom JDBC Driver
==================

Looker comes with a built-in JDBC driver for Presto. But we prefer that the internal
released Presto JDBC driver is used instead as it has support for ``SessionProperties`` and
performance enhancements.

Steps to configure custom JDBC driver:

1. Add ``LOOKERARGS="--use-custom-jdbc-config"`` in ``lookerstart.cfg``
2. Create file ``custom_jdbc_config.yml`` in the Looker installation directory with the following content.

.. code-block:: text

  - name: presto
    file_name: presto-jdbc.jar
    module_path: com.facebook.presto.jdbc.PrestoDriver

3. Create new directory ``custom_jdbc_drivers`` in looker directory.
4. Execute the following to get the latest JDBC driver and put it in the ``custom_jdbc_drivers`` directory.

.. code-block:: text

  yinst install presto_client -br stable
  ln -s /home/y/libexec/presto_client/lib/presto-jdbc.jar /path_to_looker_installation/custom_jdbc_drivers/presto-jdbc.jar

Or if the node does not have ``yinst``, you can download the presto-jdbc jar from
ymaven repository (http://ymaven.corp.yahoo.com:9999/proximity/repository/public/yahoo/yinst/presto_client/presto-jdbc/<internal presto version>/presto-jdbc-<internal presto version>.jar)
or from artifactory repository (https://artifactory.ouroath.com/artifactory/maven-release/com/facebook/presto/presto-jdbc/<internal presto version>/)
and copy to the ``custom_jdbc_drivers`` directory. You can find the stable internal
presto version from `dist <https://dist.corp.yahoo.com/by-package/presto_client/>`_.


JDBC Connection
===============

The Looker to Presto authentication will be through mutual TLS authentication with Athenz role certificates.

Steps to configure a new Presto connection:

1) Follow instructions in :ref:`X.509 certificate authentication <x509_auth>` to setup ``griduser.uid.<headless username>`` Athenz role and configure ``hca`` to fetch and renew the role certificates.
2) Verify that the permission of the key file ``/var/lib/sia/certs/griduser.role.uid.<headless username>.key.pem`` is set to ``400`` and the file is only readable by the headless user.
3) Go to ``https://<looker server host name>:4443/admin/connections/new``
4) Enter the details as per the below example.

The example connects to ``xandarblue`` Presto cluster and accesses the
``dilithiumblue`` hive catalog and assumes the headless username to be ``p_search``.

Please replace with your headless user details.

  .. image:: images/looker_new_connection.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

.. code-block:: text

  Name: Presto-XandarBlue-DilithiumBlue
  Dialect: PrestoDB
  Host:Port: xandarblue-presto.blue.ygrid.yahoo.com 4443
  Database: dilithiumblue
  Username: p_search
  Schema: <Enter name of hive database here>
  Additional Params: SSLCertificatePath=/var/lib/sia/certs/griduser.role.uid.p_search.cert.pem&SSLKeyStorePath=/var/lib/sia/certs/griduser.role.uid.p_search.key.pem&SSLTrustStorePath=/home/y/share/ssl/certs/yahoo_certificate_bundle.pem&SessionProperties=distributed_join=false,query_max_run_time=15m
  SSL: true
  Verify SSL Cert: true
  Database TimeZone: UTC
  Query TimeZone: UTC


The value ``SessionProperties=distributed_join=false,query_max_run_time=15m`` in
additional parameters is optional.

  - ``distributed_join=false`` is to improve performance if all joins are with dimension tables that fit in memory. It should not be set if you join two fact tables or if your dimension tables are larger than 300MB before compression.
  - ``query_max_execution_time=15m`` fails a query if it does not finish with 15 minutes. This prevents users from running very large long-running queries which Presto is not intended for.


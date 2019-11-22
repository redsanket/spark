Command Line
############

The Presto CLI provides a terminal-based interactive shell for running SQL queries.
Run the CLI with the ``--help`` option to see the available options.

By default, the results of queries are paginated using the ``less`` program which is
configured with a carefully selected set of options. This behavior can be
overridden by setting the environment variable ``PRESTO_PAGER`` to the name of a
different program such as ``more``, or set it to an empty value to completely disable pagination.

Presto CLI is available through the
`presto_client <https://dist.corp.yahoo.com/by-package/presto_client/>`_ dist package.

Gateway
*******
The gateways of different Hadoop clusters will have Presto CLI installed similar
to hive CLI.

YGRID
=====
Currently ``presto_client`` package is installed only on JB and TT gateways and
you can use them to run queries against Xandar Blue and Xandar Tan. In the future,
each hadoop cluster gateway will have it installed, and the default hive catalog
will be set to that of the hadoop cluster.

To run queries:

1. Login to JB or TT gateway and run ``kinit`` as yourself or as a headless user you have sudo access to.
2. Invoke ``presto`` with options. For example:

.. code-block:: text

  ssh -A jet-gw.blue.ygrid.yahoo.com
  kinit rohinip@Y.CORP.YAHOO.COM
  presto --server https://xandarblue-presto.blue.ygrid.yahoo.com:4443 --catalog dilithiumblue --schema benzene
  show tables;

VCG
===
The presto_client package is installed on Kessel and Polaris gateways, and the
default catalog is set to ``kessel`` and ``polaris`` respectively.

To run queries:

1. Login to Kessel or Polaris gateway and run ``kinit`` as yourself or as a headless user you have sudo access to.
2. Invoke ``presto --schema databasename`` or just run ``presto`` and then do ``use databasename``

   For example:

.. code-block:: text

  ssh -A kessel-gw.gq.vcg.yahoo.com
  kinit rohinip@Y.CORP.YAHOO.COM
  
  (or)
  
  sudo -iu p_vcgheadlessuser
  kinit -kt /homes/p_vcgheadlessuser/p_vcgheadlessuser.prod.headless.keytab p_vcgheadlessuser
  presto --schema myvcgdb
  show tables;

Launcher
********
Follow the below steps to install ``presto_client`` in your Launcher and execute queries.

1. Request for :ref:`ACL <acl>` if ports are not open.
2. Install ``presto_client`` package
3. Configure the defaults to connect to: a) Presto Coordinator URL, b) Hive cluster name, and c) database name. For example:

.. code-block:: text

  yinst install presto_client -br current
  yinst set presto_client.coordinator_url=https://xandarblue-presto.blue.ygrid.yahoo.com:4443
  yinst set presto_client.catalog=jetblue
  yinst set presto_client.schema=benzene

4. Run ``kinit`` and invoke ``presto``. For example:

.. code-block:: text

  kinit rohinip@Y.CORP.YAHOO.COM
  presto
  show tables;

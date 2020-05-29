Connectivity
############

.. toctree::
   :maxdepth: 2
   :hidden:

   cli
   hue
   superset
   jdbc
   dbvis
   python_client
   looker
   tableau

There are multiple ways and tools to connect to a Presto server and run queries.
Below is the supported set of tools and classification based on whether they are hosted by
the Grid team or to be installed by the users.

+--------------------------------------+--------+------------------+----------+
| Tool                                 | Type   | Hosting model    | Location |
+======================================+========+==================+==========+
| :doc:`CLI <cli>`                     | Client | hosted           | Gateway  |
+--------------------------------------+--------+------------------+----------+
|                                      |        | self-hosting     | Launcher |
+--------------------------------------+--------+------------------+----------+
| :doc:`Hue <hue>`                     | Server | hosted           |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`Superset <superset>`           | Server | hosted (Q4 2020) |          |
+--------------------------------------+--------+------------------+----------+
|                                      |        | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`Looker <looker>`               | Server | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| Tableau Server                       | Server | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`Tableau Desktop <tableau>`     | Client | self-hosting     | Laptop   |
+--------------------------------------+--------+------------------+----------+
| :doc:`Direct JDBC <jdbc>`            | Client | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`DbVisualizer <dbvis>`          | Client | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`Python Client <python_client>` | Client | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+

Business Intelligence Tools
---------------------------

As the tool intended for business intelligence use cases, Presto has good connectivity to standard BI tools.

We have tested and currently support the following BI tools:
  1. Looker
  2. Tableau
  3. `Superset <https://superset.incubator.apache.org/>`_. Superset is an open source BI tool that we plan to host for users in the near future. For now, each user can deploy their own version and connect to the hosted Presto.

In addition, Presto supports JDBC connectivity so, theoretically, any BI tool that supports JDBC can connect to Presto. Complexity arises due to security considerations as well as making sure that Presto-friendly SQL is generated. Because of this, we recommend using one of the tools listed above.



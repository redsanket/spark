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
| :doc:`Superset <superset>`           | Server | hosted (Q2 2020) |          |
+--------------------------------------+--------+------------------+----------+
|                                      |        | self-hosting     |          |
+--------------------------------------+--------+------------------+----------+
| :doc:`Looker <looker>`               | Server | hosted           |          |
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


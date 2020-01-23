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

+---------------------------------------------------+--------+------------------+----------+
| Tool                                              | Type   | Hosting model    | Location |
+===================================================+========+==================+==========+
| :doc:`CLI <connectivity/cli>`                     | Client | hosted           | Gateway  |
+---------------------------------------------------+--------+------------------+----------+
|                                                   |        | self-hosting     | Launcher |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Hue <connectivity/hue>`                     | Server | hosted           |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Superset <connectivity/superset>`           | Server | hosted (Q2 2020) |          |
+---------------------------------------------------+--------+------------------+----------+
|                                                   |        | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Looker <connectivity/looker>`               | Server | hosted           |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Tableau Server <connectivity/tableau>`      | Server | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Tableau Desktop <connectivity/tableau>`     | Client | self-hosting     | Laptop   |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Direct JDBC <connectivity/jdbc>`            | Client | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`DbVisualizer <connectivity/dbvis>`          | Client | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+
| :doc:`Python Client <connectivity/python_client>` | Client | self-hosting     |          |
+---------------------------------------------------+--------+------------------+----------+


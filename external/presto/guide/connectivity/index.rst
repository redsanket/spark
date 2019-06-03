Connectivity
############

.. toctree::
   :maxdepth: 2
   :hidden:

   cli
   hue
   superset
   jdbc
   python_client
   looker
   tableau

There are multiple ways and tools to connect to a Presto server and run queries.
Below is the supported set of tools and classification based on whether they are hosted by
the Grid team or to be installed by the users.

+-----------------+--------+------------------+----------+
| Tool            | Type   | Hosting model    | Location |
+=================+========+==================+==========+
| CLI             | Client | hosted           | Gateway  |
+-----------------+--------+------------------+----------+
|                 |        | self-hosting     | Launcher |
+-----------------+--------+------------------+----------+
| Hue             | Server | hosted           |          |
+-----------------+--------+------------------+----------+
| Superset        | Server | hosted (Q3 2019) |          |
+-----------------+--------+------------------+----------+
|                 |        | self-hosting     |          |
+-----------------+--------+------------------+----------+
| Looker          | Server | hosted           |          |
+-----------------+--------+------------------+----------+
| Tableau Server  | Server | self-hosting     |          |
+-----------------+--------+------------------+----------+
| Tableau Desktop | Client | self-hosting     | Laptop   |
+-----------------+--------+------------------+----------+
| Direct JDBC     | Client | self-hosting     |          |
+-----------------+--------+------------------+----------+


Connectors
##########

.. toctree::
   :maxdepth: 2
   :hidden:

   hive
   mysql
   postgresql
   druid
   
Presto is built around the notion of connectors. Connectors allow Presto to
consume data from a variety of data sources. Though Presto supports a number
of `connectors <https://prestodb.github.io/docs/0.204/connector.html>`_, our
installation at Verizon Media only supports the following:

1. Hive
2. PostgreSQL
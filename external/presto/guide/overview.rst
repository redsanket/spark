Overview
########

What is Presto?
***************

Presto is an open source distributed ANSI SQL compliant query engine for running interactive
analytic queries. It was created by Facebook in 2012 and is successfully used in many companies,
including Teradata, Netflix, LinkedIn, Uber, Airbnb and more.

We have selected Presto to the BI engine of choice for Verizon Media.

Presto is Intended for
**********************

Presto is an excellent tool for interactive queries that require less than 10 second response time.
These queries tend to run against a relatively small (<1TB) data sets and produce small (<5k rows)
output.

Presto is also a reasonable choice for slightly larger queries that run in under a minute and that
would take much longer with the alternative solutions such as Pig or Hive.

Presto is generally intended for use with BI tools. The list of supported tools can be found in the
Connectivity section.

Cost and business needs should also be taken into account when deciding whether to use Presto.
Presto requires dedicated hardware and is a significantly more expensive option than Hive.

Presto is not Intended for
**************************

Presto is not intended for ETL or large batch queries that operate on data over 1 TB and produces
over 5K of rows.

It is also not cost efficient for canned reports and non business critical applications. Hive, Pig,
and Spark are better alternatives in these cases.

Presto Usage Criteria
*********************
+------------------------------------------+-------------------+------------------+
| Use case                                 | Presto            | Hive, Pig, Spark |
+==========================================+===================+==================+
| Require <30 second response time         | Use               | Avoid            |
+------------------------------------------+-------------------+------------------+
| Dataset size                             | < 1 TB            | > 1 TB           |
+------------------------------------------+-------------------+------------------+
| Output rows size                         | < 1 MB            | > 1 MB           |
+------------------------------------------+-------------------+------------------+
| Query run-time                           | < 2 min           | > 2 min          |
+------------------------------------------+-------------------+------------------+
| For use with BI tools:                   | Use               | Can use          |
| Looker, Tableau, Superset                |                   | HiveServer 2     |
+------------------------------------------+-------------------+------------------+
| Does not need dedicated hardware         | Avoid             | Use              |
+------------------------------------------+-------------------+------------------+
| Canned reports or                        | Avoid             | Use              |
| non-business critical applications       |                   |                  |
+------------------------------------------+-------------------+------------------+




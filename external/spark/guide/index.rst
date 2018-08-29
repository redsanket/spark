================
Spark User Guide
================

.. 05/13/15: Edited.

.. toctree::
   :maxdepth: 2
   :hidden:

   spark_on_yarn
   spark_from_oozie
   pyspark_with_python
   debugging
   spark_sql
   spark_r

This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

The goal of this guide is to help developers use Spark for Yahoo. For general Spark documentation,
see the `Apache Spark documentation <http://spark.apache.org/>`_.

.. _onboard:

On-Boarding
===========

Before you can use Spark at Yahoo, you need to on-board by completing the following
steps:

#. `Request a grid account <http://yo/supportshop>`_ with HDFS and JobTracker queue access.
#. Select the authentication mechanism for your Workflows: Kerberos (common for 
   headless users) or YCA.
#. Complete one of the quick starts in the :ref:`Getting Started <getting_started>` chapter.


Getting Help
============

General Questions
-----------------

- spark-users@oath.com

Support
-------

- spark-devel@oath.com

You could also reach out on the slack channel for spark - `#spark-users`

Filing Jira Tickets
-------------------

#. Go to the `Jira Spark Summary Panel <https://jira.corp.yahoo.com/browse/YSPARK/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel>`_.
#. From the top navigation bar, click **Create**.
#. Fill out the **Create Issue** form and click **Create**.

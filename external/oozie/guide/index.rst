================
Oozie User Guide
================

.. 05/13/15: Edited.

.. toctree::
   :maxdepth: 2
   :hidden:

   overview
   use_cases
   getting_started
   workflows
   coords
   bundles
   cookbook
   architecture
   auth
   client
   api
   faq
   ts  
   references
   resources
   oozie_https

This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

The goal of this guide is to help developers use Oozie for Yahoo. For general Oozie documentation,
see the `Apache Oozie documentation <http://oozie.apache.org/>`_.

.. _onboard:

On-Boarding
===========

Before you can use Oozie at Yahoo, you need to on-board by completing the following
steps:

#. `Request a grid account <http://yo/supportshop>`_ with HDFS and JobTracker queue access.
#. Select the authentication mechanism for your Workflows: Kerberos (common for 
   headless users) or YCA.
#. Complete one of the quick starts in the :ref:`Getting Started <getting_started>` chapter.


Getting Help
============

General Questions
-----------------

- oozie-users@yahoo-inc.com 

Support
-------

- oozie-request@yahoo-inc.com

Filing Jira Tickets
-------------------

#. Go to the `Jira Oozie Summary Panel <https://jira.corp.yahoo.com/browse/YOOZIE/?selectedTab=com.atlassian.jira.jira-projects-plugin:summary-panel>`_.
#. From the top navigation bar, click **Create**.
#. Fill out the **Create Issue** form and click **Create**.

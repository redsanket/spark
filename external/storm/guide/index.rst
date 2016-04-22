================
Storm User Guide
================

This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

Our goal is to help Yahoos use Storm on the Yahoo grid, and thus, is not a comprehensive guide for Storm.
For more comprehensive documentation, we recommend the `Apache Storm documentation <http://storm.incubator.apache.org>`_. 

On-Boarding
===========

In addition to an `On-Boarding <./onboarding/>`_ chapter, we also have a `quick start <./quickstart/>`_
and `tutorials <./tutorials>`_ to help you get started.

Getting Help
============

General Questions
-----------------

- storm-users@yahoo-inc.com - Storm User Communications 

Request Support
---------------

- storm-devel@yahoo-inc.com

Product/Engineering 
-------------------

Write to the Product Manager or Engineering Manager, whose emails you can find in 
the **Contacts** section on the `Developer Central: Storm <http://developer.corp.yahoo.com/product/Storm>`_
page.

Emergency Support
-----------------

Check on-call in the **Service Now** group "Dev-Spark" to get immediate support.

Filing Tickets
--------------

Bugzilla Links
##############

- Dev: http://bug.corp.yahoo.com/enter_bug.cgi?product=Low%20Latency
- Grid Ops: http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite

Ticket Filing Process
#####################

If you have identified a Storm issue within your environment or 
while testing, you may file a Bugzilla Ticket. When filing the ticket, 
include the following if relevant:

- Grid on which the issue was seen. For example, append [ebony-red] to the description of the ticket.
- The topology name, and component names if specific components have problems.
- Gateway used to launch the topology, if applicable.
- Relevant log messages with the error (Please include the entire stack trace).
- If possible, save the output of around 5 complete stack traces (via jstack) of a JVM with the issue.
- If possible, save the heap dump (via jmap) of a JVM with the issue.
- How you observed the issue (Steps to Reproduce)


Storm Clusters 
==============

See the `Storm Support: Deployment Details <http://twiki.corp.yahoo.com/view/Grid/SupportStorm#Deployment_Details>`_
for a list of the available clusters for both non-production and production. You'll also find
the type of clusters (Nimbus/Supervisor/ZooKeeper), number of nodes, and the hardware configuration for each cluster.

Typographic Conventions
=======================
 
This document uses the following typographic conventions:
 
 
.. Here is an example of creating a table with the ``csv-table``
   directive. You can also use the ``list-table`` directive to
   create tables.
 
 
 
.. csv-table:: Typographic Conventions
   :header: "Convention", "Description"
   :widths: 15, 40
 
 
   "**Bold**", "User interface objects such as buttons and menus."
   "*Italic*", "Emphasis for new terms, book titles, and glossary terms."
   "``Code``", "User input, system output, file names, code examples, and URLs."
 
 
 
Iconographic Conventions
========================
 
 
This document uses the following iconographic conventions:
Indicates a user tip related to a procedural step or general information.
 
 
 
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| **Convention**                                                                  | **Description**                                                        |
+=================================================================================+========================================================================+
| .. note:: Storm is compatible with Tej.                                         | Calls attention to additional information.                             |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. warning:: Storm does not replace the need for batch operations.              | Indicates an important caution or warning related to a procedural step |
|                                                                                 | or general information.                                                |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. tip:: It's easy to use Yahoo Spouts to tap into data.                        | Indicates a best practice that we recommend.                           |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. important:: Be sure to use a non-production environment for testing Storm.   | Emphasizes the importance of information given earlier.                |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. error:: Topology submission exception.                                       | Indicates a commonly seen error message that users may see.            |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. caution:: Your topology must be assigned to a supervisor.                    | Advises user to perform actions to avoid errors.                       |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+

.. toctree::
   :maxdepth: 2
   :hidden:

   overview/index
   quickstart/index
   onboarding/index
   tutorials/index
   programming/index
   security/index
   registry_service_api/index
   monitoring/index
   architecture/index
   reference/index

================
Storm User Guide
================

About This Guide
================

This guide provides Yahoos with a simplified version of documentation for Storm, 
in this guide, see ....

Getting Help
============

On-Boarding
-----------


Asking Questions
----------------

Request Support
---------------

- iList: storm-devel@yahoo-inc.com
- Phone: Check on-call in the Service Now group "Dev-Spark”

Filing Tickets
--------------

- Dev: http://bug.corp.yahoo.com/enter_bug.cgi?product=Low%20Latency
- Grid Ops: http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite

If you have identified an issue with storm itself in your own environment or while testing, you may file a Bugzilla Ticket.
When filing the ticket, please include the following if relevant:
Grid on which the issue was seen. For example, append [ebony-red] to the description of the ticket.

The topology name, and component names if specific components have problems.
Gateway used to launch the topology, if applicable.
Relevant log messages with the error (Please include the entire stack trace).
If possible, save the output of around 5 complete stack traces (via jstack) of a JVM with the issue.
If possible, save the heap dump (via jmap) of a JVM with the issue.
How you observed the issue (Steps to Reproduce)
Reporting Directly to Dev Team

If you have identified an issue with storm itself in your own environment or while testing, you may file a Bugzilla Ticket.
Reporting Issues on the Grid (Production/Non-Production)
For issues with a storm cluster on the grid, including production, refer to Reporting Problems.
Reference SupportStorm to see which supported Storm clusters are production and otherwise.

Contact
#######


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
| .. note:: YMonkey is compatible with YGorilla.                                  | Calls attention to additional information.                             |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. warning:: YMonkey does not run on Node.js.                                   | Indicates an important caution or warning related to a procedural step |
|                                                                                 | or general information.                                                |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. tip:: It's easy to create templates with YChimp.                             | Indicates a best practice that we recommend.                           |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. important:: Before you install YMonkey, be sure to review the prerequisites. | Emphasizes the importance of information given earlier.                |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. error:: Can't find ``ymonkey-renderer``.                                     | Indicates a commonly seen error message that users may see.            |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
| .. caution:: Use ``wss`` for WebSocket connections in YMonkey.                  | Advises user to perform actions to avoid errors.                       |
+---------------------------------------------------------------------------------+------------------------------------------------------------------------+
.. toctree::
   :maxdepth: 2
   :hidden:

   overview/index
   tutorial/index
   programming/index
   security/index
   registry_service_api/index
   monitoring/index
   architecture/index
   reference/index

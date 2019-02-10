===================
Starling User Guide
===================

.. toctree::
   :maxdepth: 2
   :hidden:

   overview/index
   getting_started/index
   architecture/index
   data/index
   query_bank/index


This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

Our goal is to help Yahoos use Starling on the Yahoo grid, and thus, is not a comprehensive guide for Starling.

.. _onboard:

On-Boarding
===========

Starling can only be accessed from the `Axonite Blue cluster <https://supportshop.cloud.corp.yahoo.com:4443/doppler/hadoop/cluster/AB>`_. You'll receive access to Axonite Blue by requesting grid access through `Doppler <http://yo/doppler>`_.

Getting Help
============

General Questions
-----------------

- #starling-users

Request Support
---------------

Filing Tickets
--------------

`Starling Jira <http://yo/starlingsupport>`

Ticket Filing Process
#####################

If you have identified an issue with Starling withn your environment or
while testing, you may file a Jira. When filing the ticket,
include the following if relevant:

- Grid on which the issue was seen. For example, append [ebony-red] to the description of the ticket.
- The topology name, and component names if specific components have problems.
- Gateway used to launch the topology, if applicable.
- Relevant log messages with the error (Please include the entire stack trace).
- If possible, save the output of around 5 complete stack traces (via jstack) of a JVM with the issue.
- If possible, save the heap dump (via jmap) of a JVM with the issue.
- How you observed the issue (Steps to Reproduce)


Environment
===========

Starling is available on the Axonite Blue cluster.


Typographic Conventions
=======================

This document uses the following typographic conventions:



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

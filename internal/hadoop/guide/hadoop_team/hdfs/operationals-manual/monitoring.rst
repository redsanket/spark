********************************
Logging, Monitoring and Alerting
********************************

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Logs
====

.. include:: /common/hdfs-logs-table.rst

Error Messages
==============

.. todo:: Key error messages which we tend to see. Table with error message, component, and meaning of error. Let’s have a few examples but I think this is better filled in as we go.

Health Checks
=============

* Configurations are hosted `git GridSE/ygrid-yamas2-checks <https://git.ouroath.com/GridSE/ygrid-yamas2-checks/tree/master/recipes>`
* What is running where & what are the thresholds
* What to do when you receive x alert

Dashboards
==========

.. include:: /common/aura-dashboards-table.rst


How to identify customers of the service
========================================

* HDFS users are LDAP user names
* Headless users can be broken down into headed users via:
  ``getent netgroup ${headed_user_name}_sudoers``
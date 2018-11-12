================
Storm User Guide
================

This document is **Oath Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

Our goal is to help Builders use Storm on the Oath grid, and thus, is not a comprehensive guide for Storm.
For more comprehensive documentation, we recommend the `Apache Storm documentation <http://storm.apache.org/releases/current/index.html>`_. 

On-Boarding
===========

In addition to an `On-Boarding <./onboarding/>`_ chapter, we also have a `quick start <./quickstart/>`_
and `tutorials <./tutorials>`_ to help you get started.

Getting Help
============

General Questions
-----------------

- storm-users@oath.com - Storm User Communications 

Request Support
---------------

- storm-devel@oath.com


Emergency Support
-----------------

- Use @oncall to find out and directly message the current oncall in `#storm <https://ouroath.slack.com/messages/C6LMLJXPG/>`_


Filing Tickets
--------------

Ticket Filing Process
#####################

If you have identified a Storm issue within your environment or 
while testing, you may file a `JIRA Ticket <http://yo/ystorm-request>`_. When filing the ticket, 
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

See the `Storm Support: Deployment Details <https://jenkins-k.ygrid.yahoo.com:4443/jenkins/job/1.01_GridVersions/lastSuccessfulBuild/artifact/versions.html>`_
for a list of the available clusters for both non-production and production. You'll also find
the type of clusters (Nimbus/Supervisor/ZooKeeper), number of nodes, and the hardware configuration for each cluster.

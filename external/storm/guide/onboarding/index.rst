===========
On-Boarding
===========

In this chapter, we'll show you how to on-board to the
non-production environment for Storm. After you're done, we
also direct you on how to on-board for production.

The process of on-boarding may take several days based on your request and
the response of the Grid team. In the future, the on-boarding
process to Storm will be automated, and thus, automatic.

.. _onboarding-create:

Storm Onboarding for Real Users
===============================

One-click access for all Yahoo grids, including Hadoop, HBase, and Storm
clusters is available for real users via `Doppler <http://yo/doppler>`_.
Once access has been requested, it takes approximately four hours for
propagation to complete across all clusters. This will grant you access
to the free pool on any Storm cluster. If you are asking for access to do experiments
with Storm using your own user, you do not need anything further from Doppler.
However, you must use a headless user if you are going to use Storm for production,
staging, or CI/CD. Read on to learn how to set that up.

Storm Onboarding for Headless Users
===================================

For Production environments, topologies must be run by headless users, and headless
users are still encouraged on Non-Production environments.  Given the long running
nature of Storm topologies, use of a headless user allows multiple team members and
SE to kill and relaunch topologies.

Creating a Storm Project
------------------------

You may or may not require a new Storm project. Please see the Doppler document
`Do I need to create a new Storm project <http://yo/doppler-howto-new-storm-project>`_.
If you do require a new project, read on. If not, skip to the
"`Create a New Project Environment`_"  section.

#. Go to `Storm on Doppler <http://yo/doppler-storm>`_.
#. Click **Create a new project** and then confirm by clicking **New Project** in the pop-up
   confirmation window.
#. In the **New Storm Project** page, enter a project name in the **Project Name** text field
   and a short project description.
#. From the **Contact Info** section of the page, select **Real User** from the **Contact Type** menu,
   your role (use **Technical Lead** if you're not sure) from the **Role** menu, and enter your
   Yahoo user name for the **User ID** text field.
#. Click **Submit New Project**. You will be taken to the project page for your newly created project.

.. https://supportshop.cloud.corp.yahoo.com:4443/doppler/storm

Create a New Project Environment
--------------------------------

Please see the Doppler document `How do I onboard in existing project onto a new
cluster? <http://yo/doppler-howto-new-storm-env>`_ for instructions on creating a new
Project Environment.

For an explanation of Project Environments in Storm, please see the Doppler document `Intro to
Storm Environments in Doppler <http://yo/doppler-storm2-intro>`_.

Adding a User to an Existing Environment
----------------------------------------

Please see the Doppler document `How do I add a new user to my Storm project <http://yo/doppler-howto-add-storm-user>`_
for instructions on adding a user to an existing Storm environment.

Next Step
=========

Try the `Tutorial: Counting Data Highway Events <../tutorials/index.html#storm_tutorials-counting>`_
or on-board to the production environment by following the same steps above but selecting
**Production** and specifying your capacity needs:

- number of servers
- number of workers
- number of executors
- isolated topology
- Rainbow Data Highway integration
- throughput
- SLA requirements

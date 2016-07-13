===========
On-Boarding
===========

In this chapter, we'll show you how to on-board to the
non-production environment for Storm. After you're done, we
also direct you on how to on-board for production.

The process of on-boarding may take several days based on your request and
the response of the Grid team. In the future, the on-boarding
process to Storm will be automated, and thus, automatic.

-.. _onboarding-create:

Storm Onboarding for Real Users
===============================

One-click access for all Yahoo grids, including Hadoop, HBase, and Storm
clusters is available for real users via `Doppler <http://yo/doppler>`_.
Once access has been requested, it takes approximately four hours for
propagation to complete across all clusters. This will grant you access
to the free pool on any Storm cluster. If you require access for your
own user for experimentation to get started using Storm, no additional
Doppler requests are required. Once you are ready to do real work, you
will need a headless user. Read on for instructions on getting headless
users onboarded to Storm.

Storm Onboarding for Headless Users
===================================

For Production environments, topologies must be run by headless users, and headless
users are still encouraged on Non-Production environments.  Given the long running
nature of Storm topologies, use of a headless user allows multiple team members and
SE to kill and relaunch topologies.

Creating a Storm Project
------------------------

You may or may not require a new Storm project. Please see the Doppler document
`Do I need to create a new Storm project <http://yo/doppler-storm-new-project-q>`_.
If you do require a new project, read on. If not, skip to the instructions to
"`Create a New Project Environment`_".

#. Go to `Storm on Doppler <http://yo/doppler-storm>`_.
#. Click **Create a new project** and then confirm by clicking **New Project** in the pop-up
   confirmation window.
#. In the **New Storm Project** page, enter a project name in the **Project Name** text field
   and a short project description.
#. From the **Contact Info** section of the page, select **Real User** from the **Contact Type** menu,
   your role (use **Technical Lead** if you're not sure) from the **Role** menu, and enter your
   Yahoo user name for the **User ID** text field.
#. Click **Submit New Project**. You will be taken to the project page for your newly created project.

.. http://ebonyred-ni.red.ygrid.yahoo.com:9999@grid.red.ebony.supervisor/
.. https://supportshop.cloud.corp.yahoo.com:4443/doppler/#/storm

Create a New Project Environment
--------------------------------

For an explanation of Project Environments in Storm, please see the Doppler document `Intro to
Storm Environments in Doppler <http://yo/doppler-storm2-intro>`_.

#. From your the **Storm Project** page, you'll see your project name listed as one of the projects that you
   a member of or have selected as a favorite. Click the project name to open your project page.
#. From your project page, scroll to the **Environments** panel.

   - If the cluster you require is already listed, please proceed to the "`Adding a User to an Existing Environment`_"
     section.
   - If the cluster you require is not listed, please proceed to the next step.
#. From your project page, click the **New Environment** button under the label **Environments**.
#. From the **New Environment** page, enter the following information in the form:

   - **Environment Type** - Select **Non-Production**.
   - **Cluster** - Select any of the non-production environments.
   - **Users** - Add the user or users you will be using to run topologies. For Production environments, topologies
     *must* be run by headless users, and headless users are still strongly encouraged on Non-Production
     environments. Given the long running nature of Storm topologies, use of a headless user allows multiple
     team members and SE to kill and relaunch topologies.

     - **Isolation** - For each user, specify if the user will be isolated or not.  Isolated users run their
       topologies on dedicated hosts that no other users can use. This is an inefficient way to use a Storm cluster
       and should be avoided when possible.
     - **Quotas** - For each user, specify a quota of RAM and vCores, or number of nodes, if isolated.
       Storm will not allow isolated users to use more nodes than their isolated nodes quota.  However, RAM and
       CPU quotas for non-isolated users are not strictly enforced at this time.

       - For non-production environments, it is expected you may not yet know your needs.  If so, 24 vCores and
         96 GB (~1 server) is a good place to start.  It's okay to use more than this as you experiment with
         Storm and figure out your production needs.
       - For production environments, it is expected your needs are more well understood.  Exceeding your quota may
         negatively impact other Storm users and could result in production incidents.  You will be able to request
         changes to your quotas in future requests.

#. Click **Submit New Environment Request**.
#. The Grid SEs will review your request before approving it. You also may need to answer questions.
   The entire process can take from three to five working days.

Adding a User to an Existing Environment
----------------------------------------

Please see the Doppler document `How do I add a new user to my Storm project <http://yo/doppler-storm-add-user-q>`_
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

===========
On-Boarding 
===========

.. Status: first draft. The Dopplr form section may need to be removed or altered.
   Currently, I've been told that the form is not to be used, but by the time
   the documentation is finished, the team may be using the form again, but a
   different version, so I've left the section in the documentation for the time being (08/26/14).

In this chapter, we'll show you how to get on-board to the 
non-production environment for Storm. After you're done, we
also show you what steps to take to get on-boarded for production.
 
The process of on-boarding may take several days based on your request and
the response of the Grid team. In the future, the on-boarding
process to Storm will be automated, and thus, automatic.

.. _onboarding-create:

Creating a Storm Project
========================

#. Go to the `Grid Support Shop <http://yo/supportshop>`_.
#. From the **Grid Support Shop**, click **Storm Onboarding**. 

   .. image:: images/support_shop-storm_onboarding.jpg
      :height: 490px
      :width: 800 px
      :scale: 90 %
      :alt: Figure showing user clicking "Storm Onboarding" from the Support Shop.
      :align: left 

#. You'll be taken to a Doppler form to create a Storm project. Click **Create a new project**
   and then confirm by clicking **New Project** in the pop-up confirmation window.
#. In the **New Storm Project** page, enter a project name in the **Project Name** text field, 
   a unique headless user (use your own user name if you don't have a headless user) in the
   **Unique Headless User** text field, and a short project description indicating that this
   is a test project.
#. From the **Contact Info** section of the page, select **Real User** from the **Contact Type** menu,
   your role (use **Technical Lead** if you're not sure) from the **Role** menu, and enter your
   Yahoo user name for the **User ID** text field. 
#. Click **Submit New Project**. You'll be taken to a confirmation page. The turnover rate varies,
   but generally, you'll be on-boarded within a few business days. 
   
.. http://ebonyred-ni.red.ygrid.yahoo.com:9999@grid.red.ebony.supervisor/
.. https://supportshop.cloud.corp.yahoo.com:4443/doppler/#/storm  

Create a New Topology
=====================

The topology you are creating in this section is not the Storm topology defined as a *graph of computation*.
Instead, you are requesting an instance in a cluster for running your Storm topology. For instance,
your Storm topology might include two spouts and two bolts, so you request an instance on a Storm cluster
to run your topology. To request an instance, you create a topology in Dopplr. We know that this is confusing
and plan on changing the nomenclature soon.

#. From your the **Storm Project** page, you'll see your project name listed as one of the projects that you 
   a member of or have selected as a favorite. Click the project name to open your project page.
#. From your project page, click the **New topology** button under the label **Topologies**.
#. From the **New Topology** page, enter the following information in the form:

   - **Topology Name** - Enter a unique string. For the first tutorial, you'll be using logging
     data from the Rainbow Data Highway,  so you might use a relevant topology name.
   - **Topology Short Description** - Again, your description should be related to your project.
     If you are creating a topology for one of the `Tutorials <../tutorials>`_, enter
     a related description.
   - **Environment Type** - Select **Non-Production**.
   - **Cluster** - Select any of the non-production environments.
#. Click **Submit New Topology Request**.
#. The Grid SEs will review your request before approving it. You also may need to answer questions.
   The entire process can take from three to five working days.

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

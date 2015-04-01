=======
How Tos
=======


This chapter is for users who have completed the  `Getting Started <../getting_started/>`_
and need to complete a specific task. See the `Overview <../overview>`_ for
general information about Hue and `Hue UI <../ui>`_ to learn about the Hue UI components.

Creating a Coordinator
======================

Coordinators allow you to schedule Oozie Workflows to run. You can think of
Coordinators as Cron jobs on the grid. Coordinators also allow you
set inputs and outputs, so you can dynamically run queries with new
parameters. 

You'll be creating a simple Hive query that will be used in a 
Oozie Workflow, and then scheduled with a Coordinator.

Prerequisites
-------------

- Complete the `Getting Started <../getting_started/>`_.
- Access to the `benzene database on Cobalt <https://supportshop.cloud.corp.yahoo.com:4443/data-discovery/search/benzen/cluster/CB/database/benzene/table/daily_data>`_.

  If you **don't** see the green button **View in Hue**, you'll need to request
  access to ``benzene`` by doing the following:

  #. From the **Data Discovery Tool**, click **Request Access** for **benzene.daily_data**.
  #. Have your manager approve your request.
  #. Complete `Data Governance at Yahoo (General Audience Version) <https://yahoo.plateau.com/learning/user/common/viewItemDetails.do?componentTypeID=ELEARN&goalid=&componentID=YHDG-DP-DATAGOVERNANCE>`_.
  #. Wait until the Grid team approves your request. This is generally done on the same day of your request.

1. Create a Hive Query With Parameterized Inputs
------------------------------------------------

Create an HiveQL script with the following:: 

    set hive.exec.compress.output=false;

    INSERT OVERWRITE DIRECTORY '${OUTPUT}'

    select count(1) as views, mobile_app_name as app from benzene.daily_data 
    where dt =SUBSTR('${DATE}', 76) and network="on" and pty_family="news" 
    and pty_device="mobile" and pty_experience="app" and event_family="view" group 
    by mobile_app_name order by views desc;

.. note:: The parameterized inputs ``${OUTPUT}`` and ``${DATE}`` will be 
          replaced with values by the coordinator.

2. Create an Oozie Workflow for the Hive Script
-----------------------------------------------

#. Create a simple Oozie Workflow with just a Hive action.
#. In the **Edit Node** page, add the two parameters **DATE=${input_dataset}** and
   **OUTPUT=${output_dataset}** as shown below:

   .. image:: images/benzene_params.jpg
      :height: 538px
      :width: 907 px
      :scale: 90%
      :alt: Editing Hive action for Benzene Hive query.
      :align: left   

#. For the **Job Properties**, in addition to ``oozie.action.sharelib.for.hive`` and
   ``hive.querylog.location``, add the properties **input** and **output** that
   should have the values **${input_dataset}** and **${output_dataset}** respectively.

   .. image:: images/benzene_job_properties.jpg
      :height: 196px
      :width: 750 px
      :scale: 93%
      :alt: Added job properties for the Benzene Hive action.
      :align: left   

#. for the **Job XML** field, add the path **/user/sumeetsi/hive-site.xml**.
#. Click **Done**.


3. Creating a Coordinator
------------------------- 

#. From the top navigation bar, click **Workflows->Editors->Coordinators**.

   .. image:: images/open_coordinator_editor.jpg
      :height: 152px
      :width: 619 px
      :scale: 95%
      :alt: Opening the Coordinator Editor.
      :align: left   

#. From the **Coordinator Manager** pane, click **Create**.

   .. image:: images/create_coordinator_button.jpg
      :height: 170px
      :width: 950 px
      :scale: 90%
      :alt: Create a Coordinator.
      :align: left   
#. 


Creating a Bundle
=================

Overview
--------

Prerequisites
-------------



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

Using the `Cobalt Blue Hue UI <http://yo/hue.cb>`, create an HiveQL script with the following:: 

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


3. Create a Coordinator
----------------------- 

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
#. From **Coordinator Editor->Step 1: General**, enter a name for your coordinator,  
   select the Oozie Workflow that you created for **Workflow** drop-down menu,
   and click **Next**.

   .. image:: images/step1_coord.jpg
      :height: 442px
      :width: 601 px
      :scale: 95%
      :alt: Step 1: Adding details for your Coordinator.
      :align: left  

#. From **Coordinator Editor->Step 2: Frequency**, leave the default value for the 
   start date but change the end date to tomorrow's date. 

   .. note:: Generally, for Coordinators that
             you create on your own, you will use an end date that is in the distant future. Unfortunately,
             you are required to select an end date.

#.  From **Coordinator Editor->Step 3: Frequency**, click **here** to create a dataset.
    We're going to need to create one for the parameters **input_dataset** and **output_dataset**
    that we defined in the Workflow.

   .. image:: images/step3_create_dataset.jpg
      :height: 404 px
      :width: 717 px
      :scale: 95%
      :alt: Step 3: Create dataset.
      :align: left  


#. From the **Create a new dataset**, enter the following values for the fields listed below:

   - **Name** - **benzene_daily_input**
   - **Frequency number** - **1**
   - **Frequency unit** - **Days**
   - **URI** - **hcat://cobaltblue-hcat.ygrid.vip.gq1.yahoo.com:50513/benzene/daily_data/dt=${YEAR}${MONTH}${DAY}**

   .. image:: images/create_dataset.jpg
      :height: 453 px
      :width: 950 px
      :scale: 90%
      :alt: Create the dataset for the input_dataset parameter.
      :align: left  

#. For the **Instance** field, select **Single**, check the **(advanced** checkbox, and enter **${coord:current(-1)}**
   in the **(advanced)** field. The **-1** indicates the Coordinator will go back one unit (day) in the past
   and execute the Workflow.

   .. image:: images/customize_instance.jpg
      :height: 222 px
      :width: 950 px
      :scale: 90%
      :alt: Customize the instance by defining a range of dates using EL functions.
      :align: left  

#. Click **Create dataset**.
#. From the **Existing datasets** pane, click **Save coordinator**.

   .. image:: images/save_coordinator.jpg
      :height: 167 px
      :width: 950 px
      :scale: 90%
      :alt: Save the Coordinator.
      :align: left  

#. Click **Step 4: Outputs**.

   .. image:: images/step4_outputs.jpg
      :height: 407 px
      :width: 582 px
      :scale: 95%
      :alt: Step 4: Creating Outputs
      :align: left  

#. Again, create another dataset and enter the values below for the listed fields:

   - **Name** - **benzene_daily_output**
   - **Frequency number** - **1**
   - **Frequency unit** - **Days**
   - **URI** - **/user/{your_user_name}/benzeneoutput/${YEAR}${MONTH}${DAY}**
   - **Instance->Single** - Check the **(advanced)** checkbox and enter **${coord:coord(-1)}** as the 
     value for **(advanced)** field.

   .. image:: images/create_output_dataset.jpg
      :height: 722 px
      :width: 950 px
      :scale: 90%
      :alt: Creating Output Dataset
      :align: left  

#. Click **Create dataset** and then **Save coordinator**.

   
4. Create a Directory to Store Output
------------------------------------- 

#. In a new tab, open the **File Browser**.
#. From your **home** directory, create the directory **benzeneoutput**. 
   Your Coordinator is going to write output files to this directory.

5. Submit Your Coordinator
--------------------------

#. From the **Coordinator Editor**, click **Submit** in the left-hand panel.

   .. image:: images/submit_coord.jpg
      :height: 441 px
      :width: 872 px
      :scale: 92%
      :alt: Submitting the Coordinator
      :align: left  

#. From the **Submit this job?** dialog prompt, click **Submit**.

   .. image:: images/submit_job.jpg
      :height: 125 px
      :width: 483 px
      :scale: 98%
      :alt: Click Submit in the Submit this job? dialog.
      :align: left  

#. From the **Coordinator Editor** page, you should see **Running** as the **Status** in the left-hand pane.

   .. image:: images/status_running.jpg
      :height: 182 px
      :width: 950 px
      :scale: 90%
      :alt: Viewing the Status of the Job.
      :align: left  


6. View Coordinator Jobs and Results
------------------------------------


#. Open the **Job Browser** in a new tab.
#. You should see that the launcher job (stays at 5% until the others are done) and the child that is doing the querying.

   .. image:: images/job_browser_coord.jpg
      :height: 177 px
      :width: 950 px
      :scale: 90%
      :alt: Monitoring Jobs in Job Browser.
      :align: left  
   
   It may take a few minutes before the job is accepted before it can start. So, if you don't see your jobs, just wait a few minutes.

#. Once your jobs have completed, the **Job Browser** will mark your jobs with the green status **Succeeded**.

   .. image:: images/jobs_succeeded.jpg
      :height: 138 px
      :width: 950 px
      :scale: 90%
      :alt: Coordinator Jobs Have Succeeded.
      :align: left  
   
#. At this point, using the **File Browser**, go to the directory **/user/{your_user_name}/benzeneoutput/**.
#. You should see directories containing the output from your Hive query.

   .. image:: images/coord_output.jpg
      :height: 265 px
      :width: 950 px
      :scale: 90%
      :alt: Output from Coordinator.
      :align: left  

#. If you open the output file, you should see something similar to that below.

   .. image:: images/coord_file_output.jpg
      :height: 270 px
      :width: 950 px
      :scale: 90%
      :alt: File Output from Coordinator.
      :align: left  
   










Creating a Bundle
=================

Overview
--------

Prerequisites
-------------



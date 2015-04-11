.. _cookbook:

Oozie Cookbook
==============

In this chapter, we will offer you examples
of how to complete common tasks for 
Workflows, Coordinators, and Bundles. 



Prerequisites
-------------

- Complete the :ref:`Getting Started <getting_started>`.

Accessing Hadoop Counters in Previous Actions
---------------------------------------------


In this example, we access a user-defined hadoop counter in a subsequent action
This is a simple way to pass data between different oozie actions
The 'mr1' action generates a user-defined hadoop counter named ['COMMON']['COMMON.ERROR_ACCESS_DH_FILES'].
The value of this counter is accessed in the subsequent 'java1' action.


Workflow
~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.5' name='java-wf'>
       <start to='mr1' />
       <action name='mr1'>
           <map-reduce>
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
               <configuration>
                   <property>
                       <name>mapred.mapper.class</name>
                       <value>org.apache.oozie.example.SampleMapper</value>
                   </property>
                   <property>
                       <name>mapred.reducer.class</name>
                       <value>org.apache.oozie.example.SampleReducer</value>
                   </property>
   
                   <property>
                       <name>mapred.input.dir</name>
                       <value>${inputDir}</value>
                   </property>
                   <property>
                       <name>mapred.output.dir</name>
                       <value>${outputDir}/streaming-output</value>
                   </property>
                   <property>
                     <name>mapred.job.queue.name</name>
                     <value>${queueName}</value>
                   </property>
   
                  <property>
                     <name>mapred.child.java.opts</name>
                     <value>-Xmx1024M</value>
                  </property>
   
               </configuration>
           </map-reduce>
           <ok to="java1" />
           <error to="fail" />
       </action>
       <action name='java1'>
           <java>
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
               <configuration>
                  <property>
                       <name>mapred.job.queue.name</name>
                       <value>${queueName}</value>
                   </property>
               </configuration>
               <main-class>org.apache.oozie.test.MyTest</main-class>
               <arg>${hadoop:counters('mr1')['COMMON']['COMMON.ERROR_ACCESS_DH_FILES']}</arg>
               <capture-output/>
           </java>
           <ok to="pig1" />
           <error to="fail" />
       </action>
   
       <kill name="fail">
           <message>Job failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
       </kill>
       <end name='end' />
   </workflow-app>

Increasing Memory for Hadoop Job
--------------------------------


You can define a property in your action:

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.5' name='streaming-wf'>
       <start to='streaming1' />
       <action name='streaming1'>
           <map-reduce>
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
               <streaming>
                   <mapper>/bin/cat</mapper>
                   <reducer>/usr/bin/wc</reducer>
               </streaming>
               <configuration>
                   <property>
                       <name>mapred.input.dir</name>
                       <value>${inputDir}</value>
                   </property>
                   <property>
                       <name>mapred.output.dir</name>
                       <value>${outputDir}/streaming-output</value>
                   </property>
                   <property>
                     <name>mapred.job.queue.name</name>
                     <value>${queueName}</value>
                   </property>
   
                  <property>
                     <name>mapred.child.java.opts</name>
                     <value>-Xmx1024M</value>
                  </property>
   
               </configuration>
           </map-reduce>
           <ok to="end" />
           <error to="fail" />
       </action>
       <kill name="fail">
           <message>Streaming Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
       </kill>
       <end name='end' />
   </workflow-app>


Using Custom Input Format
-------------------------

You can define a property in your action:

.. code-block:: xml

   <property>
     <name>mapred.input.format.class</name>
     <value>com.yahoo.mycustominputformat.TextInputFormat</value>
   </property>

Workflow
~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.5' name='streaming-wf'>
       <start to='streaming1' />
       <action name='streaming1'>
           <map-reduce>
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
               <streaming>
                   <mapper>/bin/cat</mapper>
                   <reducer>/usr/bin/wc</reducer>
               </streaming>
               <configuration>
                   <property>
                       <name>mapred.input.dir</name>
                       <value>${inputDir}</value>
                   </property>
                   <property>
                       <name>mapred.output.dir</name>
                       <value>${outputDir}/streaming-output</value>
                   </property>
                   <property>
                      <name>mapred.job.queue.name</name>
                      <value>${queueName}</value>
                   </property>
                   <property>
                      <name>mapred.input.format.class</name>
                      <value>com.yahoo.ymail.antispam.featurelibrary.TextInputFormat</value>
                   </property>
               </configuration>
           </map-reduce>
           <ok to="end" />
           <error to="fail" />
       </action>
       <kill name="fail">
           <message>Streaming Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
       </kill>
      <end name='end' />
   </workflow-app>



Submitting a Workflow With a YCAv2(gYCA) Certificate
----------------------------------------------------

User has to specify the gYCA credential explicitly in the workflow beginning and 
asks Oozie to retrieve certificate whenever an actions needs to call YCA protected
web service. In each credential element, attribute "name" is key and attribute 
"type" indicates which credential to use.

- The credential "type" is defined in oozie server. For example, on axoniteblue-oozie.blue.ygrid.yahoo.com, 
  the YCA credential type is defined as "yca", as in yoozie_conf_axoniteblue.axoniteblue_conf_oozie_credentials_credentialclasses: yca=com.yahoo.oozie.action.hadoop.YCAV2Credentials,howl=com.yahoo.oozie.action.hadoop.HowlCredentials,hcat=com.yahoo.oozie.action.hadoop.HowlCredentials
- User can give multiple "credential" elements under "credentials" and specify a list of credentials with comma separated to use under each action "cred" attribute.
- There is only parameter required for the credential "type".
  yca-role : the role name contains the user names for yca v2 certificates.
- There are three optional parameters for the credential type "yca".
  yca-webserver-url : the yca server url. Default is http://ca.yca.platform.yahoo.com:4080
  yca-cert-expiry: Expiry time of the yca certificate in seconds. Default is 1 day (86400). Available from Oozie 3.3.1

- ``yca-http-proxy-role``: The roles DB role name which contains the hostnames of 
  the machines in the http proxy vip. Default value is grid.httpproxy which contains 
  all http proxy hosts. Depending on the http proxy vip you will be using to send 
  the obtained YCA v2 certificate to the web service outside the grid, you can 
  limit the corresponding role name that contains the hosts of the http proxy vip. 
  The role names containing members of production http proxy vips are grid.blue.prod.httpproxy, 
  grid.red.prod.httpproxy and grid.tan.prod.httpproxy. For eg: http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.prod.httpproxycontains 
  the hosts of production httpproxy. http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.httpproxy 
  is a uber role which contains staging, research and production httpproxy hosts.http://twiki.corp.yahoo.com/view/Grid/HttpProxyNodeList 
  gives the role name and VIP name of the deployed http proxies for staging, research and sandbox grids.

Example Workflow
~~~~~~~~~~~~~~~~

.. code-block:: xml

   <workflow-app>
     <credentials>
        <credential name='myyca' type='yca'>
           <property>
              <name>yca-role</name>
              <value>griduser.actualuser</value>
           </property>
         /credential> 
     </credentials>
     <action cred='myyca'>
        <map-reduce>
        --IGNORED--
        </map-reduce>
     </action>
   <workflow-app>

Proxy
~~~~~

When Oozie action executor sees a "cred" attribute in current action, depending 
on credential name given, it finds the appropriate credential class to retrieve 
the token or certificate and insert to action conf for further use. In above example, 
Oozie gets the certificate of gYCA and passed to action conf. Mapper can then use 
this certificate by getting it from action conf, and add to http request header 
when connect to YCA protected web service through HTTPProxy. A certificate or token 
which retrieved in credential class would set in action conf as the name of 
credential defined in workflow.xml. The following examples shows sample code to 
use in mapper or reducer class for talking to YCAV2 protected web service from grid.

.. code-block:: java


   //**proxy setup**

   //blue proxy
   //InetSocketAddress inet = new InetSocketAddress("flubberblue-httpproxy.blue.ygrid.yahoo.com", 4080);
   //gold proxy
   InetSocketAddress inet = new InetSocketAddress("httpproxystg-rr.gold.ygrid.yahoo.com", 4080);
   Proxy proxy = new Proxy(Type.HTTP, inet);
   URL server = new URL(fileURL);

   //**web service call**
   String ycaCertificate = conf.get("myyca");
   HttpURLConnection con = (HttpURLConnection) server.openConnection(proxy);
   con.setRequestMethod("GET");
   con.addRequestProperty("Yahoo-App-Auth", ycaCertificate);

Passing Parameters to Coordinator EL Functions
----------------------------------------------

One can pass parameterize parameter to EL function, which is defined as job property.
Example.

.. code-block:: xml

   <input-events>
       <data-in name="zas_daily_datain" dataset="zas_daily_dataset">
           <start-instance>${coord:latest(coord.end.instance)}</start-instance>
           <end-instance>${coord:latest(coord.start.instance)}</end-instance>
       </data-in>
   </input-events>


Where coord.start.instance and coord.end.instance are parameters defined in job.properties used during submission.

Using Headless Users
--------------------

Oozie uses Backyard authentication.
If you want to use a headless user, you need to do the following:

- Request a `Headless Bouncer account <http://twiki.corp.yahoo.com/view/SSO/HeadlessAccountSetup>`_. These accounts need a underscore "_" in their name. 
- Request a headless UNIX account, that matches the name of your headless Backyard account.

Use the following steps to setup your Headless User with Oozie:

#. Setup your ``keydb`` file in the path ``/home/y/conf/keydb/``::

       $ sudo keydbkeygen oozie headlessuer.pw

#. Confirm that your ``keydb`` file looks similar to that below:

   .. code-block:: xml

      <keydb>
         <keygroup name="oozie" id="0">
            <keyname name="headless_user.pw" usage="all" type="a">
               <key version="0"
                   value = "mYsecreTpassworD" current = "true"
                   timestamp = "20040916001312"
                   expiry = "20070916001312">
               </key>
            </keyname>
         </keygroup>
      </keydb>


Configuring Oozie Jobs to Use Two NameNodes (Oozie Striping)
------------------------------------------------------------

1. Identify the JobTracker and its native NameNode
**************************************************

For example, the jobtracker to be used is JT1, and JT1's native (or default) namenode is NN1.
then the second namenode is NN2.

2. Configure the Oozie job application path
*******************************************

oozie job's application path, including coordinator.xml, workflow.xml, lib/, needs to be on jobtracker's default namenode, i.e., NN1.
default namenode should be set to NN1.

For example:

Coordinator: **job.properties**

.. code-block:: bash

   oozie.coord.application.path=hdfs://NN1:8020/projects/test_sla2-4
   nameNode=hdfs://NN1:8020
   wf_app_path=hdfs://NN1:8020/projects/test_sla2-4/demo
   jobTracker=JT1:50300

Workflow: **job.properties**

.. code-block:: bash

   oozie.wf.application.path=hdfs://NN1:8020/yoozie_test/workflows/pigtest
   nameNode=hdfs://NN1:8020
   jobTracker=JT1:50300

3. Creating the Pig action
**************************

the pig script should be on NN1.
for pig 0.8, use 0.8.0..1011230042 patch to use correct hadoop queue.

For example:

**job.properties**

.. code-block:: bash

   inputDir=hdfs://NN2:8020/projects/input-data
   outputDir=hdfs://NN2:8020/projects/output-demo


4. Add a new property to configuration
**************************************

For every oozie action that needs to refer to input/output on the second namenode, 
add this property to the action's configuration in workflow.xml

.. code-block:: xml

   <property>
    <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
    <value>hdfs://NN2:8020</value>
   </property>


5. Confirm that Oozie properties and XML tags are on the default namenode
*************************************************************************

- oozie.coord.application.path
- oozie.wf.application.path
- <name-node>
- <file>
- <archive>
- <sub-workflow><app-path>
- <job-xml>
- pipes action's <program>
- fs action <move source target>
- pig action's <script>


**************************************
**************************************




More info here.
Your KEYDB file will look something like this:
   #. 

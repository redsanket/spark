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


Java Action Copying a Local File to HDFS
----------------------------------------

Assume a local file ``${filename}`` can be accessed by all cluster nodes. 

For example, the file is located in the home directory, which is globally mounted in blue colo. 
All cluster nodes can read the local file by the same path, ``${filename}``.

#. Define a java action in your ``workflow.xml``:

.. code-block:: xml

   <action name='java5'>
       <java>
           <job-tracker>${jobTracker}</job-tracker>
           <name-node>${nameNode}</name-node>
           <configuration>
               <property>
                   <name>mapred.job.queue.name</name>
                   <value>${queueName}</value>
               </property>
           </configuration>
           <main-class>qa.test.tests.testCopyFromLocal</main-class>
           <arg>${filename}</arg>
           <arg>${nameNode}${testDir}</arg>
           <capture-output/>
       </java>
       <ok to="decision1" />
       <error to="fail" />
   </action>

#. Create your Java main class with the following:

.. code-block:: java

   package qa.test.tests;
   
   import org.apache.hadoop.fs.FileSystem;
   import org.apache.hadoop.fs.FSDataInputStream;
   import org.apache.hadoop.fs.FSDataOutputStream;
   import org.apache.hadoop.fs.Path;
   import org.apache.hadoop.conf.Configuration;
   
   import java.io.File;
   import java.io.FileNotFoundException;
   import java.io.FileOutputStream;
   import java.io.IOException;
   import java.io.OutputStream;
   import java.util.Calendar;
   import java.util.Properties;
   import java.util.Vector;
   
   public class testCopyFromLocal {
           public static void main (String[] args) throws IOException {
                   String src = args[0];
                   String dst = args[1];
                   System.out.println("testCopyFromLocal, source= " + src);
                   System.out.println("testCopyFromLocal, target= " + dst);
   
                   Configuration conf = new Configuration();
   
                   Path src1 = new Path(src);
                   Path dst1 = new Path(dst);
   
                   FileSystem fs = FileSystem.get(conf);
   
                   try{
                      //delete local file after copy
                      fs.copyFromLocalFile(true, true, src1, dst1);
                   }
                      catch(IOException ex) {
                      System.err.println("IOException during copy operation " + ex.toString());
                      ex.printStackTrace();
                      System.exit(1);
                    }
              }
   }



Java Action Printing a List of Dates
------------------------------------

The example below prints a list of dates, based on the given start date, end date, and frequency. The *end date* is not included.

#. Define a java action in your ``workflow.xml``.

.. code-block:: xml

   <action name='java_1'>
       <java>
           <job-tracker>${jobTracker}</job-tracker>
           <name-node>${nameNode}</name-node>
           <configuration>
               <property>
                   <name>mapred.job.queue.name</name>
                   <value>${queueName}</value>
               </property>
           </configuration>
           <main-class>org.apache.oozie.example.DateList</main-class>
           <!-- Usage: java DateList <start_time>  <end_time> <frequency> <timeunit> <timezone> -->
           <arg>${START}</arg>
           <arg>${END}</arg>
           <arg>${FREQUENCY}</arg>
           <arg>${TIMEUNIT}</arg>
           <arg>${TIMEZONE}</arg>
           <capture-output/>
       </java>
       <ok to="decision1" />
       <error to="fail" />
   </action>

#. Specify a ``wf:actionData`` function to refer to the output of the java action in the workflow. For example:o

   .. code-block:: xml

      <decision name="decision1">
          <switch>
              <case to="end">${(wf:actionData('java_1')['datelist'] == EXPECTED_DATE_RANGE)}</case>
              <default to="fail" />
          </switch>
      </decision>

#. Create an example of the ``job.property`` file:

   .. code-block:: bash

      oozie.wf.application.path=hdfs://gsbl90359.blue.ygrid.yahoo.com:8020/user/strat_ci/yoozie_test/workflows/test_w43-1
      nameNode=hdfs://gsbl90359.blue.ygrid.yahoo.com:8020
      jobTracker=gsbl90358.blue.ygrid.yahoo.com:50300
      queueName=grideng

      START=2011-03-07T01:00Z
      END=2011-03-07T02:00Z
      FREQUENCY=15
      TIMEUNIT=MINUTES
      TIMEZONE=UTC
      EXPECTED_DATE_RANGE=2011-03-07T01:00Z,2011-03-07T01:15Z,2011-03-07T01:30Z,2011-03-07T01:45Z

      mapreduce.jobtracker.kerberos.principal=mapred/_HOST@DEV.YGRID.YAHOO.COM
      dfs.namenode.kerberos.principal=hdfs/_HOST@DEV.YGRID.YAHOO.COM 

#. Create a Java main class:

   .. code-block:: java

      /**
       * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
       * Licensed under the Apache License, Version 2.0 (the "License");
       * you may not use this file except in compliance with the License.
       * You may obtain a copy of the License at
       *
       *   http://www.apache.org/licenses/LICENSE-2.0
       *
       *  Unless required by applicable law or agreed to in writing, software
       *  distributed under the License is distributed on an "AS IS" BASIS,
       *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       *  See the License for the specific language governing permissions and
       *  limitations under the License. See accompanying LICENSE file.
       */
      package org.apache.oozie.example;
      
      import java.io.File;
      import java.io.FileOutputStream;
      import java.io.OutputStream;
      import java.text.DateFormat;
      import java.text.SimpleDateFormat;
      import java.util.Calendar;
      import java.util.Date;
      import java.util.Properties;
      import java.util.TimeZone;
      
      public class DateList {
              private static final TimeZone UTC = getTimeZone("UTC");
              private static String DATE_LIST_SEPARATOR = ",";
      
              public static void main(String[] args) throws Exception {
                      if (args.length < 5) {
                              System.out
                                              .println("Usage: java DateList <start_time>  <end_time> <frequency> <timeunit> <timezone>");
                              System.out
                                              .println("Example: java DateList 2009-02-01T01:00Z 2009-02-01T02:00Z 15 MINUTES UTC");
                              System.exit(1);
                      }
                      Date startTime = parseDateUTC(args[0]);
                      Date endTime = parseDateUTC(args[1]);
                      Repeatable rep = new Repeatable();
                      rep.setBaseline(startTime);
                      rep.setFrequency(Integer.parseInt(args[2]));
                      rep.setTimeUnit(TimeUnit.valueOf(args[3]));
                      rep.setTimeZone(getTimeZone(args[4]));
                      Date date = null;
                      int occurrence = 0;
                      StringBuilder dateList = new StringBuilder();
                      do {
                              date = rep.getOccurrenceTime(startTime, occurrence++, null);
                              if (!date.before(endTime)) {
                                      break;
                              }
                              if (occurrence > 1) {
                                      dateList.append(DATE_LIST_SEPARATOR);
                              }
                              dateList.append(formatDateUTC(date));
                      } while (date != null);
      
                      System.out.println("datelist :" + dateList+ ":");
                      //Passing the variable to WF that could be referred by subsequent actions
                      File file = new File(System.getProperty("oozie.action.output.properties"));
                      Properties props = new Properties();
                      props.setProperty("datelist", dateList.toString());
                      OutputStream os = new FileOutputStream(file);
                      props.store(os, "");
                      os.close();
              }
      
              //Utility methods
              private static DateFormat getISO8601DateFormat() {
                      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
                      dateFormat.setTimeZone(UTC);
                      return dateFormat;
              }
      
              private static TimeZone getTimeZone(String tzId) {
                      TimeZone tz = TimeZone.getTimeZone(tzId);
                      if (!tz.getID().equals(tzId)) {
                              throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
                      }
                      return tz;
              }
      
              private static Date parseDateUTC(String s) throws Exception {
                      return getISO8601DateFormat().parse(s);
              }
              private static String formatDateUTC(Date d) throws Exception {
                      return (d != null) ? getISO8601DateFormat().format(d) : "NULL";
              }
      
              private static String formatDateUTC(Calendar c) throws Exception {
                      return (c != null) ? formatDateUTC(c.getTime()) : "NULL";
              }
      
      }



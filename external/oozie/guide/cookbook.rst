.. _cookbook:

Cookbook Examples
=================

.. 04/20/15: Rewrote

In this chapter, we will offer you examples
of how to complete common tasks for 
Workflows, Coordinators, and Bundles. 


Prerequisites
-------------

- Complete the :ref:`Getting Started <getting_started>`.

Accessing Hadoop Counters in Previous Actions
---------------------------------------------

In this example, we access a user-defined Hadoop counter in a subsequent action.
This is a simple way to pass data between different Oozie actions.

In the ``workflow.xml`` below, the ``'mr1'`` action generates a user-defined 
Hadoop counter named ``['COMMON']['COMMON.ERROR_ACCESS_DH_FILES']``.
The value of this counter is accessed in the subsequent ``'java1'`` action.

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

You can define a property in your action that uses ``mapred.child.java.opts``
that allows you to specify the memory usage.

Here's an example of the defined property that specifies
memory usage:

.. code-block:: xml

   <property>
       <name>mapred.child.java.opts</name>
       <value>-Xmx1024M</value>
       <description>Setting memory usage to 1024MB</description>
   </property>

Below is the ``workflow.xml`` that includes the defined property for
expanding memory usage:

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

After you create your class that accepts a custom input format, you can 
define a property in your action that uses that class as shown below.

.. code-block:: xml

   <property>
     <name>mapred.input.format.class</name>
     <value>com.yahoo.mycustominputformat.TextInputFormat</value>
   </property>

Workflow
~~~~~~~~

The Workflow XML file below uses the custom input class for
handling spam.

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

For an Oozie action to call a YCA-protected Web service, users have to specify the gYCA credential 
explicitly in the Workflow beginning and ask Oozie to retrieve the appropriate certificates.
In each ``credential`` element, the attribute ``name`` is the key and the attribute 
``type`` indicates which credential to use.

To use YCAv2 certificates, ensure that the following is true:

- The credential ``type`` is defined in Oozie server. For example, on ``axoniteblue-oozie.blue.ygrid.yahoo.com``, 
  the YCA credential type is defined as ``yca``, as in ``yoozie_conf_axoniteblue.axoniteblue_conf_oozie_credentials_credentialclasses: yca=com.yahoo.oozie.action.hadoop.YCAV2Credentials,howl=com.yahoo.oozie.action.hadoop.HowlCredentials,hcat=com.yahoo.oozie.action.hadoop.HowlCredentials``.
- User give multiple ``credential`` elements under ``credentials`` and specify a comma-separated list of credentials under each action's 
  ``cred`` attribute.
- There is only parameter required for the credential ``type``.

  - ``yca-role``: The role name contains the user names for YCA v2 certificates.
- There are three optional parameters for the credential type ``yca``.

  - ``yca-webserver-url``: The YCA server URL. The default is http://ca.yca.platform.yahoo.com:4080.
  - ``yca-cert-expiry``: The expiry time of the YCA certificate in seconds. The default is one day (86400) and available from Oozie 3.3.1.
  - ``yca-http-proxy-role``: The roles DB role name which contains the hostnames of 
    the machines in the HTTP proxy VIP. The default value is ``grid.httpproxy`` which contains 
    all HTTP proxy hosts. Depending on the HTTP proxy VIP you will be using to send 
    the obtained YCA v2 certificate to the Web service outside the grid, you can 
    limit the corresponding role name that contains the hosts of the HTTP proxy VIP. 
    The role names containing members of production http proxy VIPs are ``grid.blue.prod.httpproxy``, 
    ``grid.red.prod.httpproxy``, and ``grid.tan.prod.httpproxy``. For example: http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.prod.httpproxy
    contains the hosts of production ``httpproxy``: The role ``http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.httpproxy``
    is a uber role which contains staging, research, and production ``httpproxy`` hosts. 

    See http://twiki.corp.yahoo.com/view/Grid/HttpProxyNodeList 
    for the role name and VIP name of the deployed HTTP proxies for staging, research, and sandbox grids.

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

When Oozie action executor sees a ``cred`` attribute in the current action, depending 
on credential name given, it finds the appropriate credential class to retrieve 
the token or certificate and inserts it into action configuration for further use. 
In the example Workflow above, 
Oozie gets the certificate of gYCA and passed to action configuration. The mapper can then use 
this certificate by getting it from action configuration, and then add it to the HTTP request header 
when connecting to the YCA-protected Web service through HTTPProxy. A certificate or token 
retrieved by the credential class would set an action configuration as the name of 
credential defined in ``workflow.xml``. (In this example, it is ``'myyca'``.) 

The following examples shows sample code to 
use in mapper or reducer class for talking to YCAv2-protected Web service from grid.

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

One can pass parameters that are defined as a job property to EL functions.

For example, the parameters ``coord.start.instance`` and ``coord.end.instance``
are defined ``job.properties`` before the Oozie job is submitted.

.. code-block:: xml

   <input-events>
       <data-in name="zas_daily_datain" dataset="zas_daily_dataset">
           <start-instance>${coord:latest(coord.start.instance)}</start-instance>
           <end-instance>${coord:latest(coord.end.instance)}</end-instance>
       </data-in>
   </input-events>


Using Headless Users
--------------------

TBD: This needs to be updated to use Kerberos.

Oozie uses Backyard authentication. If you want to use a headless user, you need to 
do the following:

- Request a `Headless Bouncer account <http://twiki.corp.yahoo.com/view/SSO/HeadlessAccountSetup>`_. These accounts need a underscore "_" in their name. 
- Request a headless UNIX account, that matches the name of your headless Backyard account.

Follow the steps below to set up your headless user for Oozie:

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

For example, if the JobTracker is JT1, then the native (or default) NameNode is NN1,
If the JobTracker is JT2, then the second namenode is NN2.

2. Configure the Oozie job application path
*******************************************

The Oozie job application path, including ``coordinator.xml``, ``workflow.xml``, and ``lib``, needs to be on JobTracker's default namenode (i.e., NN1).
The default NameNode should be set to NN1.

For example:

Coordinator: **job.properties**

.. code-block:: bash

   oozie.coord.application.path=hdfs://{NN1}:8020/projects/test_sla2-4
   nameNode=hdfs://{NN1}:8020
   wf_app_path=hdfs://{NN1}:8020/projects/test_sla2-4/demo
   jobTracker={JT1}:50300

Workflow: **job.properties**

.. code-block:: bash

   oozie.wf.application.path=hdfs://{NN1}:8020/yoozie_test/workflows/pigtest
   nameNode=hdfs://{NN1}:8020
   jobTracker={JT1}:50300

3. Creating the Pig action
**************************

The pig script should be on NN1.
For pig 0.8, use the 0.8.0..1011230042 patch to use correct the Hadoop queue.

For example:

**job.properties**

.. code-block:: bash

   inputDir=hdfs://{NN2}:8020/projects/input-data
   outputDir=hdfs://{NN2}:8020/projects/output-demo


4. Add a new property to configuration
**************************************

For every Oozie action that needs to refer to input/output on the second NameNode, 
add this property to the action's configuration in ``workflow.xml``.

.. code-block:: xml

   <property>
    <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
    <value>hdfs://{NN2}:8020</value>
   </property>


5. Confirm that Oozie properties and XML tags are on the default NameNode
*************************************************************************

- ``oozie.coord.application.path``
- ``oozie.wf.application.path``
- ``<name-node>``
- ``<file>``
- ``<archive>``
- ``<sub-workflow><app-path>``
- ``<job-xml>``
- pipes action's ``<program>``
- Fs action <move source target>
- Pig action's ``<script>``


Java Action Copying a Local File to HDFS
----------------------------------------

To copy a local file to HDFS, the local file ``${filename}`` 
must be accessible by all cluster nodes. 

For example, the file is located in the home directory, which is globally mounted in blue colo. 
All cluster nodes can read the local file by the same path ``${filename}``.

#. Define a Java action in your ``workflow.xml``:

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

The example below prints a list of dates, based on the given start date, end date, 
and frequency. The *end date* is not included.

#. Define a Java action in your ``workflow.xml``.

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

#. Specify a ``wf:actionData`` function to refer to the output of the Java action in the Workflow XML. For example:

   .. code-block:: xml

      <decision name="decision1">
          <switch>
              <case to="end">${(wf:actionData('java_1')['datelist'] == EXPECTED_DATE_RANGE)}</case>
              <default to="fail" />
          </switch>
      </decision>

#. Create a ``job.property`` file defining the parameters shown below.

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

Using Oozie Maven Artifacts
---------------------------

If you have a Java Maven project which uses an Oozie client or core library, you can 
use Oozie Maven artifacts. Given below is the Maven repository and dependency 
settings for your POM file.

POM XML
~~~~~~~

.. code-block:: xml

   <repositories>
     <repository>
       <id>yahoo</id>
         <url>http://ymaven.corp.yahoo.com:9999/proximity/repository/public</url>
         <snapshots>
         <enabled>false</enabled>
         </snapshots>
     </repository>
   </repositories>
   <dependencies>
     <dependency>      
       <groupId>org.apache.oozie</groupId>
       <artifactId>yoozie-client</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
       </dependency>
     </dependency>
     <dependency>
       <groupId>org.apache.oozie</groupId>
       <artifactId>oozie-core</artifactId>
       <version>${oozie.version}</version>
       <classifier>tests</classifier>   
       <scope>compile</scope>
     </dependency>
     <dependency>
       <groupId>org.apache.oozie</groupId>
       <artifactId>oozie-core</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
     </dependency>
   <dependency>      
       <groupId>org.apache.oozie</groupId>
       <artifactId>yoozie-auth</artifactId>
       <version>${oozie.version}</version>
       <scope>compile</scope>
       </dependency>
     </dependency>
   </dependencies>         

Getting the Required Yinst Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alternately, you can also install following ``yoozie`` yinst packages to get the Oozie Jars and POM files.

::

    yinst i yoozie_maven -br stable
    yinst i yoozie_hadooplibs_maven -br stable
    yinst i yoozie_hbaselibs_maven -br stable
    yinst i yoozie_hcataloglibs_maven -br stable

.. note:: The ``current`` branch might also contain the version deployed on a research cluster. 
          Package is promoted to stable only when it is deployed on production.

Workflows
=========

.. 05/15/15: Edited.

.. _workflows-overview:

Overview
--------

Oozie Workflows are a collection of tasks known as *actions* that are organized 
in a control dependency DAG (Direct Acyclic Graph). The control dependency DAG 
determines the order of execution of the actions. You create the DAG through an XML file called
the Workflow definition, which is based on the Workflow XML Schema. Oozie executes 
an instance of your Workflow definition in its Workflow Engine (also known as the DAG engine).

Oozie Workflows can be parameterized (using variables like ``${inputDir}`` within the 
Workflow definition). When submitting a Workflow job values for the parameters must 
be provided. If properly parameterized (i.e., using different output directories) 
several identical workflow jobs can run concurrently.

We're going to look at the available actions and then 
examine Workflows examples.

.. _workflows-actions:

Actions
-------

Actions are the mechanism by which a Workflow triggers the execution of a 
computation or processing task. Oozie provides support for the following 
types of actions: 

- MapReduce
- Java
- Hive
- Pig
- Fs 
- Shell
- Email
- Sub-workflow
- Streaming
- DistCp
- Spark

Oozie can also be extended to support additional type of actions. 
We're going to take a look at each type of action in the sections below. 

.. _actions-mapreduce:

MapReduce
~~~~~~~~~

For a basic MapReduce example, we recommend that you 
see the external Oozie `MapReduce Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook>`_.

The example below using the new MapReduce API (version 0.23+).

.. _mapreduce-new_api:

New MapReduce API
*****************

To run MapReduce jobs using new API in Oozie, you need to do the following:

- change ``mapred.mapper.class to mapreduce.map.class``
- change ``mapred.reducer.class to mapreduce.reduce.class``
- add ``mapred.output.key.class``
- add ``mapred.output.value.class``
- include the following property in the configuration for the MapReduce action:

  .. code-block:: xml

     <property>
       <name>mapred.reducer.new-api</name>
       <value>true</value>
     </property>
     <property>
       <name>mapred.mapper.new-api</name>
       <value>true</value>
     </property>

.. _new_api-workflow:

Workflow for New MapReduce API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The example Workflow XML below shows you how you
would configure your Workflow to use the new MapReduce API.

.. code-block:: xml

   <map-reduce xmlns="uri:oozie:workflow:0.5">
     <job-tracker>gsbl90392.blue.ygrid.yahoo.com:8032</job-tracker>
     <name-node>hdfs://gsbl90390.blue.ygrid.yahoo.com:8020</name-node>
     <prepare>
       <delete path="hdfs://gsbl90390.blue.ygrid.yahoo.com:8020/user/mchiang/yoozie_test/output-mr20-fail" />
     </prepare>
     <configuration>
       <property>
         <name>mapred.mapper.new-api</name>
         <value>true</value>
       </property>
       <property>
         <name>mapred.reducer.new-api</name>
         <value>true</value>
       </property>
       <property>
         <name>mapreduce.map.class</name>
         <value>org.apache.hadoop.examples.WordCount$TokenizerMapper</value>
       </property>
       <property>
         <name>mapreduce.reduce.class</name>
         <value>org.apache.hadoop.examples.WordCount$IntSumReducer</value>
       </property>
       <property>
         <name>mapred.output.key.class</name>
         <value>org.apache.hadoop.io.Text</value>
       </property>
       <property>
         <name>mapred.output.value.class</name>
         <value>org.apache.hadoop.io.IntWritable</value>
       </property>
       <property>
         <name>mapred.map.tasks</name>
         <value>1</value>
       </property>
       <property>
         <name>mapred.input.dir</name>
         <value>/user/mchiang/yoozie_test/input-data</value>
       </property>
       <property>
         <name>mapred.output.dir</name>
         <value>/user/mchiang/yoozie_test/output-mr20/mapRed20</value>
       </property>
       <property>
         <name>mapred.job.queue.name</name>
         <value>grideng</value>
       </property>
       <property>
         <name>mapreduce.job.acl-view-job</name>
         <value>*</value>
       </property>
       <property>
         <name>oozie.launcher.mapreduce.job.acl-view-job</name>
         <value>*</value>
       </property>
     </configuration>
   </map-reduce>

.. _actions-java:

Java Action
~~~~~~~~~~~

In addition to the below example, we suggest you also see the external Oozie
`Java Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Java%20Cookbook>`_.

.. _java-workflow:

Workflow XML
************

Define a Java XML element in your ``workflow.xml``, 
specifying the NameNode, JobTracker, the Hadoop queue,
the Java main class, and an output directory if there's output.

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.5' name='java-wf'>
     <start to='java1' />
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
         <arg>${wf:conf('outputDir')}/pig-output1/part-00000</arg>
         <capture-output/>
       </java>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _java-main_class:

Java main Class
***************

The sample class ``org.apache.oozie.test.MyTest`` should be packaged in a JAR file 
and put in your Workflow ``lib`` directory.

Here's the sample Java ``main`` class.

.. code-block:: java

   package org.apache.oozie.test;

   import java.io.*;
   import java.util.Properties;
   
   public class MyTest {
      
     ////////////////////////////////
     // Do whatever you want in here
     ////////////////////////////////
     public static void main (String[] args)
     {
       String fileName = args[0];
       try {
         File file = new File(System.getProperty("oozie.action.output.properties"));
         Properties props = new Properties();
            
         OutputStream os = new FileOutputStream(file);
         props.store(os, "WORKING");
         os.close();
         System.out.println(file.getAbsolutePath()); 
       }
       catch (Exception e) {
         e.printStackTrace();
       }
     }
   }

.. _java-perl:

Create Java Action Using Perl Script
************************************

Define a Java action in your ``workflow.xml``:

.. code-block:: xml

   <action name='java2'>
     <java>
       <job-tracker>${jobTracker}</job-tracker>
       <name-node>${nameNode}</name-node>
       <configuration>
         <property>
           <name>mapred.job.queue.name</name>
           <value>${queueName}</value>
         </property>
       </configuration>
       <main-class>qa.test.tests.testShell</main-class>
       <arg>./test.pl</arg>
       <arg>WORLD</arg>
       <file>${testDir}/test.pl#test.pl</file>
       <file>${testDir}/DatetimeHlp.pm#DatetimeHlp.pm</file>
       <capture-output/>
     </java>
     <ok to="decision1" />
     <error to="fail" />
   </action>

.. _java-perl_wrapper:

Write Java Wrapper for Perl Script
**********************************

For the wrapper, you would Upload the Perl script (``test.pl``) 
and Perl module (``DatetimeHlp.pm``) to the ``oozie.wf.application.path`` 
directory on HDFS. Also, the ``main`` class should be packaged 
in a JAR file and uploaded to ``oozie.wf.application.path/lib`` directory.

Here's the sample Java ``main`` class:

.. code-block:: java

   package qa.test.tests;
   import qa.test.common.*;
   import java.io.*;
   import java.util.*;
   public class testShell {

     public static void main (String[] args) {
         
       String cmdfile = args[0];
       String text = args[1];
       try {
         String runCmd1;
         runCmd1 = cmdfile +" "+text;
         System.out.println("Command: "+runCmd1);
         CmdRunner cr1 = new CmdRunner(runCmd1);
         Vector v1  = cr1.run();
         String l1  = ((String) v1.elementAt(0));
         System.out.println("Output: "+l1);
         String s2 = "HELLO WORLD Time:";
         File file = new File(System.getProperty("oozie.action.output.properties"));
         Properties props = new Properties();
         if (l1.contains(s2)) {
           props.setProperty("key1", "value1");
           props.setProperty("key2", "value2");
         } else {
           props.setProperty("key1", "novalue");
           props.setProperty("key2", "novalue");
         }
         OutputStream os = new FileOutputStream(file);
         props.store(os, "");
         os.close();
         System.out.println(file.getAbsolutePath());
       } catch (Exception e) {
         e.printStackTrace();
       } finally {
         System.out.println("Done.");
       }
     }
   }

.. _action-hive:

Hive Action
~~~~~~~~~~~

See the `Hive Action <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_HiveActionExtension.html>`_
documentation on the Kryptonite Red cluster, which also includes the Hive schema for v0.2 to v0.5.
 
.. _action-pig:

Pig Action
~~~~~~~~~~

See the `Pig Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Pig+Cookbook>`_ in the Apache documentation. 

.. note:: The following options for Pig actions are not supported at Yahoo:

          - ``-4 (-log4jconf)``
          - ``-e (-execute)`` 
          - ``-f (-file)``
          - ``-l (-logfile)``
          - ``-r (-dryrun)``
          - ``-x (-exectype)``
          - ``-P (-propertyFile)``


.. _pig-udf:

Using UDFs (User Defined Functions)
***********************************

**Summary Table for Cases**
 
.. csv-table:: Use Cases for UDFs in Pig Actions
   :header: "", "``udf.jar`` in Workflow ``lib`` Directory?", "Registered in the Pig Script?", "``udf.jar`` in File?", "``udf.jar`` in Archive?"
   :widths: 20, 30, 15, 15, 15 

   "Case 1", "Yes", "No", "No", "No"
   "Case 2", "No (must use a different directory other than ``lib``)", "Yes", "Yes", "No"
   "Case 3", "No (must use a different directory other than ``lib``)", "Yes", "No", "Yes"


.. _pig_udf-basic:

Use Case One: Basic Pig Script
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first use case simply reads input, processes that input, and then writes
the date to an output directory. We're also defining to
variables in the Workflow XML that are used in the Pig script.

.. _basic-ex:

Example Pig Script
++++++++++++++++++

The simple Pig script below loads a text file, capitalizes the ``name`` string, and
writes the data to file.

.. code-block:: bash

   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();

.. _pig_basic-workflow:

Example Workflow
++++++++++++++++

The Pig action must point to the path containing the Pig script
as shown in the ``<script>`` element and define the input and
output directories if data is being read and written.

.. code-block:: xml

   <action name='pig2'>
     <pig>
       <job-tracker>${jobTracker}</job-tracker>
       <name-node>${nameNode}</name-node>
       <configuration>
         <property>
           <name>mapred.job.queue.name</name>
           <value>${queueName}</value>
         </property>
         <property>
           <name>mapred.compress.map.output</name>
           <value>true</value>
         </property>
       </configuration>
       <script>org/apache/oozie/examples/pig/script.pig</script>
       <param>INPUT=${inputDir}</param>
       <param>OUTPUT=${outputDir}/pig-output2</param>
     </pig>
     <ok to="decision1" />
     <error to="fail" />
   </action>

.. _pig_use_case-custom_jar:

Use Case 2: Using a Custom JAR
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In this example use case, we're putting a custom JAR in the HDFS directory 
in addition to the Workflow ``lib`` directory. The location of the JAR needs to
be specified in the ``<file>`` element in ``workflow.xml`` and registered in the Pig script.

Also, the specified path in ``workflow.xml`` must include the symlink (e.g., ``#udf.jar``),
otherwise an error will occur. The symlink ensures that the TaskTracker creates 
a symlink in the current working directory of the Pig client (on the launcher mapper);
without the symlink, the Pig client cannot find the UDF JAR file.

.. _custom_jar-script:

Pig Script
++++++++++

We use the same Pig script essentially with the addition of registering the
``udf.jar`` JAR file.

.. code-block:: bash

   REGISTER udf.jar
   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();

.. _custom_jar-workflow:

Workflow XML
++++++++++++

In this ``workflow.xml``, in addition to using the ``<script>`` element to point
to the path of the Pig script, you specify the path to the JAR file in
the ``<file>`` element.

.. code-block:: xml

   <action name='pig2'>
     <pig>
       <job-tracker>${jobTracker}</job-tracker>
       <name-node>${nameNode}</name-node>
       <configuration>
         <property>
           <name>mapred.job.queue.name</name>
           <value>${queueName}</value>
         </property>
         <property>
           <name>mapred.compress.map.output</name>
           <value>true</value>
         </property>
       </configuration>
       <script>org/apache/oozie/examples/pig/script.pig</script>
       <param>INPUT=${inputDir}</param>
       <param>OUTPUT=${outputDir}/pig-output2</param>
       <file>/tmp/tutorial-udf.jar#udf.jar</file>
     </pig>
     <ok to="decision1" />
     <error to="fail" />
   </action>

.. _action-streaming:

Streaming Action
~~~~~~~~~~~~~~~~

The following example of a Streaming action simply 
takes output from ``cat`` and then counts the lines, 
words, and bytes. The count is then written to an
output directory. 

.. _streaming-output:

Workflow XML
************

In the ``workflow.xml`` below, the output from the reducer ``wc`` will be written 
to ``${outputDir}/streaming-output``. The Streaming action pipes output from a
mapper to a reducer with ``org.apache.hadoop.streaming.PipeMapRunner``.

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
             <name>mapred.map.runner.class</name>
             <value>org.apache.hadoop.streaming.PipeMapRunner</value>
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

.. _action-fs:

Fs Action
~~~~~~~~~

If you wanted to recursively change the permissions of a directory and its contents,
you would run the following HDFS command: ``$ hdfs dfs -chmod -R 766 <dir>;``

The following ``workflow.xml``, like the command above, recursively changes the permissions
of a directory:

.. code-block:: xml

   <workflow-app name="sample-wf" xmlns="uri:oozie:workflow:0.5">
     ...
     <action name="hdfscommands">
       <fs>
         <delete path='hdfs://foo:8020/usr/tucu/temp-data'/>
         <mkdir path='archives/${wf:id()}'/>
         <move source='${jobInput}' target='archives/${wf:id()}/processed-input'/>
         <chmod path='${jobOutput}' permissions='-rwxrw-rw-' dir-files='true'><recursive/></chmod>
         <chgrp path='${jobOutput}' group='testgroup' dir-files='true'><recursive/></chgrp>
       </fs>
       <ok to="myotherjob"/>
       <error to="errorcleanup"/>
     </action>
     ...
   </workflow-app>


See `Fs HDFS Action <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a3.2.4_Fs_HDFS_action>`_
for more detailed information and an additional examples.

.. note:: You can also recursively change permissions in a Pig script. For example,
          a Pig script could have the command ``hdfs dfs -chmod -R 766 <dir>;``.


.. _action-shell:

Shell Action
~~~~~~~~~~~~

Using Global Section
********************

To use the global section in your Oozie workflow for defining configuration 
parameters applicable to all actions, specifically shell actions, use
the latest shell XML namespace 0.3 as shown below.

.. code-block:: xml

   <workflow-app name="wf_app" xmlns="uri:oozie:workflow:0.4">
     <global>
       <job-tracker>${JT}</job-tracker>
       <name-node>${NN}</name-node>
       <configuration>
         <property>
           <name>mapred.job.queue.name</name>
           <value>${JQ}</value>
         </property>
       </configuration>
     </global>    
     <start to="action1"/>
     <action name="action1>
       <shell xmlns="uri:oozie:shell-action:0.3"> 
         <!-- Action xmlns version GOES HERE -->
         <exec>python</exec>
         <argument>...</argument>
         ...
       </shell>
     </action>
     ...
   </workflow>

.. _action-email:

Email Action
~~~~~~~~~~~~

The example Email action below sends a message with a subject,
body, and the sender's address. 

.. code-block:: xml

   <action name="email_notification" cred="">
     <email xmlns="uri:oozie:email-action:0.1">
       <to>someyahoo@yahoo-inc.com</to>
       <subject>Oozie Workflow Example</subject>
       <body>This is a sample email</body>
     </email>
     <ok to="end"/>
     <error to="kill"/>
   </action>

See `Oozie Email Action Extension <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_EmailActionExtension.html>`_ 
for the syntax, an example, and the schema for the Email action.

.. note:: To send email to an iList, the iList setting needs to allow posts 
          from non-members (set iList to **public list (open)**). No configuration 
          changes are needed when sending to individual account.

.. _action-subflow:

Sub-workflow Action
~~~~~~~~~~~~~~~~~~~

See `Oozie Sub-Workflow Action Extension <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a3.2.6_Sub-workflow_Action>`_
for the syntax, an example, and the schema for the Sub-workflow action.

.. _action-distcp:

DistCp Action
~~~~~~~~~~~~~

`DistCp <https://hadoop.apache.org/docs/r1.2.1/distcp.html>`_ is a tool used for large inter/intra-cluster copying.
The following ``workflow.xml`` copies a bzipped file to a user's home directory. 

.. code-block:: xml

   <workflow-app name="hue_tutorial_workflow" xmlns="uri:oozie:workflow:0.4">
     <start to="copy_dataset"/>
     <action name="copy_data" cred="hcat">
       <distcp xmlns="uri:oozie:distcp-action:0.1">
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
             <value>${sourceNameNode}</value>
           </property>
         </configuration>
         <arg>${sourceNameNode}/tmp/dataset.bz2</arg>
         <arg>${nameNode}/user/yhoo_star/</arg>
       </distcp>
       <ok to="del_db_tables"/>
       <error to="kill"/>
     </action>
     <kill name="kill">
       <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name="end"/>
   </workflow-app>


.. _action-spark:

Spark Action
~~~~~~~~~~~~

- See the `Spark Action <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_SparkActionExtension.html>`_ in the Apache documentation. 
- To pick up spark-assembly.jar along with some standard jars/files, use: 

.. code-block:: xml

    <property>
        <name>oozie.action.sharelib.for.spark</name>
        <value>spark_current</value>
    </property>

- Please *DO NOT* include the jars which are present under spark_current on your own. That will 
  cause conflicts and exceptions. You can always check what is included in the sharelib using
  `this <http://jetblue-oozie.blue.ygrid.yahoo.com:4080/oozie/v2/admin/list_sharelib?lib=spark_current>`_
  cluster specific url.
- Only yarn is supported as master with cluster as mode; yarn as master with client as mode is not recommended.
- For accessing HBase tables, include following sharelibs

.. code-block:: xml

    <property>
        <name>oozie.action.sharelib.for.spark</name>
        <value>spark_current,hbase_current,hbase_conf_reluxred</value>
    </property>

.. note:: If your jar is packaged with hadoop jars whose version is not compatible with the hadoop jars present on the cluster, 

          - Create new jar for your job by excluding hadoop jars **OR**
          - Include following properties in the workflow configuaration 

             .. code-block:: xml

               <property>
                  <name>oozie.launcher.mapreduce.task.classpath.user.precedence</name>
                  <value>false</value>
               </property>
               <property >
                  <name>oozie.launcher.mapreduce.user.classpath.first</name>
                  <value>false</value>
               </property>


Running Java/Scala Code
***********************


.. code-block:: xml

   ......
   ......
   <action name='spark-node'>
      <spark xmlns="uri:oozie:spark-action:0.2">
          <configuration>
              <property>
                  <name>oozie.action.sharelib.for.spark</name>
                  <value>spark_current</value>
              </property>
          </configuration>
          <master>yarn</master>
          <mode>cluster</mode>
          <name>Spark-FileCopy</name>
          <class>org.apache.oozie.example.SparkFileCopy</class>
          <jar>hdfs://jetblue-nn1.blue.ygrid.yahoo.com:8020/tmp/examples/apps/spark/lib/oozie-examples.jar</jar>
          <spark-opts>--conf spark.my.configuration.name=value --queue default</spark-opts>
          <arg>hdfs://jetblue-nn1.blue.ygrid.yahoo.com:8020/tmp/examples/input-data/text/data.txt</arg>
          <arg>hdfs://jetblue-nn1.blue.ygrid.yahoo.com:8020/tmp/examples/output-data/spark</arg>
      </spark>
      <ok to="end" />
      <error to="fail" />
   </action>
   ......
   ......

Running PySpark Script
**********************


.. code-block:: xml

   ......
   ......
   <action name='spark-node'>
      <spark xmlns="uri:oozie:spark-action:0.2">
          <configuration>
              <property>
                    <name>oozie.action.sharelib.for.spark</name>
                  <value>spark_current</value>
              </property>
          </configuration>
          <master>yarn</master>
          <mode>cluster</mode>
          <name>spark-pyspark</name>
          <jar>hdfs://jetblue-nn1.blue.ygrid.yahoo.com:8020/tmp/examples/apps/spark-pyspark/lib/pi.py</jar>
          <spark-opts>--conf spark.my.configuration.name=value --queue default</spark-opts>
      </spark>
      <ok to="end" />
      <error to="fail" />
   </action>
   ......
   ......

Spark SQL Accessing Hive
************************


.. code-block:: xml

   ......
   ......
   <credentials>
        <credential name='hcatauth' type='hcat'>
            <property>
              <name>hcat.metastore.uri</name>
              <!-- Remember to put in your cluster specific values -->
              <value>thrift://jetblue-hcat.ygrid.vip.gq1.yahoo.com:50513</value>
            </property>
            <property>
              <name>hcat.metastore.principal</name>
              <!-- Remember to put in your cluster specific values -->
              <value>hcat/jetblue-hcat.ygrid.vip.gq1.yahoo.com@YGRID.YAHOO.COM</value>
            </property>
         </credential>
   </credentials>
   <action name='spark-node' cred='hcatauth'>
      <spark xmlns="uri:oozie:spark-action:0.2">
          <configuration>
               <property>
                  <name>oozie.action.sharelib.for.spark</name>
                  <value>spark_current</value>
               </property>
          </configuration>
          <master>yarn</master>
          <mode>cluster</mode>
          <name>Spark-Hive</name>
          <class>org.apache.spark.examples.sql.hive.HiveFromSpark</class>
          <jar>hdfs://jetblue-nn1.blue.ygrid.yahoo.com:8020/tmp/examples/apps/spark/lib/spark-examples.jar</jar>
          <spark-opts>--conf spark.my.configuration.name=value --queue default</spark-opts>
      </spark>
      <ok to="end" />
      <error to="fail" />
   </action>
   ......
   ......

Getting YCA Certificate
***********************
Please refer to :ref:`examples here<submit_ycav2-java_code_ex>`.


.. _workflow-examples:

Workflow Examples
-----------------

The following sections provide examples of complete Workflow XML files
for different actions.

.. _workflow-mr:

Map Reduce Action
~~~~~~~~~~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='map-reduce-wf'>
     <start to='hadoop1' />
     <action name='hadoop1'>
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
             <name>mapred.map.tasks</name>
             <value>1</value>
           </property>
           <property>
             <name>mapred.input.dir</name>
             <value>input-data</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>output-map-reduce</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>unfunded</value>
           </property>
         </configuration>
       </map-reduce>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _workflow-pig:

Pig Action
~~~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='pig-wf'>
     <start to='pig1' />
     <action name='pig1'>
       <pig>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>mapred.compress.map.output</name>
             <value>true</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>unfunded</value>
           </property>
         </configuration>
         <script>org/apache/oozie/examples/pig/id.pig</script>
         <param>INPUT=input-data</param>
         <param>OUTPUT=output-data-pig/pig-output</param>
       </pig>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _pig_workflow-udfs:

PIG Action with UDFs
********************

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='pig-wf'>
     <action name="pig_1">
       <pig>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <prepare>
           <delete path="${nameNode}${outputDir}/pig_1" />
         </prepare>
         <configuration>
           <property>
             <name>mapred.map.output.compress</name>
             <value>false</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
           <!-- optional -->
           <property>
             <name>mapred.child.java.opts</name>
             <value>-server -Xmx1024M -Djava.net.preferIPv4Stack=true -Dtest=QA</value>
           </property>
         </configuration>
         <script>org/apache/oozie/example/pig/script.pig</script>
         <param>INPUT=${inputDir}</param>
         <param>OUTPUT=${outputDir}/pig_1</param>
         <file>archivedir/tutorial-udf.jar#udfjar</file>
       </pig>
       <ok to="end" />
       <error to="fail" />
     </action>
   </workflow-app>

.. _pig_workflow-script:

Pig Script
**********

.. code-block:: bash

   REGISTER udfjar/tutorial-udf.jar;
   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage(); 


.. _action-streaming:

Streaming Action
~~~~~~~~~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='streaming-wf'>
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

.. _workflow-subworkflow:

Sub-workflow Action
~~~~~~~~~~~~~~~~~~~

.. _subworkflow-config:

Configuration Files
*******************

Add ``oozie_url`` of the ``sub-workflow`` to the job configuration XML:

.. code-block:: xml

   <configuration>
      <property>
         <name>oozie</name>
         <!-- OOZIE_URL -->
         <value>http://localhost:4080/oozie</value> 
      </property>
   </configuration>

You can also use the following in the ``job.properties`` file::

    oozie=http://localhost:4080/oozie

.. note:: If the ``sub-workflow`` runs in different Oozie server, add this property 
          to the configuration of action ``sub-workflow`` in ``workflow.xml``.
          
.. _subworkflow-workflow:

Workflow XML
************


.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='subwf'>
     <start to='subwf1' />
     <action name='subwf1'>
       <sub-workflow>
         <app-path>${nameNode}/tmp/${wf:user()}/workflows/map-reduce</app-path>
         <propagate-configuration/>
         <configuration>
           <property>
             <name>jobTracker</name>
             <value>${jobTracker}</value>
           </property>
           <property>
             <name>nameNode</name>
             <value>${nameNode}</value>
           </property>
           <property>
             <name>mapred.mapper.class</name>
             <value>org.apache.oozie.example.SampleMapper</value>
           </property>
           <property>
             <name>mapred.reducer.class</name>
             <value>org.apache.oozie.example.SampleReducer</value>
           </property>
           <property>
             <name>mapred.map.tasks</name>
             <value>1</value>
           </property>
           <property>
             <name>mapred.input.dir</name>
             <value>${inputDir}</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>${outputDir}/mapRed</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
       </sub-workflow>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Sub workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _action-java_main:

Java-Main Action
~~~~~~~~~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='java-main-wf'>
     <start to='java1' />
     <action name='java1'>
       <java>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>mapred.job.queue.name</name>
             <value>default</value>
           </property>
         </configuration>
         <main-class>org.apache.oozie.example.DemoJavaMain</main-class>
         <arg>argument1</arg>
         <arg>argument2</arg>
       </java>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Java failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _workflow_java-main:

Java-Main Action With Script Support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Java-Main action could be use to run a Perl or any shell script. In this example, a 
Perl script ``test.pl`` that uses the Perl module ``DatetimeHlp.pm``.

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='java-script-wf'>
     <start to='java2' />
     <action name='java2'>
       <java>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
         <main-class>qa.test.tests.testShell</main-class>
         <arg>./test.pl</arg>
         <arg>WORLD</arg>
         <file>/tmp/${wf:user()}/test.pl#test.pl</file>
         <file>/tmp/${wf:user()}/DatetimeHlp.pm#DatetimeHlp.pm</file>
         <capture-output/>
       </java>
       <ok to="decision1" />
       <error to="fail" />
     </action>
     <decision name="decision1">
       <switch>
         <case to="end">${(wf:actionData('java2')['key1'] == "value1") and (wf:actionData('java2')['key2'] == "value2")}</case>
         <default to="fail" />
       </switch>
     </decision>
     <kill name="fail">
       <message>Java failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _java_main-program:

Java Program
~~~~~~~~~~~~

The corresponding java class is shown below.

.. code-block:: java

   package qa.test.tests;
   import qa.test.common.*;
   import java.io.File;
   import java.io.FileNotFoundException;
   import java.io.FileOutputStream;
   import java.io.IOException;
   import java.io.OutputStream;
   import java.util.Calendar;
   import java.util.Properties;
   import java.util.Vector;
   
   
   public class testShell {
      
     public static void main (String[] args)
     {
       String cmdfile = args[0];
       String text = args[1];
   
       try{
         String runCmd1;
         runCmd1         = cmdfile +" "+text;
         System.out.println("Command: "+runCmd1);
         CmdRunner cr1 = new CmdRunner(runCmd1);
         Vector    v1  = cr1.run();
         String    l1  = ((String) v1.elementAt(0));
         System.out.println("Output: "+l1);
 
         String s2 = "HELLO WORLD Time:";
         File file = new File(System.getProperty("oozie.action.output.properties"));
         Properties props = new Properties();
   
         if (l1.contains(s2)) {
           props.setProperty("key1", "value1");
           props.setProperty("key2", "value2");
         } else {
           props.setProperty("key1", "novalue");
           props.setProperty("key2", "novalue");
         }
   
         OutputStream os = new FileOutputStream(file);
         props.store(os, "");
         os.close();
         System.out.println(file.getAbsolutePath());
       }
   
       catch (Exception e) {
         e.printStackTrace();
       } finally {
         System.out.println("Done.");
       }
     }
   }


.. _actions-multiple:

Multiple Actions
~~~~~~~~~~~~~~~~

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.1' name='demo-wf'>
     <start to="map_reduce_1" />
     <action name="map_reduce_1">
       <map-reduce>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>mapred.mapper.class</name>
             <value>org.apache.oozie.example.DemoMapper</value>
           </property>
           <property>
             <name>mapred.mapoutput.key.class</name>
             <value>org.apache.hadoop.io.Text</value>
           </property>
           <property>
             <name>mapred.mapoutput.value.class</name>
             <value>org.apache.hadoop.io.IntWritable</value>
           </property>
           <property>
             <name>mapred.reducer.class</name>
             <value>org.apache.oozie.example.DemoReducer</value>
           </property>
           <property>
             <name>mapred.map.tasks</name>
             <value>1</value>
           </property>
           <property>
             <name>mapred.input.dir</name>
             <value>${inputDir}</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>${outputDir}/mapred_1</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
       </map-reduce>
       <ok to="fork_1" />
       <error to="fail_1" />
     </action>
     <fork name='fork_1'>
       <path start='hdfs_1' />
       <path start='hadoop_streaming_1' />
     </fork>
     <action name="hdfs_1">
       <fs>
         <mkdir path="${nameNode}/tmp/${wf:user()}/hdfsdir1" />
       </fs>
       <ok to="join_1" />
       <error to="fail_1" />
     </action>
     <action name="hadoop_streaming_1">
       <map-reduce>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <prepare>
           <delete path="${nameNode}/tmp/${wf:user()}/hdfsdir1" />
         </prepare>
         <streaming>
           <mapper>/bin/cat</mapper>
           <reducer>/usr/bin/wc</reducer>
         </streaming>
         <configuration>
           <property>
             <name>mapred.input.dir</name>
             <value>${outputDir}/mapred_1</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>${outputDir}/streaming</value>
           </property>
         </configuration>
       </map-reduce>
       <ok to="join_1" />
       <error to="fail_1" />
     </action>
     <join name='join_1' to='pig_1' />
     <action name="pig_1">
       <pig>
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <configuration>
           <property>
             <name>mapred.map.output.compress</name>
             <value>false</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
         <script>org/apache/oozie/examples/pig/id.pig</script>
         <param>INPUT=${outputDir}/mapred_1</param>
         <param>OUTPUT=${outputDir}/pig_1</param>
       </pig>
       <ok to="end_1" />
       <error to="fail_1" />
     </action>
     <kill name="fail_1">
       <message>Demo workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name="end_1" />
   </workflow-app>

.. _workflow-sla:

Workflow Job to Create SLA events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Workflow job could be configured to record the events required to evaluate SLA compliance.
See `Configuring SLA in Applications <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/DG_SLAMonitoring.html>`_
for more information.

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.4'  xmlns:sla="uri:oozie:sla:0.2" name='map-reduce-wf'>
     <start to='hadoop1' />
     <action name='hadoop1'>
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
             <name>mapred.map.tasks</name>
             <value>1</value>
           </property>
           <property>
             <name>mapred.input.dir</name>
             <value>${inputDir}</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>${outputDir}/mapRed</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
       </map-reduce>
       <ok to="end" />
       <error to="fail" />
     </action>
     <kill name="fail">
       <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
     <sla:info> 
       <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time> 
       <sla:should-start>5</sla:should-start> 
       <sla:should-end>120</sla:should-end> 
       <sla:alert-contact>abc@yahoo.com</sla:alert-contact> 
       <sla:alert-events>start_miss,end_miss,duration_miss</sla:alert-events>
       <sla:max-duration>${2 * HOURS}</sla:max-duration>
     </sla:info>
   </workflow-app>

.. _workflow_sla-explanation:

Explanation of Workflow
***********************

Each workflow job will create at least three events for normal processing.
The event ``CREATED`` specifies that the Workflow job is registered for SLA tracking.
When the job starts executing, an event record of type ``STARTED`` is inserted into ``sla_event`` table.
Finally, when a job finishes, event of type either ``SUCCEEDED``, ``KILLED``, ``FAILED`` is generated.

.. _workflow-create_sla_event:

Workflow Action to Create SLA Events
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Workflow action could be configured to record the events required to evaluate 
SLA compliance.

.. code-block:: xml

   <workflow-app xmlns='uri:oozie:workflow:0.4'  xmlns:sla="uri:oozie:sla:0.2" name='map-reduce-wf'>
     <start to='hadoop1' />
     <action name='hadoop1'>
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
             <name>mapred.map.tasks</name>
             <value>1</value>
           </property>
           <property>
             <name>mapred.input.dir</name>
             <value>${inputDir}</value>
           </property>
           <property>
             <name>mapred.output.dir</name>
             <value>${outputDir}/mapRed</value>
           </property>
           <property>
             <name>mapred.job.queue.name</name>
             <value>${queueName}</value>
           </property>
         </configuration>
       </map-reduce>
       <ok to="end" />
       <error to="fail" />
       <sla:info> 
         <sla:nominal-time>2009-03-06T10:00Z</sla:nominal-time> 
         <sla:should-start>${10 * MINUTES}</sla:should-start> 
         <sla:should-end>${1 * HOURS}</sla:should-end> 
         <sla:alert-contact>abc@yahoo.com</sla:alert-contact> 
         <sla:alert-events>start_miss, end_miss</sla:alert-events>
         <sla:max-duration>${2 * HOURS}</sla:max-duration>
       </sla:info>
     </action>
     <kill name="fail">
       <message>Map/Reduce failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
     </kill>
     <end name='end' />
   </workflow-app>

.. _create_sla_event-explanation:

Explanation of the Workflow
***************************

Each workflow job will create at least three events for normal processing.
The event ``CREATED`` specifies that the Workflow action is registered for SLA tracking.
When the action starts executing, an event record of type ``STARTED`` is inserted into 
the ``sla_event`` table. Finally when an action finishes, event of type either 
``SUCCEEDED``, ``KILLED``, ``FAILED`` is generated.

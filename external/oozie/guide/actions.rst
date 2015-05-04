Actions
=======

.. 04/15/15: Copy edited section.
.. TBD: Need to act links for each action to the public Oozie docs.

Overview
--------

Actions are the mechanism by which a Workflow triggers the execution of a 
computation or processing task. Oozie provides support for the following 
types of actions: 

- MapReduce
- Java
- Hive
- Pig
- Fs 
- Shell
- Ssh
- Email
- Sub-workflow
- Streaming
- DistCp

Oozie can also be extended to support additional type of actions. 
We're going to take a look at each type of action in the sections below. 

MapReduce
---------

For a basic MapReduce example, we recommend that you 
see the external Oozie `MapReduce Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook>`_.

The example below using the new MapReduce API (version 0.23+).

New MapReduce API
~~~~~~~~~~~~~~~~~

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

Workflow for New MapReduce API
******************************

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



Java Action
-----------

In addition to the below example, we suggest you also see the external Oozie
`Java Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Java%20Cookbook>`_.

Workflow
~~~~~~~~

Define a Java XML block in your ``workflow.xml``.

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

Java main Class
~~~~~~~~~~~~~~~

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

Create Java Action Using Perl Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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


Write Java Wrapper for Perl Script
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upload the Perl script (``test.pl``) and Perl module (``DatetimeHlp.pm``) 
to the ``oozie.wf.application.path`` directory on HDFS. The ``main`` 
class should be packaged in a JAR file and uploaded to  
``oozie.wf.application.path/lib`` directory.

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

Hive Action
-----------

See the `Hive Action <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_HiveActionExtension.html>`_
documentation on the Kryptonite Red cluster, which also includes the Hive schema for v0.2 to v0.5.
 

Pig Action
----------


See the `Pig Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Pig+Cookbook>`_ in the Apache documentation. 

.. note:: The following options for Pig actions are not supported:

          - ``-4 (-log4jconf)``
          - ``-e (-execute)`` 
          - ``-f (-file)``
          - ``-l (-logfile)``
          - ``-r (-dryrun)``
          - ``-x (-exectype)``
          - ``-P (-propertyFile)``


Using UDFs (User Defined Functions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Summary Table for Cases**
 
.. csv-table:: Use Cases for UDFs in Pig Actions
   :header: "", "``udf.jar`` in Worklow ``lib`` Directory?", "Register in Pig Script?", "``udf.jar`` in File?", "``udf.jar`` in Archive?"
   :widths: 15, 10, 30

   "Case 1", "Yes", "No", "No", "No"
   "Case 2", "No (must use a different directory other than ``lib``)", "Yes", "Yes", "No"
   "Case 3", "No (must use a different directory other than ``lib``)", "Yes", "No", "Yes"


Use Case One: Basic Pig Script
******************************

The first use case simply reads input, processes that input, and then writes
the date to an output directory. We're also defining to
variables in the Workflow XML that are used in the Pig script.

Example Pig Script
++++++++++++++++++

``script.pig``

.. code-block:: bash

   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();


Example Workflow
++++++++++++++++

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

Use Case 2: Using a Custom JAR
******************************

In this example use case, we're putting a custom JAR in the HDFS directory 
in addition to the Workflow ``lib`` directory. The location of the JAR needs to
be specified in the ``<file>`` element in ``workflow.xml`` and registered in the Pig script.

Also, the specified path in ``workflow.xml`` must include the symlink (e.g., ``#udf.jar``),
otherwise an error will occur. The symlink ensures that the TaskTracker creates 
a symlink in the current working directory of the Pig client (on the launcher mapper);
without the symlink, the Pig client cannot find the UDF JAR file.

Pig Script
++++++++++

.. code-block:: bash

   REGISTER udf.jar
   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();


Workflow
++++++++

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


.. Case Three: TBD
.. ***********


.. (NOT recommended, NOT work with Hadoop 23 and after): put a customized jar in the 
.. HDFS directory other than workflow lib/ directory, then jar file in <archive> 
.. instead of <file> . This case has been working with hadoop version up to 0.20.*** , 
.. since <archive> has undocumented behavior of copying the original jar file under 
.. top directory after being expanded. Note this is NOT supported in Hadoop 23 

.. .. note:: 
          It is strongly recommended to start using ``<file>``
          Refer to Case Two above or http://bug.corp.yahoo.com/show_bug.cgi?id=5729898 about 
          how to change ``<archive>`` to ``<file>``. (http://twiki.corp.yahoo.com/view/Grid/HadoopNextUserImpact#Application_Compatibility), 
          and this case will break. 

.. Pig Script
.. ++++++++++
.. 
.. 
.. .. code-block:: bash
.. 
..    REGISTER udfjar/tutorial-udf.jar
..    A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
..    B = foreach A generate org.apache.pig.tutorial.UPPER(name);
..    store B into '$OUTPUT' USING PigStorage();
.. 
.. 
.. Workflow
.. ++++++++
.. 
.. 
.. .. code-block:: xml
.. 
..    ... ...
..        <action name='pig2'>
..            <pig>
..                <job-tracker>${jobTracker}</job-tracker>
..                <name-node>${nameNode}</name-node>
..                <configuration>
..                    <property>
..                        <name>mapred.job.queue.name</name>
..                        <value>${queueName}</value>
..                    </property>
..                    <property>
..                        <name>mapred.compress.map.output</name>
..                        <value>true</value>
..                    </property>
..                </configuration>
..                <script>org/apache/oozie/examples/pig/script.pig</script>
..                <param>INPUT=${inputDir}</param>
..                <param>OUTPUT=${outputDir}/pig-output2</param>
..                <archive>/tmp/tutorial-udf.jar#udfjar</archive>
..            </pig>
..            <ok to="decision1" />
..            <error to="fail" />
..        </action>
..    ... ...
.. 
.. .. note:: You cannot put ``udf.jar` in the Workflow ``lib/`` when file is already in 
..           ``<file>`` or ``<archive>`` otherwise oozie will error out::
.. 
..               Error starting action [pig2]. ErrorType [TRANSIENT], ErrorCode [JA009], Message [JA009: 
..               The core URI, "hdfs://gsbl90390.blue.ygrid.yahoo.com/user/mchiang/yoozie_test/workflows/pig-2/lib/tutorial-udf.jar" 
..               is listed both in mapred.cache.files and in mapred.cache.archives .]
.. 
.. Use Case 4: TBD
.. ***************
.. 
.. Pig Script
.. ++++++++++
.. 
.. .. code-block:: bash
.. 
..    REGISTER udfjar/tutorial-udf.jar
..    A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
..    B = foreach A generate org.apache.pig.tutorial.UPPER(name);
..    store B into '$OUTPUT' USING PigStorage();
.. 
.. Workflow
.. ++++++++
.. 
.. .. code-block:: xml
.. 
..    ... ...
..        <action name='pig2'>
..            <pig>
..                <job-tracker>${jobTracker}</job-tracker>
..                <name-node>${nameNode}</name-node>
..                <configuration>
..                    <property>
..                        <name>mapred.job.queue.name</name>
..                        <value>${queueName}</value>
..                    </property>
..                    <property>
..                        <name>mapred.compress.map.output</name>
..                        <value>true</value>
..                    </property>
..                </configuration>
..                <script>org/apache/oozie/examples/pig/script.pig</script>
..                <param>INPUT=${inputDir}</param>
..                <param>OUTPUT=${outputDir}/pig-output2</param>
..                <!-- error: lib/*jar cannot be in archive -->
..                <archive>lib/tutorial-udf.jar#udfjar</archive>
..            </pig>
..            <ok to="decision1" />
..            <error to="fail" />
..         </action>
..    ... ...
.. 
.. 
.. 


Streaming Action
----------------

The following example of a Streaming action simply 
takes output from ``cat`` and then counts the lines, 
words, and bytes. The count is then written to an
output directory. 


Example
~~~~~~~

The Streaming action simply pipes output from a
mapper to a reducer with ``org.apache.hadoop.streaming.PipeMapRunner``
as shown below.

.. code-block:: xml

   <action>
   ...
       <configuration>
           <property>
               <name>mapred.map.runner.class</name>
               <value>org.apache.hadoop.streaming.PipeMapRunner</value>
           </property>
           ...
       </configuration>
   ...
   </action>


Sample Output
~~~~~~~~~~~~~

The output from the reducer ``wc`` will be written to ``${outputDir}/streaming-output``.

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

Fs Action
---------

If you wanted to recursively change the permissions of a directory and its contents,
you would run the following HDFS command: ``hdfs dfs -chmod -R 766 <dir>;``

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
on the Mithril Blue cluster for more detailed information and an additional examples.

.. note:: You can also recursively change permissions in a Pig script. For example,
          the Pig script ``script.pig`` could have the command ``hdfs dfs -chmod -R 766 <dir>;``.


Shell Action
------------

Using Global Section
~~~~~~~~~~~~~~~~~~~~

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
           <shell xmlns="uri:oozie:shell-action:0.3"> <!-- Action xmlns version GOES HERE -->
               <exec>python</exec>
               <argument>...</argument>
               ...
           </shell>
       </action>
       ...
   </workflow>


Ssh Action
----------

See `Oozie Ssh Action Extension <https://oozie.apache.org/docs/3.2.0-incubating/DG_SshActionExtension.html>`_ 
for the syntax, an example, and the schema for the Ssh action.

Email Action
------------

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

See `Oozie Email Action Extension <http://oozie.apache.org/docs/3.2.0-incubating/DG_EmailActionExtension.html>`_ 
for the syntax, an example, and the schema for the Email action.

.. note:: To send email to an iList, the iList setting needs to allow posts 
          from non-members. no config change needed when sending to individual account.
          "List admin" > "Edit List Config" > "Sending/Reception" > "Who can send messages" 
          should probably be set to 'public list (open)'


Sub-workflow Action
-------------------

See `Oozie Sub-Workflow Action Extension <https://oozie.apache.org/docs/3.2.0-incubating/WorkflowFunctionalSpec.html#a3.2.6_Sub-workflow_Action
>`_ for the syntax, an example, and the schema for the Sub-workflow action.

DistCp Action
-------------

`DistCp <https://hadoop.apache.org/docs/r1.2.1/distcp.html>`_ is a tool used for large inter/intra-cluster copying.

The following ``workflow.xml`` copies a bzipped file 
to a user's home directory. 

.. code-block:: xml

   <workflow-app name="hue_tutorial_workflow" xmlns="uri:oozie:workflow:0.4">
      <start to="copy_dataset"/>

         <action name="copy_data" cred="hcat">
            <distcp xmlns="uri:oozie:distcp-action:0.1">
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
                  <arg>/tmp/dataset.bz2</arg>
                  <arg>/user/yhoo_star/</arg>
            </distcp>
            <ok to="del_db_tables"/>
            <error to="kill"/>
         </action>
         <kill name="kill">
            <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
         </kill>
         <end name="end"/>
   </workflow-app>



-

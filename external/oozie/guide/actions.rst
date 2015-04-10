Actions
=======

Overview
--------

Action nodes are the mechanism by which a workflow triggers the execution of a 
computation/processing task. Oozie provides support for different types of actions: 

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

Oozie can be extended to support additional type of actions. 
`Map Reduce <https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook>`_, and 
`Pig <https://cwiki.apache.org/confluence/display/OOZIE/Pig+Cookbook>`_ examples, we'll focus
on the other actions.  

MapReduce
---------

See the external Oozie `MapReduce Cookbook <https://cwiki.apache.org/confluence/display/OOZIE/Map+Reduce+Cookbook>`_.
We also have an example below for using MapReduce API (version 0.23+).

New MapReduce API
~~~~~~~~~~~~~~~~~
To run MR jobs using new API in Oozie, you have to do the following:

- change ``mapred.mapper.class to mapreduce.map.class``
- change ``mapred.reducer.class to mapreduce.reduce.class``
- add ``mapred.output.key.class``
- add ``mapred.output.value.class``
- include the following property into MR action configuration:

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

In addition to the example given below, we suggest you also see the external Oozie
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
and put in your workflow lib/ directory.

Here's a sample Java main class.

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

Define a java action in your workflow.xml

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

Upload the Perl script (test.pl) and Perl module (DatetimeHlp.pm) to the ``oozie.wf.application.path`` 
directory on HDFS. The ``main`` class should be packaged in a JAR file and uploaded to 
``oozie.wf.application.path/lib directory``.

Here's the sample Java ``main`` class:


.. code-block:: java

   package qa.test.tests;
   import qa.test.common.*;
   import java.io.*;
   import java.util.*;
   public class testShell {
           public static void main (String[] args)
           {
                   String cmdfile = args[0];
                   String text = args[1];
                   try{
                           String runCmd1;
                           runCmd1       = cmdfile +" "+text;
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

Hive Action
-----------

http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_HiveActionExtension.html
Example for using Hive Action in oozie workflow with HCatalog
kryptonitered-oozie.red.ygrid.yahoo.com%3A4080%2Foozie%2Fdocs%2FDG_HiveActionExtension.html&sa=D&sntz=1&usg=AFQjCNEpSSun_h-SAGH7zQfOX_aDl5TC2Q




Pig Action
----------

TBD: Note 2: NOT supported pig options: -4 (-log4jconf), -e (-execute), -f (-file), -l (-logfile), -r (-dryrun), -x (-exectype), -P (-propertyFile)


Using UDFs (User Defined Functions)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Summary Table for Cases**
 
.. csv-table:: Use Cases for UDFs in Pig Actions
   :header: "", "``udf.jar in Worklow ``/lib`` Directory?", "Register in Pig Script?", "``udf.jar`` in File?", "``udf.jar`` in Archive?"
   :widths: 15, 10, 30

   "Case 1", "Yes", "No", "No", "No"
   "Case 2", "No (must use a different directory other than ``/lib``)", "Yes", "Yes", "No"
   "Case 3", "No (must use a different directory other than ``/lib``)", "Yes", "No", "Yes"


Use Case 1: TBD
***************

Example Pig Script
++++++++++++++++++

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

Use Case 2: TBD
***************

put a customized jar in the HDFS directory other than workflow lib/ directory. then 
this jar file needs to be in <file> and needs to be "Register" in pig script. also 
please make sure that symlink (e.g, #udf.jar) is specified in <file> otherwise 
error out. Symlink option ensures tasktracker to create symlink in current working 
directory of pig client(on launcher mapper), and without it, pig client cannot find the udf jar file.

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
           <archive>/tmp/tutorial-udf.jar#udf.jar</archive>
       </pig>
       <ok to="decision1" />
       <error to="fail" />
   </action>


Case 3: TBD
***********

(NOT recommended, NOT work with Hadoop 23 and after): put a customized jar in the 
HDFS directory other than workflow lib/ directory, then jar file in <archive> 
instead of <file> . This case has been working with hadoop version up to 0.20.*** , 
since <archive> has undocumented behavior of copying the original jar file under 
top directory after being expanded. Note this is NOT supported in Hadoop 23 

.. note:: (http://twiki.corp.yahoo.com/view/Grid/HadoopNextUserImpact#Application_Compatibility), 
          and this case will break. It is strongly recommended to start using <file> Please 
          refer to CASE 2 above orhttp://bug.corp.yahoo.com/show_bug.cgi?id=5729898 about 
          how to change <archive> to <file>.

Pig Script
++++++++++


.. code-block:: bash

   REGISTER udfjar/tutorial-udf.jar
   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();


Workflow
++++++++


.. code-block:: xml

   ... ...
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
               <archive>/tmp/tutorial-udf.jar#udfjar</archive>
           </pig>
           <ok to="decision1" />
           <error to="fail" />
       </action>
   ... ...

.. note:: You cannot put ``udf.jar` in the Workflow ``lib/`` when file is already in 
          ``<file>`` or ``<archive>`` otherwise oozie will error out::

              Error starting action [pig2]. ErrorType [TRANSIENT], ErrorCode [JA009], Message [JA009: 
              The core URI, "hdfs://gsbl90390.blue.ygrid.yahoo.com/user/mchiang/yoozie_test/workflows/pig-2/lib/tutorial-udf.jar" 
              is listed both in mapred.cache.files and in mapred.cache.archives .]

Use Case 4: TBD
***************

Pig Script
++++++++++

.. code-block:: bash

   REGISTER udfjar/tutorial-udf.jar
   A = load '$INPUT/student_data' using PigStorage('\t') as (name: chararray, age: int, gpa: float);
   B = foreach A generate org.apache.pig.tutorial.UPPER(name);
   store B into '$OUTPUT' USING PigStorage();

Workflow
++++++++

.. code-block:: xml

   ... ...
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
               <!-- error: lib/*jar cannot be in archive -->
               <archive>lib/tutorial-udf.jar#udfjar</archive>
           </pig>
           <ok to="decision1" />
           <error to="fail" />
        </action>
   ... ...




Streaming Action
----------------

Overview
~~~~~~~~

Example
~~~~~~~

.. code-block::

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

Output will be written to ``${outputDir}/streaming-output``.

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

How to chmod recursively

Oozie can perform the ‘chmod’ in a directory. Please see the following spec.
http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a3.2.4_Fs_HDFS_action
You can chmod recursively in a PIG script

$ cat script.pig

fs -chmod -R 777 <dir>;


Shell Action
------------

Using Global Section
~~~~~~~~~~~~~~~~~~~~

To be able to use the global section in your Oozie workflow for defining configuration 
parameters applicable to all actions, specifically shell action here, make sure 
you are using the latest shell xml namespace 0.3

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



Email Action
------------

To send email to ilist, ilist setting needs to be changed such that it allows post 
from non-members. no config change needed when sending to individual account.
"List admin" > "Edit List Config" > "Sending/Reception" > "Who can send messages" 
should probably be set to 'public list (open)'


Sub-workflow Action
-------------------

DistCp Action
-------------


Distcp V2 action support
http://twiki.corp.yahoo.com/view/CCDI/DistcpV2Action


-

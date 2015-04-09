Oozie Actions
=============

Overview
--------

Action nodes are the mechanism by which a workflow triggers the execution of a 
computation/processing task. Oozie provides support for different types of actions: 

- MapReduce
- Java
- Hive
- Pig
- Fs 
- Ssh
- Email
- Sub-workflow
- Streaming

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


Ssh Action
----------

Email Action
------------

Sub-workflow Action
-------------------

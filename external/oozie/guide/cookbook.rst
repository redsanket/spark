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



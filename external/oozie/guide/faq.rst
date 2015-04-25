===
FAQ
===

.. 04/22/15: Rewrote

This page answers some of the most common questions we get about Oozie at Yahoo. For 
troubleshooting issues, see `Troubleshooting <ts.html>`_.

Questions
=========

* :ref:`Where are the log files created? <log_files>`  
* :ref:`How do you turn off the uber Application (AM) for launcher jobs? <turn_off>`
* :ref:`How do you run a Workflow action accessing the NameNode of a different cluster? <run_different_cluster>`
* :ref:`How do you allow other user(s) to view the Hadoop logs? <allow_users_view_logs>`
* :ref:`How do you pass variables to Oozie? <pass_variables>`
* :ref:`How do you pass variables between actions? <pass_vars_actions>`
* :ref:`How do you use common libraries? <common_libs>`
* :ref:`How do you delete directories only when you rerun a job? <del_dir_rerun>`
* :ref:`How do you pass environment variables to actions? <pass_env_vars_actions>`
* :ref:`How do you programmatically access action configuration? <access_action_config>`
* :ref:`How to run an Oozie query with -filter option? <filter>`
* :ref:`How to submit a MapReduce job through HTTP? <mr_http>`
* :ref:`Where do I view the Pig client log for executed Pig scripts? <view_pig_log>`
* :ref:`Why does my job run fine as a standalone Pig script but not through Oozie? <standalone_oozie>`
* :ref:`How can I increase the memory for the Pig launcher job? <pig_job_memory>`
* :ref:`How do you pass parameters to Pig actions? <pig_params_pass>`
* :ref:`How to submit a Pig job through HTTP? <submit_pig_http>`
* :ref:`How do you check whether the gYCA Web server is serving certificates? <yca_serve_certs>`
* :ref:`How do you change the timeout for Coordinator actions? <timeout_coord_actions>`
* :ref:`How do you reprocess Coordinator actions? <reprocess_coord_actions>`
* :ref:`How do you update a Coordinator definition on the fly? <update_coord>`
* :ref:`How to reprocess Coordinator actions? <reprocess_cord>`
* :ref:`Why does Oozie take a long time to update after finishing the corresponding Hadoop job? <long_time_finish>`


Answers
=======

.. _log_files:
.. topic::  **Where are the log files created?**

   The Hive server log is located at ``/home/y/libexec/hive_server/logs/hive_server.log``. 
   The Hive CLI log is in ``$HADOOP_TOOLS_HOME/var/logs/hive_cli/${userid}/hive.log``.


.. _turn_off:

.. topic:: **How do you turn off the uber ApplicationMaster (AM) for launcher jobs?**

   In Hadoop 2.x, the launcher job is run in uberized mode. For example, the launcher map task 
   is run as part of the launcher AM to avert launching an additional container. 
   If that is not desired, it can be turned off per workflow action 
   by configuring ``oozie.launcher.mapreduce.job.ubertask.enable`` to 
   ``false`` in the action configuration.



.. _run_different_cluster:

.. topic:: **How do you run a Workflow action accessing the NameNode of a different cluster?**

   You set the value of ``oozie.launcher.mapreduce.job.hdfs-servers`` configuration to 
   ``hdfs://sourcenamenode.colo.ygrid.yahoo.com`` (the same colo, different cluster) or 
   ``webhdfs://sourcenamenode.colo.ygrid.yahoo.com`` (cross-colo cluster). You will have 
   to use the same protocol while referring the other NameNode in your Workflow or 
   Pig scripts. A comma-separated list can be specified.

   For example, the Workflow XML below allows an action running in Dilithium Blue trying 
   to access data in Uraniumb Blue(same colo) and Phazon Tan(cross-colo).

   .. code-block:: xml

      <property>
         <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
         <value>hdfs://uraniumblue-nn1.blue.ygrid.yahoo.com,webhdfs://phazontan-nn1.tan.ygrid.yahoo.com</value>
      </property>


    
.. _allow_users_view_logs:

.. topic:: **How do you allow other user(s) to view the Hadoop logs?**

   In Hadoop 20S+, any user other than the submitter of the job can not view the generated Hadoop logs. 
   However, the job submitter could allow specific user(s) to see its log by defining 
   few parameters during job submission. The same thing could be achieved through Oozie.

   More information, see the 
   `Grid Security User Impact <http://twiki.corp.yahoo.com/view/Grid/GridSecurityUserImpact>`_
   The following example shows how to configure that in ``workflow.xml``.

   .. code-block:: xml

      <workflow-app xmlns='uri:oozie:workflow:0.5' name='streaming-wf'>
         <start to='streaming1' />
         <action name='streaming1'>
            <map-reduce>
               <job-tracker>${jobTracker}</job-tracker>
               <name-node>${nameNode}</name-node>
               <prepare>
                  <delete path="${outputDir}"/>
               </prepare>
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
                     <value>${outputDir}</value>
                  </property>
                  <property>
                     <name>mapred.job.queue.name</name>
                     <value>${queueName}</value>
                  </property>
                  <property>
                     <name>mapred.input.format.class</name>
                     <value>org.apache.hadoop.mapred.TextInputFormat</value>
                  </property>
                  <property>
                     <name>dfs.umask</name>
                     <value>18</value>
                  </property>
                  <!------ Start of configuration to allow other user to view the hadoop log ------>                
                  <property>
                     <name>mapreduce.job.acl-modify-job</name>
                     <value>users</value>
                  </property>
                  <property>
                     <name>mapreduce.job.acl-view-job</name>
                     <value>kamrul,marchen</value>
                  </property>
                  <property>
                     <name>oozie.launcher.mapreduce.job.acl-modify-job</name>
                     <value>users</value>
                  </property>
                  <property>
                     <name>oozie.launcher.mapreduce.job.acl-view-job</name>
                     <value>kamrul,marchen</value>
                  </property>
                  <!------ End of configuration ------>
               </configuration>
            </map-reduce>
            <ok to="end" />
            <error to="fail" />
         </action>
         <kill name="fail">
            <message>Streaming Map/Reduce failed, error
       message[${wf:errorMessage(wf:lastErrorNode())}]</message>
         </kill>
         <end name='end' />
      </workflow-app>


 
.. _pass_variables:

.. topic:: **How do you pass variables to Oozie?**


   You pass configuration parameters to Oozie CLI using the ``-config`` option::

       $ oozie job -run -config map-reduce-job.properties


   The properties file would look something like the following::

       oozie.wf.application.path=hdfs://localhost:9000/user/danielwo/workflows/map-reduce
       inputDir=hdfs://gsbl91034.blue.ygrid.yahoo.com:9000/user/danielwo/input-data
       outputDir=hdfs://gsbl91034.blue.ygrid.yahoo.com:9000/user/danielwo/output-data-map-reduce
       jobTracker=gsbl91034.blue.ygrid.yahoo.com:9001
       nameNode=hdfs://gsbl91034.blue.ygrid.yahoo.com:9000
       queueName=unfunded
       group.name=users


   .. note:: From Hadoop 0.23, you pass the ResourceManager ``hostname:port`` to 
             the Oozie ``<job-tracker>`` element. 


.. _pass_vars_actions:

.. topic:: **How do you pass variables between actions?**

   In this example, we pass the ``PASS_ME`` variable between the Java action and the ``pig1`` action.
   The ``PASS_ME`` variable is given the value ``123456`` in the ``java-main`` action named ``java1``.
   The ``pig1`` action subsequently reads the value of the ``PASS_ME`` variable and passes it to the 
   Pig script.

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
                  <capture-output/>
              </java>
              <ok to="pig1" />
              <error to="fail" />
          </action>
          <action name='pig1'>
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
                  <param>MY_VAR=${wf:actionData("java1")["PASS_ME"]}</param>
      
                  <file>/tmp/${wf:user()}/tutorial-udf.jar#tutorial-udf.jar</file>
              </pig>
              <ok to="end" />
              <error to="fail" />
          </action>
          <kill name="fail">
              <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
          </kill>
          <end name='end' />
      </workflow-app>

   In the Java ``Main`` class, the sample class ``org.apache.oozie.test.MyTest`` should be 
   packaged in a JAR file and put in your Workflow ``lib`` directory. The ``main()`` 
   method writes a Property file to the path specified in the 
   ``oozie.action.output.properties`` environment variable.

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
            try{
               File file = new File(System.getProperty("oozie.action.output.properties"));
               Properties props = new Properties();
               props.setProperty("PASS_ME", "123456"); 
      
               OutputStream os = new FileOutputStream(file);
               props.store(os, "");
               os.close();
               System.out.println(file.getAbsolutePath()); 
            }
            catch (Exception e) {
               e.printStackTrace();
            }
         }
      }

      
.. _common_libs:

.. topic:: **How do you use common libraries?** 

   You can save all common library JARs in the ``lib`` directory, which is at the same level as ``workflow.xml``.
   Or, you can store common library JARs in a shared location in HDFS and 
   refer to them in each of your Workflows.

   Examples of common JARS are: ``hadoop-streaming.jar``, ``pig.jar``, etc..
   Use the ``<file>`` XML element to refer to the absolute path to these JARs in HDFS. 
   You do not need to include them in your workflow ``lib`` directory.
   Refer to the `Oozie documentation <http://oozie.apache.org/docs/3.3.2/WorkflowFunctionalSpec.html#a3.2.2.1_Adding_Files_and_Archives_for_the_Job>`_for details on how to use the ``<file>`` element.
   In Oozie 5.0, you store common library JARs in a shared location in HDFS. For example, 
   in the ``job.properties`` file, you would specify ``oozie.libpath=hdfs://nn:8020/tmp/commonlib``.

.. _del_dir_rerun:

.. topic:: **How do you delete directories only when you rerun a job?** 

   The ``myOutputDir`` will only be deleted when the job is rerun. Otherwise, some dummy (non-existing) 
   directory will be removed.

   .. code-block:: xml

      <prepare>
          <delete path="${ (wf:run() != 0) ? myOutpuDir : '/tmp/dummy'  }"/>
      </prepare>

.. _pass_env_vars_actions:

.. topic:: **How do you pass environment variables to actions?**

   To set an environment variable for a MapReduce action:

   .. code-block:: xml

      <property>
          <name>mapred.child.env</name>
          <value>A=foo</value>
      </property> 


   To set an environment variable for a Pig action:

   .. code-block:: xml

      <property>
          <name>oozie.launcher.mapred.child.env</name>
          <value>A=foo</value>
      </property> 


   To set an environment variable for the MapReduce jobs started by a Pig action:

   .. code-block:: xml

      <property>
          <name>mapred.child.env</name>
          <value>A=foo</value>
      </property> 

.. _access_action_config:

.. topic:: **How do you programmatically access action configuration?**


   For each Oozie action, the configuration is stored locally where the job runs, 
   and its location is passed by the system variable ``oozie.action.conf.xml``.

   If you are accessing some configuration properties in your ``java-action`` main 
   class or custom ``map-reduce`` action mapper/reducer class, do the following::

       String confLocation = System.getProperty("oozie.action.conf.xml");
       Path localConfPath = new Path(confLocation);
       Configuration conf = new Configuration();
       conf.addResource(localConfPath);

       // .. continue here


.. _filter:

.. topic:: **How to run an Oozie query with -filter option?**

   You can run the query with multiple filter options by escaping ";" as \; or quoting the whole filter::

       $ oozie jobs -filter "user=user123;status=KILLED"

   or::
   
       $ oozie jobs -filter user=user123\;status=KILLED

.. _mr_http:

.. topic:: **How to submit a MapReduce job through HTTP?** 

   You use the XOozieClient API to submit a MapReduce job through HTTP.
  
   **1. Install yinst Dependencies** 

   ::   

       $ yinst install bouncer_auth_java
       $ yinst install yjava_byauth
       $ yinst install java_log4j
       $ yinst install yoozie_client
  
   **2. Set CLASSPATH**
 
   ::
     
       $ export CLASSPATH=".:/home/y/var/yoozieclient/lib/yoozie-client-4.0.0.4.jar:/home/y/var/yoozieclient/lib/oozie-client-4.0.0.4.jar:/home/y/var/yoozieclient/lib/json-simple-1.1.jar:/home/y/var/yoozieclient/lib/commons-cli-1.2.jar:/home/y/lib/jars/yjava_byauth.jar:/home/y/lib/jars/bouncer_auth_java.jar:/home/y/lib/jars/log4j.jar"
      
   **3. Create Java Oozie Client**

   .. code-block:: java

      import org.apache.oozie.client.OozieClient;
      import org.apache.oozie.client.WorkflowJob;
      import org.apache.oozie.client.OozieClientException;
      
      import java.util.Properties;
      
      //for bouncer authentication start
      import java.io.BufferedReader;
      import java.io.InputStreamReader;
      import com.yahoo.bouncer.sso.CookieInfo;         // provided by the bouncer_auth_java package
      import com.yahoo.bouncer.sso.CookieValidator;
      import yjava.byauth.jaas.HttpClientBouncerAuth;  // provided by the yjava_byauth package, which requires org.apache.log4j.Logger (comes with yjava_log4j)
      
      import java.io.IOException;
      import java.security.NoSuchAlgorithmException;
      import java.security.spec.InvalidKeySpecException;
      import java.security.InvalidKeyException;    //for bouncer authentication end
      
      public class MyOozieClient {
      
          public static void main(String[] args) throws InterruptedException, WorkflowClientException,IOException,java.security.NoSuchAlgorithmException,java.security.spec.InvalidKeySpecException,java.security.InvalidKeyException {
              // get a WorkflowClient for local Oozie
              WorkflowClient wc = new WorkflowClient("http://gsbl91034.blue.ygrid.yahoo.com:8080/oozie");
      
              // create a workflow job configuration and set the workflow application path
              Properties conf = wc.createConfiguration();
              conf.setProperty(WorkflowClient.APP_PATH, "hdfs://localhost:9000/user/danielwo/workflows/map-reduce");
      
              // setting workflow parameters
              conf.setProperty("jobTracker", "localhost:9001");
              conf.setProperty("inputDir", "/user/danielwo/input-data");
              conf.setProperty("outputDir", "/user/danielwo/output-map-reduce");
      
         //set your group
              conf.setProperty("group.name", "users");
      
         //Bouncer authentication
         System.out.print("Username: ");
         System.out.flush();
         String username = new BufferedReader(new InputStreamReader(System.in)).readLine();
         char[] password = System.console().readPassword("%s", "Password: ");
      
         HttpClientBouncerAuth auth = new  HttpClientBouncerAuth();
         String YBYCOOKIE = auth.authenticate("https://bouncer.gh.corp.yahoo.com/login/",  username, password);
         wc.setHeader("cookie",  YBYCOOKIE);
      
              // verify cookie
         CookieValidator validator = new CookieValidator();
         validator.initialize();
         CookieInfo info = validator.authSig(YBYCOOKIE);
         System.out.println("Valid cookie: " + info.isValid());
      
              // submit and start the workflow job
              String jobId = wc.run(conf);
              System.out.println("Workflow job submitted");
      
              // wait until the workflow job finishes printing the status every 10 secs
              while (wc.getJobInfo(jobId).getStatus() == Workflow.Status.RUNNING) {
                  System.out.println("Workflow job running ...");
                  Thread.sleep(10 * 1000);
              }
      
              // print the final status o the workflow job
              System.out.println("Workflow job completed ...");
              System.out.println(wc.getJobInfo(jobId));
          }
      
      }
   
   **4. Compile Code**

   :: 
   
       $ javac MyOozieClient.java

   **5. Run Program**

   ::

      $ java MyOozieClient
      Username: [your user name here]
      Password: [your password here]
      Valid cookie: true
      Workflow job submitted
      Workflow job running ...
      Workflow job running ...
      Workflow job running ...
      Workflow job running ...
      Workflow job completed ...
      Workflow id[3-091009212100197-oozie-danielwo] status[SUCCEEDED]


.. Left off here on 04/21/15

.. _view_pig_log:

.. topic:: **Where do I view the Pig client log for executed Pig scripts?** 

   Click the **Console URL** of the Pig action in the Oozie UI. It will take you to 
   the Pig launcher Hadoop job in the ResourceManager or the Job History UI. The Hadoop 
   job should have one map task. Click the map task logs to view 
   three separate logs: ``stdout``, ``stderr``, and ``syslog``. The ``stdout`` logs will 
   give the Pig client log. If there are any failures, look at ``stderr`` as well for 
   exception stacktraces.

.. _standalone_oozie:

.. topic:: **Why does my job run fine as a standalone Pig script but not through Oozie?** 

   When Pig runs from gateways, it uses a pre-configured command with cluster 
   specific settings. If the same configuration is given in ``worklflow.xml``, Oozie 
   should be able to use those configurations. 

   The most frequent issue is related to memory used by ``pig`` command. You can view the memory used 
   and other information with the following command:

   :: 

       [kamrul@gwbl7003 ~]$ /home/gs/pig/latest/bin/pig -useversion 0.7 -secretDebugCmd
       USING: /home/gs/pig/0.7
       Would run /grid/0/gs/java/jdk/bin/java -Xmx2048m -cp /grid/0/gs/pig/0.7/lib/pig.jar:/grid/0/gs/pig/0.7/conf/:/grid/0/gs/conf/current:/grid/0/gs/pig/0.7/lib/myna.jar:/grid/0/gs/pig/0.7/lib/piggybank.jar:/grid/0/gs/pig/0.7/lib/sds.jar:/grid/0/gs/pig/0.7/lib/zebra.jar:/grid/0/gs/conf/current:/grid/0/gs/java/jdk/lib/tools.jar:/grid/0/gs/hadoop/current/bin/..:/grid/0/gs/hadoop/current/bin/../hadoop-mapreduce-client-jobclient-0.23.9.3.1310251519.jar:/grid/0/gs/hadoop/current/bin/../lib/aspectjrt-1.6.5.jar:/grid/0/gs/hadoop/current/bin/../lib/aspectjtools-1.6.5.jar:/grid/0/gs/hadoop/current/bin/../lib/axis-ant.jar:/grid/0/gs/hadoop/current/bin/../lib/axis.jar:/grid/0/gs/hadoop/current/bin/../lib/bouncer_auth_java-0.5.12.jar:/grid/0/gs/hadoop/current/bin/../lib/BouncerFilterAuth-1.1.4.jar:/grid/0/gs/hadoop/current/bin/../lib/chukwa-hadoop-0.1.1-client.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-cli-1.2.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-codec-1.4.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-daemon-1.0.1.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-discovery-0.2.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-el-1.0.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-httpclient-3.0.1.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-logging-1.0.4.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-logging-api-1.0.4.jar:/grid/0/gs/hadoop/current/bin/../lib/commons-net-1.4.1.jar:/grid/0/gs/hadoop/current/bin/../lib/core-3.1.1.jar:/grid/0/gs/hadoop/current/bin/../lib/hadoop-gpl-compression-0.1.0-1007030707.jar:/grid/0/gs/hadoop/current/bin/../lib/hsqldb-1.8.0.10.jar:/grid/0/gs/hadoop/current/bin/../lib/jackson-core-asl-1.0.1.jar:/grid/0/gs/hadoop/current/bin/../lib/jackson-mapper-asl-1.0.1.jar:/grid/0/gs/hadoop/current/bin/../lib/jasper-compiler-5.5.12.jar:/grid/0/gs/hadoop/current/bin/../lib/jasper-runtime-5.5.12.jar:/grid/0/gs/hadoop/current/bin/../lib/jaxrpc.jar:/grid/0/gs/hadoop/current/bin/../lib/jets3t-0.6.1.jar:/grid/0/gs/hadoop/current/bin/../lib/jetty-6.1.14.jar:/grid/0/gs/hadoop/current/bin/../lib/jetty-util-6.1.14.jar:/grid/0/gs/hadoop/current/bin/../lib/json.jar:/grid/0/gs/hadoop/current/bin/../lib/junit-4.5.jar:/grid/0/gs/hadoop/current/bin/../lib/kfs-0.2.2.jar:/grid/0/gs/hadoop/current/bin/../lib/log4j-1.2.15.jar:/grid/0/gs/hadoop/current/bin/../lib/mockito-all-1.8.0.jar:/grid/0/gs/hadoop/current/bin/../lib/oro-2.0.8.jar:/grid/0/gs/hadoop/current/bin/../lib/saaj.jar:/grid/0/gs/hadoop/current/bin/../lib/servlet-api-2.5-6.1.14.jar:/grid/0/gs/hadoop/current/bin/../lib/SimonPlugin.jar:/grid/0/gs/hadoop/current/bin/../lib/slf4j-api-1.4.3.jar:/grid/0/gs/hadoop/current/bin/../lib/slf4j-log4j12-1.4.3.jar:/grid/0/gs/hadoop/current/bin/../lib/wsdl4j-1.5.1.jar:/grid/0/gs/hadoop/current/bin/../lib/xmlenc-0.52.jar:/grid/0/gs/hadoop/current/bin/../lib/yjava_byauth-0.5.6.jar:/grid/0/gs/hadoop/current/bin/../lib/yjava_servlet_filters-0.4.2-0.4.2.jar:/grid/0/gs/hadoop/current/bin/../lib/yjava_ysecure-1.3.2.jar:/grid/0/gs/hadoop/current/bin/../lib/yjava_ysecure_native-1.3.0.jar:/grid/0/gs/hadoop/current/bin/../lib/ymonmetricscontext-0.1.0.jar:/grid/0/gs/hadoop/current/bin/../lib/jsp-2.1/jsp-2.1.jar:/grid/0/gs/hadoop/current/bin/../lib/jsp-2.1/jsp-api-2.1.jar:/grid/0/gs/hadoop/current/bin/../hadoop-capacity-scheduler-0.20.104.3.1007030707.jar -Djava.io.tmpdir=/grid/0/tmp -Dmetadata.impl=org.apache.hadoop.owl.pig.metainterface.OwlPigMetaTables -Dudf.import.list=org.apache.pig.builtin:org.apache.pig.impl.builtin:com.yahoo.pig.yst.sds.ULT:myna:org.apache.pig.piggybank.evaluation:org.apache.pig.piggybank.evaluation.datetime:org.apache.pig.piggybank.evaluation.decode:org.apache.pig.piggybank.evaluation.math:org.apache.pig.piggybank.evaluation.stats:org.apache.pig.piggybank.evaluation.string:org.apache.pig.piggybank.evaluation.util:org.apache.pig.piggybank.evaluation.util.apachelogparser:string:util:math:datetime:sequence:util:org.apache.hadoop.zebra.pig -Djava.library.path=/grid/0/gs/hadoop/current/lib/native/Linux-i386-32 org.apache.pig.Main


.. _pig_job_memory:

.. topic:: **How can I increase the memory for the Pig launcher job?**

   You can define the property (``oozie.launcher.mapred.child.java.opts``) in your action:

    .. code-block:: xml

       <property>
           <name>oozie.launcher.mapred.child.java.opts</name>
           <value>-server -Xmx1G -Djava.net.preferIPv4Stack=true</value>
           <description>setting memory usage to 1024MB</description>
       </property>

   **Example**

   .. code-block:: xml

      <workflow-app xmlns='uri:oozie:workflow:0.5' name='pig-wf'>
          <start to='pig1' />
          <action name='pig1'>
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
      
                      <property>
                          <name>oozie.launcher.mapred.child.java.opts</name>
                          <value>-server -Xmx1G -Djava.net.preferIPv4Stack=true</value>
                      </property>
      
                  </configuration>
                  <script>org/apache/oozie/examples/pig/script.pig</script>
              </pig>
              <ok to="end" />
              <error to="fail" />
          </action>
          <kill name="fail">
              <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
          </kill>
          <end name='end' />
      </workflow-app>

   If you need more than 1.5 G memory for the Pig launcher, 
   increase the property ``oozie.launcher.mapred.job.map.memory.mb`` (to 2GB) in addition to 
   ``oozie.launcher.mapred.child.java.opts``:

   .. code-block:: xml

      <property>
          <name>oozie.launcher.mapred.child.java.opts</name>
          <value>-server -Xmx2G -Djava.net.preferIPv4Stack=true</value>
      </property>
      <property>
          <name>oozie.launcher.mapred.job.map.memory.mb</name>
          <value>2560</value>
      </property>

   .. note:: The default value for most tasks on the grid is 1.5G (corresponding to 1 slot). 
             Increasing this value allows a launcher map task to be assigned multiple slots 
             as high-RAM job and able to use more than 1.5G. (It could take a bit longer time 
             for the launcher map task to be scheduled and launched, but that should be minimal.)

.. _pig_params_pass:

.. topic:: **How do you pass parameters to Pig actions?**

   If you want to pass ``mapred.*`` properties to your Pig action, simply define them 
   in the ``<property>`` element of your Pig action.

   .. code-block:: xml

      <property>
          <name>mapred.min.split.size</name>
          <value>536870912</value>
      </property> 

   **Passing Parameters Through a Parameter File**

   Pig has an option to pass all the parameters through a file. The same functionality 
   could be achieved through Oozie. Follow these three steps: 

   #. Upload the parameter file into HDFS.
   #. Create a symbolic link with the ``file`` element within the Pig action.xml``.
   #. Pass the file name through the ``argument`` element of the Pig action.


   - Parameter file ('paramfile') is HDFS.
   - Here is the ``workflow.xml``:

     .. code-block:: xml

        <workflow-app xmlns='uri:oozie:workflow:0.2' name='pig-paramfile-wf'>
            <start to='pig2' />
            <action name='pig2'>
                <pig>
                    <job-tracker>${jobTracker}</job-tracker>
                    <name-node>${nameNode}</name-node>
                    <prepare>
                       <delete path="${nameNode}${outputDir}" />
                    </prepare>
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
                    <script>script.pig</script>
                    <!----- Pass the param file as argument. ----->
                    <argument>-param_file</argument>
                    <argument>paramfile</argument>
                    <file>lib/tutorial-udf.jar#udf.jar</file> 
                    <!----- Create a symbolic link   ----->
                    <file>paramfile#paramfile</file> 
                </pig>
                <ok to="decision1" />
                <error to="fail" />
            </action>
            <decision name="decision1">
                <switch>
                    <case to="end">${fs:exists(wf:conf('outputFile'))}</case>
                    <default to="fail" />
                </switch>
            </decision>
            <kill name="fail">
                <message>Pig failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
            </kill>
            <end name='end' />
        </workflow-app>
   
.. _submit_pig_http:

.. topic:: **How to submit a Pig job through HTTP?**

   **Command-line syntax:** ``oozie pig -oozie <OOZIE_URL> -file <pig script> -config job.properties -X <all pig options>``

   **Example command:** ``$ oozie pig -file multiquery1.pig -config job.properties -X -Dmapred.job.queue.name=grideng -Dmapred.compress.map.output=true -Ddfs.umask=18 -param_file paramfile -p INPUT=/tmp/workflows/input-data``
 
   .. note::  The option ``-X`` is the last argument in the command line.


   **Example job.properties**

   .. code-block:: bash

      fs.default.name=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020
      mapred.job.tracker=gsbl91029.blue.ygrid.yahoo.com:8032
      oozie.libpath=hdfs://gsbl91027.blue.ygrid.yahoo.com:8020/tmp/user/workflows/lib
      mapreduce.jobtracker.kerberos.principal=mapred/gsbl91029.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM
      dfs.namenode.kerberos.principal=hdfs/gsbl91027.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM

   **Example for Cross-NameNodes Operation:**

   #. Add ``-Doozie.launcher.mapreduce.job.hdfs-servers`` to the command line::

          $ oozie pig -file multiquery1.pig -config job.properties -X -Doozie.launcher.mapreduce.job.hdfs-servers="hdfs://sourcenamenode.blue.ygrid.yahoo.com:8020" ... ...

   #. Use ``_HOST`` for the Kerberos principal.
   
   #. Create a ``job.properties`` file, making sure you specify the parameters ``mapreduce.jobtracker.kerberos.principal`` and
      ``dfs.namenode.kerberos.principal``::

          ...
          mapreduce.jobtracker.kerberos.principal=mapred/_HOST@DEV.YGRID.YAHOO.COM 
          dfs.namenode.kerberos.principal=hdfs/_HOST@DEV.YGRID.YAHOO.COM


.. _yca_serve_certs:

.. topic:: **How do you check whether the gYCA Web server is serving certificates?**


   Use kerberos authentication::

       $ /usr/bin/curl --negotiate -u : {yca-webserver-url}/wsca/v2/certificates/kerberos/{yca-role}?http_proxy_role={yca-http-proxy-role}

   For example::

       $ curl --negotiate -u : http://gyca1-vm3.gamma.yosws.ac4.yahoo.com:4080/wsca/v2/certificates/kerberos/yca.example.gyca.test1?http_proxy_role=grid.blue.flubber.httpproxy\&do_as=strat_ci



   Or::
 
       $ (kinit)
       $ curl -v --negotiate -u : "http://stage-ca.yca.platform.yahoo.com:4080/wsca/v2/vertificated/kerberos/yca.example.gyca.test1?http_proxy_role=grid.blue.flubber.httpproxy&do_as=strat_ci"
  
.. _timeout_coord_actions:

.. topic:: **How do you change the timeout for Coordinator actions?** 

   Each Coordinator action waits for timeout duration before timing out. 
   For normal running job, the default timeout is two hours. 
   For catchup jobs, the value is infinite. 

   We strongly suggest, however, that users choose a realistic timeout value (in minutes) when defining Coordinator jobs. 
   A timeout of five hours could be defined in ``coordinator.xml`` as follows:

   .. code-block:: xml

      <controls>
          <timeout>300</timeout>
      </controls>


.. _reprocess_coord_actions:

.. topic:: **How do you reprocess Coordinator actions?**

   See `Rerunning a Coordinator Action or Multiple Actions <http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#Rerunning_a_Coordinator_Action_or_Multiple_Actions>`_ and :ref:`Rerun Coordinator Action[s] (Oozie 2.1+) <rerun_coords>`.

.. _update_coord:

.. topic:: **How do you update a Coordinator definition on the fly?**

   To change a Coordinator definition, users can update Coordinator definition in HDFS and issue an 
   ``update`` command. The existing Coordinator definition will be replaced by a new definition. 
   The refreshed Coordinator would keep the same Coordinator ID, state, and Coordinator 
   actions.

   Users can also use the option ``-dryrun`` to validate changes. All created Coordinator actions (including 
   in waiting) will use the old configuration. Users can rerun actions with the ``-refresh`` option, 
   which will use the new configuration to rerun Coordinator actions.

   For example, the following will update the Coordinator definition and action:: 

       $ oozie job -update -config examples/apps/aggregator/job.properties 


.. _long_time_finish:

.. topic:: **Why does Oozie take a long time to update after finishing the corresponding Hadoop job?**


   Oozie receives the external status in two ways:

   - When a Hadoop job finishes, Hadoop makes notifies Oozie.
   - If Oozie don't get the callback in 10 minutes, it proactively queries Hadoop about the job status. 
     The later is used as a fall-back step; however, this step will cause a delay of nearly 10 minutes.

   Reasons why a Hadoop callbacks are not received on-time:

   - Hadoop took a long time to call back Oozie.
   - Hadoop made the callback, but Oozie either missed it or rejected it due to an internal queue overflow.

   How could we discern whether Oozie received the Hadoop callback very late:
   
   - By looking at the Oozie log, we can determine whether there were a lot of late callback received by Oozie.
   - Use the following command: ``grep "E0800: Action it is not running its in \[OK\] state" oozie.log.2010-04-05-* | wc -l``
     If there are lot of lines, that means, Oozie is getting a lot of late callbacks.



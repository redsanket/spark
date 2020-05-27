..  _troubleshooting:

Testing, Debugging, & Troubleshooting
====================================

..  _code_testing:

1.Code Testing
---------------

..  _code_testing_patch:

1.1.Patch Testing
`````````````````

**Run test cases:**

-  Skip all test execution during maven build:
   ``mvn package -DskipTests``
-  Execute All test cases: ``mvn test``
-  Execute specific test group:
   ``mvn test -DincludeGroups=TestGroup1,TestGroup2``
-  Exclude specific test group during execution:
   ``mvn test -DexcludeGroups=TestGroup3,TestGroup4``
-  Execute specific package testcases:
   ``mvn test -Dtest="test.java.com.service.map.**"``
-  Execute specific test: ``mvn test -Dtest=Test1,Test2``
-  Sometimes, you may need to run test inside a loop to make sure that it is stable. Lets assume that we want to test ``TestBalancerRPCDelay``


   .. code-block:: shell-session

     mvn test -Dtest=TestBalancerRPCDelay
     # navigate to the enclosing module
     cd hadoop-hdfs-project/hadoop-hdfs
     mvn test -Dtest=TestBalancerRPCDelay
     # the following command will keep running until it fails
     while :;do mvn surefire:test -Dtest=TestBalancerRPCDelay || break;done


**P.S: The following steps are from the hadoop git repository root folder.**

Prerequsites
'''''''''''''''''''

**TODO: Installing findbugs**

Step1: Create patch
'''''''''''''''''''

Create a patch with your changes like so:

  .. code-block:: shell-session

     git diff commit_1 commit_2 --no-prefix > path_to_patch_file


If you are making chamges to a different branch, say branch-2.10

  .. code-block:: shell-session

     git diff commit_1 commit_2 --no-prefix branch-2.10 > path_to_patch_file


Step2: Run test-patch
'''''''''''''''''''''

``dev-support/bin/test-patch`` can be used to `test the patch <https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute#HowToContribute-Testingyourpatch>`_
with compile, unit tests, checkstyle, whitespace, etc. It wants a clean
git tree so stash changes using ``git stash`` before using ``test-patch``:

  .. code-block:: shell-session

     git stash
     dev-support/bin/test-patch --run-tests
                                --test-parallel=true path_to_patch_file


**TODO: Add findbugs**



It can take quite some time to run all the checks. ``test-patch`` can
also run specific tests, eg, just checkstyle like so:

  .. code-block:: shell-session

     dev-support/bin/test-patch --plugins="maven,checkstyle"
                                --test-parallel=true path_to_patch_file


Above example by default builds first and then runs ``checkstyle``.
``checkstyle`` should be able to run without the build step, which takes
quite a bit of time. I found that I can “skip” that by pressing
``Ctrl+C`` when it is building the code, once for clean tree and once
for the patched code. The new errors introduced by the patch are stored
in a txt file printed at the output.



..  _code_testing_deflaker:

1.2.DeFlaker
````````````


1.2.1.Resources
''''''''''''''''''''''''''

-  ICSE’2018 Paper `DeFlaker: Automatically Detecting Flaky Tests <https://www.jonbell.net/icse18-deflaker.pdf>`_
-  Slides from `our ICSE 2018 talk <https://speakerdeck.com/michaelhilton/icse18-deflaker>`_
-  `Listing of flaky tests detected in historical reruns <https://docs.google.com/spreadsheets/d/1uuCaUck7gdNi-f9UfAROQI8iO1ThSC3XrnQQzd67Rhc/edit?usp=sharing>`_
-  `List of flaky tests confirmed in Hadoop <https://docs.google.com/spreadsheets/d/1cNqfSbG21x8grb2LrUfMSQp5Al1IbIRaFdHcLbDQ1ZY/edit?usp=sharing>`_
-  `DeFlaker: GET RID OF YOUR FLAKES <https://www.deflaker.org/>`_
-  `Apache Maven Project: Rerun Failing Tests <http://maven.apache.org/surefire/maven-surefire-plugin/examples/rerun-failing-tests.html>`_

1.2.2.DeFlaker Pupose
''''''''''''''''''''''''''

DeFlaker is a Maven build extension that identifies when a test is
flaky. DeFlaker declares a test as likely flaky if it fails without
executing any changed code. DeFlaker collects statement-level coverage
for every test run. To reduce the overhead of collecting coverage that
is imposed by existing tools, DeFlaker collects coverage only on the
statements that have changed since the last successful build. When a
test fails, DeFlaker checks if its coverage includes any changed code.
To help debug these flaky tests, DeFlaker can also capture a complete
core dump of the JVM at the time of the test failure. Finally, DeFlaker
re-runs each likely flaky test at the end of the test execution, in an
isolated JVM, to see if it fails again or passes (and records the
output).

1.2.3.Usage
'''''''''''''

1.2.3.1.Adding to the project
.......................................

DeFlaker is deployed on Maven Central, and is compatible with JUnit 4
and TestNG tests that are executed with Maven, using the surefire or
failsafe plugin. To add DeFlaker to your build, add it to your pom.xml:

  .. code-block:: XML

     <build>
       <extensions>
         <extension>
           <groupId>org.deflaker</groupId>
           <artifactId>deflaker-maven-extension</artifactId>
           <version>1.4</version>
         </extension>
       </extensions>
     </build>


DeFlaker adds output like this after your tests run, if a likely flaky
test is detected:

  .. code-block:: shell-session

     [WARNING] FLAKY>> Test TestMiniMRWithDFSWithDistinctUsers.testDistinctUsers failed,
                     but did not appear to run any changed code


This line indicates that a test failed, but didn’t run any changed code.

1.2.3.2.Example usage and output
.......................................

By default, DeFlaker will rerun your failing tests up to 10 times (5
times in the same JVM where the test failed and then for 5 times in new,
clean JVMs). The system property deflaker.rerunFlakies configures this
behavior, specifying the number of times to rerun tests in each mode (in
the same JVM and in the clean JVMs. For instance running
``mvn -Ddeflaker.rerunFlakies=1`` test will cause failing tests to be
re-run once in the JVM they failed in, then once in a clean JVM.

  .. code-block:: shell-session

     mvn -Ddeflaker.rerunFlakies=1 test -Dtest=TestMiniMRWithDFSWithDistinctUsers


If you successfully added DeFlaker to your project, the output of the
execution will have lines like this:

  .. code-block:: shell-session

     [INFO] --- deflaker-maven-plugin:1.4:diff (deflaker-diff) @ hadoop-dist ---
     [INFO]
     [INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ hadoop-dist ---
     [INFO] No sources to compile
     [INFO]
     [INFO] --- maven-surefire-plugin:2.18:test (default-test) @ hadoop-dist ---
     [INFO]
     [INFO] --- maven-surefire-plugin:2.18:test (default-test-rerunfailures) @ hadoop-dist ---
     [INFO]
     [INFO] --- deflaker-maven-plugin:1.4:report (deflaker-report-tests) @ hadoop-dist ---
     [INFO] ------------------------------------------------------------------------
     [INFO] TEST DIFFCOV ANALYSIS
     [INFO] Apache Hadoop Distributionhadoop-dist
     [INFO] ------------------------------------------------------------------------
     [INFO] Using covFile: /Users/ahussein/workspace/repos/community/amahadoop-testMiniMR/hadoop-dist/target/diffcov.log
     [INFO] Using difFile: /Users/ahussein/workspace/repos/community/amahadoop-testMiniMR/.diffCache
     [INFO] No test data found


.. _logging_monitoring_and_alerting:

2.Logging, Monitoring and Alerting
----------------------------------

- **Logs Categories:**

  - Server Logs: record all requests that were made of the server: IP, URL, response, ..etc. they provide access and error logs
  - Application Server Logs: logs generated by applications. This helps understanding how the application is used.
  Logs generated by hadoop daemons can be considered Application level.

- **Types of Logs:**

  - ``.log``: Logs of running daemons will be available here in this .log file.
  - ``.out``: will have startup messages of a daemon. These messages will be useful to troubleshoot startup failures of a daemon.
  - ``.log.{timestamp}.(bz2|gz)``: Old log files will have date in their name. by default log rotation is daily.
  - ``gc-{component}.log-{yyyyMMddHHmm}``: GC logs for each component  (namenode, datanode, ..etc)

Hadoop offers CLI to browse the logs

- **applications logs:** ``yarn logs -applicationId <application ID> [OPTIONS]``
  - am <AM Containers> Prints the AM Container logs for this application.
  - containerId <Container ID> it will only print syslog if the application is running. Work with ``-logFiles`` to get other logs.
  - logFiles <Log File Name> Work with -am/-containerId and specify comma-separated value to get specified container log files.
  Use "ALL" to fetch all the log files for the container.

- **job logs:** ``mapred job [-logs <job-id> <task-attempt-id>]``


.. _logging_monitoring_and_alerting_logs_location:

2.1.Logs location and content
`````````````````````````````

2.1.1.Namenode (/home/gs/var/log/hdfs):
''''''''''''''''''''''''''''''''''''''''''''''''''''

.. table:: Namenode Logs

  +-----------------------------------------------------+----------------------------------------+
  | Log                                                 | Description                            | 
  +=====================================================+========================================+
  | hadoop-hdfs-namenode-{hostname} |br|                |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the namenode process |
  +-----------------------------------------------------+----------------------------------------+
  | gc-namenode.log-{yyyyMMddHHmm}                      | Logs of the GC from namenode           |
  +-----------------------------------------------------+----------------------------------------+
  | gc-balancer.log-{yyyyMMddHHmm}                      | Logs of the GC from balancer           |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-hdfs-balancer-{hostname} |br|                |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the balancer process |  
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-hdfs-balancer-{hostname}.out                 | Messages during startup from balancer  |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-namenode-jetty.log.{yyyy_MM_dd}(.gz)?        | Runtime logs from Jetty for namenode   |
  +-----------------------------------------------------+----------------------------------------+
  | hdfs-audit.log(.{yyyy-MM-dd-HH}.bz2)?               | Accounting all operations              |
  +-----------------------------------------------------+----------------------------------------+
  | hdfs-auth.log(.{yyyy-MM-dd-HH}.bz2)?                | Accounting authentications             |
  +-----------------------------------------------------+----------------------------------------+

2.1.2.Datanode (/home/gs/var/log/hdfs):
''''''''''''''''''''''''''''''''''''''''''''''''''''

.. table:: Datanode Logs

  +-----------------------------------------------------+----------------------------------------+
  | Log                                                 | Description                            | 
  +=====================================================+========================================+
  | hadoop-hdfs-datanode-{hostname} |br|                |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the datanode process |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-datanode-webhdfs.log(.{yyyy-MM-dd}.bz2)?     | Records URL, size, user..etc           |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-hdfs-datanode-{hostname}.out                 | Messages during startup                |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-datanode-jetty.log.{yyyy_MM_dd}(.gz)?        | Access Requests URLs                   |  
  +-----------------------------------------------------+----------------------------------------+
  | gc-datanode.log-{yyyyMMddHHmm}                      | Logs of the GC from datanode process   |
  +-----------------------------------------------------+----------------------------------------+
  | jsvc.out                                            |                                        |
  +-----------------------------------------------------+----------------------------------------+
  | jsvc.err                                            |                                        |
  +-----------------------------------------------------+----------------------------------------+
  
2.1.3.Mapreduce (/home/gs/var/log/mapred):
''''''''''''''''''''''''''''''''''''''''''''''''''''

.. table:: Yarn/Mapreduce Logs

  +-----------------------------------------------------+----------------------------------------+
  | Log                                                 | Description                            | 
  +=====================================================+========================================+
  | yarn-mapred-historyserver-{hostname} |br|           |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from historyserver process|
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-resourcemanager-{hostname} |br|         |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the resource Manager |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-timelineserver-{hostname} |br|          |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the timelineserver   |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-nodemanager-{hostname} |br|             |                                        |
  | .log(.{yyyy-MM-dd-HH}.bz2)?                         | Runtime logs from the nodemanager      |  
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-historyserver-{hostname}.out            | Startup of historyserver               |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-resourcemanager-{hostname}.out          | Startup of resourcemanager             |
  +-----------------------------------------------------+----------------------------------------+
  | yarn-mapred-timelineserver-{hostname}.out           | Startup of teimlineserver              |
  +-----------------------------------------------------+----------------------------------------+
  | mapred-jobsummary.log(.{yyyy-MM-dd}.bz2)?           | Logs the summary of each completed job |
  +-----------------------------------------------------+----------------------------------------+
  | rm-appsummary.log(.{yyyy-MM-dd}.bz2)?               | the summary of each app                |
  +-----------------------------------------------------+----------------------------------------+
  | rm-audit.log(.{yyyy-MM-dd}.bz2)?                    | List of all operations                 |
  +-----------------------------------------------------+----------------------------------------+
  | rm-auth.log(.{yyyy-MM-dd}.bz2)?                     |  logs the authentications for the RM   |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-jobhistory-jetty.log.{yyyy_MM_dd}(.gz)?      | Runtime logs from historyserver Jetty  |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-resourcemanager-jetty.log.{yyyy_MM_dd}(.gz)? | Runtime logs from the jetty RM         |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-timelineserver-jetty.log.{yyyy_MM_dd}(.gz)?  | Runtime logs from the timeline Jetty   |
  +-----------------------------------------------------+----------------------------------------+
  | hadoop-nodemanager-jetty.log.{yyyy_MM_dd}(.gz)?     | Runtime logs from the nodemanager Jetty|
  +-----------------------------------------------------+----------------------------------------+
  | timelineserver-auth.log(.yyy-MM-dd-HH.bz2)?         | Authentications for timelineserver     |
  +-----------------------------------------------------+----------------------------------------+
  | gc-jobhistory.log-{yyyyMMddHHmm}                    | Logs of the GC from jobhistory process |
  +-----------------------------------------------------+----------------------------------------+
  | gc-nodemanager.log-{yyyyMMddHHmm}                   | Logs of the GC from NM process         |
  +-----------------------------------------------------+----------------------------------------+
  | gc-resourcemanager.log-{yyyyMMddHHmm}               | Logs of the GC from RM process         |
  +-----------------------------------------------------+----------------------------------------+
  | gc-timelineserver.log-{yyyyMMddHHmm}                |  GC logs from timelineserver  process  |
  +-----------------------------------------------------+----------------------------------------+

  
.. |br| raw:: html

   <br />

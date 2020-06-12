################
Knowledge Center
################

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

*************************
Commands and Common Tasks
*************************


This section lists useful commands and frequently executed tasks.

- Java PS: ``jps``
- Java Stack: ``jstack``
- ``Hadoop fs -put, -get, -cat, -ls``
- Example of running a test in a loop:

  .. code-block:: bash

    while :;do mvn surefire:test -Dtest=TestJobImpl#testUnusableNodeTransition || break;done


- Build command:

  .. code-block:: bash

    # to compile C code Add -Pnative
    mvn install -Pdist -Dtar -DskipTests -DskipShade -Dmaven.javadoc.skip
    # to build with shading
    mvn clean install -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip

- More repos for linux

  .. code-block:: bash

    --enablerepo=latest-rhel-7-server-optional-rpms
    --enablerepo=latest-rhel-7-server-extras-rpms install docker libcgroup-tools

- Restart hadoop processes on QE clusters:

  .. code-block:: bash

    yinst stop resourcemanager -root /home/gs/gridre/yroot.openqe99blue; yinst start resourcemanager -root /home/gs/gridre/yroot.openqe99blue
    yinst stop namenode -root /home/gs/gridre/yroot.openqe99blue; yinst start namenode -root /home/gs/gridre/yroot.openqe99blue
    yinst stop nodemanager -root /home/gs/gridre/yroot.openqe99blue; yinst start nodemanager -root /home/gs/gridre/yroot.openqe99blue
    yinst stop datanode -root /home/gs/gridre/yroot.openqe99blue; yinst start datanode -root /home/gs/gridre/yroot.openqe99blue

***********************
Hadoop Overview
***********************

*****************************
Hadoop Configurations
*****************************

***************
Metrics
***************

Simon
======

Yamas
======

*************************************
Testing, Debugging, & Troubleshooting
*************************************

.. _hadoop_team_core_code_testing:

Code Testing
============

Patch Testing from Command Line
-------------------------------

.. note:: The following steps are from the hadoop git repository root folder

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

   .. code-block:: bash

     mvn test -Dtest=TestBalancerRPCDelay
     #navigate to the enclosing module
     cd hadoop-hdfs-project/hadoop-hdfs
     mvn test -Dtest=TestBalancerRPCDelay
     #the following command will keep running until it fails
     while :;do mvn surefire:test -Dtest=TestBalancerRPCDelay || break;done

:guilabel:`&Prerequisites`

Install `findbugs`: Download `tarball <http://findbugs.sourceforge.net/downloads.html>`_ and un-tar it.

:guilabel:`&Step1:Create patch`

* Create a patch with your changes like so:

  .. code-block:: bash

     git diff commit_1 commit_2 --no-prefix > path_to_patch_file

* If you are making chamges to a different branch, say branch-2.10

  .. code-block:: bash

     git diff commit_1 commit_2 --no-prefix branch-2.10 > path_to_patch_file
 
:guilabel:`&Step2.Run test-patch`

``dev-support/bin/test-patch`` can be used to `test the patch <https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute#HowToContribute-Testingyourpatch>`_
with compile, unit tests, checkstyle, whitespace, etc. It wants a clean
git tree so stash changes using ``git stash`` before using ``test-patch``:

  .. code-block:: bash

     git stash
     dev-support/bin/test-patch --run-tests
                                --test-parallel=true path_to_patch_file
                                --findbugs-home=FINDBUGS_HOME

* ``FINDBUGS_HOME`` is the directory you un-tared `findbugs`
* I have had a problem with the HTML `findbugs` output; it doesn't tell you what went wrong. You have to look at the XML output file.

.. note::
  Above example by default builds first and then runs ``checkstyle``.
  ``checkstyle`` should be able to run without the build step, which takes
  quite a bit of time. I found that I can “skip” that by pressing
  ``Ctrl+C`` when it is building the code, once for clean tree and once
  for the patched code. The new errors introduced by the patch are stored
  in a txt file printed at the output.


.. _hadoop_team_general_testiong_code_run_in_ide:

Run tests from IDE
------------------

If you have followed the instructions to import the project into the IDE (see :ref:`hadoop_team_getting_started_development_importing_into_ide`), then you should be also to debug and run test cases from the IDE.

* Make sure that you build hadoop enabling ``shading``
* Run the UT at least once from the command line as described in previous section.
* You can navigate recursively in modules to run the UT from the command line.
* Run/Debug the UT from the IDE. If all the dependencies are available, the IDE will successfully debug and run the test.
* If the IDE cannot find depencies, it will show an error `“package does not exist...``

DeFlaker
--------

Resources
^^^^^^^^^

-  ICSE’2018 Paper `DeFlaker: Automatically Detecting Flaky Tests <https://www.jonbell.net/icse18-deflaker.pdf>`_
-  Slides from `our ICSE 2018 talk <https://speakerdeck.com/michaelhilton/icse18-deflaker>`_
-  `Listing of flaky tests detected in historical reruns <https://docs.google.com/spreadsheets/d/1uuCaUck7gdNi-f9UfAROQI8iO1ThSC3XrnQQzd67Rhc/edit?usp=sharing>`_
-  `List of flaky tests confirmed in Hadoop <https://docs.google.com/spreadsheets/d/1cNqfSbG21x8grb2LrUfMSQp5Al1IbIRaFdHcLbDQ1ZY/edit?usp=sharing>`_
-  `DeFlaker: GET RID OF YOUR FLAKES <https://www.deflaker.org/>`_
-  `Apache Maven Project: Rerun Failing Tests <http://maven.apache.org/surefire/maven-surefire-plugin/examples/rerun-failing-tests.html>`_

DeFlaker Purpose
^^^^^^^^^^^^^^^^

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

Usage
^^^^^^

Adding to the project
""""""""""""""""""""""

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

  .. code-block:: bash

     [WARNING] FLAKY>> Test TestMiniMRWithDFSWithDistinctUsers.testDistinctUsers failed,
                     but did not appear to run any changed code


This line indicates that a test failed, but didn’t run any changed code.

Example usage and output
""""""""""""""""""""""""

By default, DeFlaker will rerun your failing tests up to 10 times (5
times in the same JVM where the test failed and then for 5 times in new,
clean JVMs). The system property deflaker.rerunFlakies configures this
behavior, specifying the number of times to rerun tests in each mode (in
the same JVM and in the clean JVMs. For instance running
``mvn -Ddeflaker.rerunFlakies=1`` test will cause failing tests to be
re-run once in the JVM they failed in, then once in a clean JVM.

  .. code-block:: bash

     mvn -Ddeflaker.rerunFlakies=1 test -Dtest=TestMiniMRWithDFSWithDistinctUsers


If you successfully added DeFlaker to your project, the output of the
execution will have lines like this:

  .. code-block:: bash

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

.. _hadoop_team_core_logging_monitoring_and_alerting:

*********************
Monitoring & Alerting
*********************

Dashboards
==========

.. include:: /common/aura-dashboards-table.rst

*******
Logging
*******

Overview
========

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

.. _hadoop_team_core_logging_monitoring_and_alerting_logs_location:

Logs location and content
=========================

.. include:: /common/mapreduce-logs-table.rst

.. include:: /common/hdfs-logs-table.rst
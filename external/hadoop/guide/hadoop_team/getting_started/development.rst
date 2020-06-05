.. _hadoop_team_getting_started_development:

###################
Development
###################

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

*********************
Machine Setup
*********************

..  _on_boarding_mac_env_setup:

Mac Environment Setup
=====================

- Install ``brew`` from the self service application.

- Install Java8 using brew or packages from `Oracle website <https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>`_
  (java8 had been pulled from brew, not sure if it has been put back in).

- Set ``Java_HOME``:

  .. code-block:: bash

    $ vim .bash_profile
    export JAVA_HOME=$(/usr/libexec/java_home)


- Install the following packages using brew

  .. code-block:: bash

    brew install gcc autoconf automake libtool \
               cmake snappy gzip bzip2 zlib openssl maven


- To install protobuf v2.5.0 (no longer available on brew), first get the
  `tarball <https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2>`_
  and untar.
  Then cd into the new folder, configure, make, and install (can optionally also run ``make check`` after ``make`` to run self-tests):

  .. code-block:: bash

    tar xfvj protobuf-2.5.0.tar.bz2
    cd protobuf-2.5.0
    ./configure
    make
    sudo make install

- Add ssh wrapper that skips running ssh when the target is local (~/bin/ssh)

  .. code-block:: bash

    #!/bin/sh

    origargs="$@"
    while [[ $1 == "-o" ]]; do
      shift 2
    done

    if [[ $1 == "localhost" \
     || $1 == "$(hostname -s)" \
     || $1 == "$(hostname)" \
     || $1 == "0.0.0.0" ]];then
       shift
       eval "$@"
    else
       exec /usr/bin/ssh $origargs
    fi


Add the new script to the path by modifying ``~/.bash_profile``

  .. code-block:: bash

    # bypass ssh on localhost
    export PATH=~/bin:$PATH

..  _on_boarding_set_opehouse:

Setting VM on Openhouse
========================

Create a new VM
---------------

**Step1:**
Create a new VM by visiting `yo/openhouse <http://yo/openhouse>`_.
In the instances page, click ``launch instance`` and follow the steps on the popup window. The
instance will get a unique name that rhyme. For example, ``combchrome.corp.ne1.yahoo.com``.
This will be the ID used to log on the VM.

**Step2:**
Setup your access to the VM.

- Wait for 15 minutes allowing the ID to be propagated through the system.
- Refresh your ubkey as explained :ref:`Sec. <hadoop_team_getting_started_onboarding_checklist>`.
- Append the VM name to your ``~/.ssh/config`` as follows:


  .. code-block:: bash

    # define VM alias and ssh parameters
    Host johndoe_vm
          HostName combchrome.corp.ne1.yahoo.com
          ForwardAgent yes


Note that the ``ForwardAgent yes`` is necessary to use your local SSH keys instead of leaving keys
(without passphrases!) sitting on your server.

**Step3:**
ssh the VM.


  .. code-block:: bash

    ssh -A johndoe_vm


That's it! You are working on the new VM.

Install Prerequisites
-----------------------

-  Install Java8 on the system

  .. code-block:: bash

    sudo yum install java-1.8.0-openjdk-devel


-  Set ``JAVA_HOME``: The best way to set ``JAVA_HOME`` is to place the
   line below in ``/etc/profile`` which assures that the ``JAVA_HOME``
   will be updated when a different version of Java is selected through
   the alternatives. After adding the line, open a new login shell.


  .. code-block:: bash

    sudo vim /etc/profile.d/java_dev.sh


  .. code-block:: bash

    ## content of java_dev.sh
    export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
    # add JAVA_HOME to the classpath
    export PATH=${JAVA_HOME}/bin:$PATH


-  Enable epel

  .. code-block:: bash

    cd /tmp
    wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    ls *.rpm
    sudo yum install epel-release-latest-7.noarch.rpm


-  Install Packages:

  .. code-block:: bash

    sudo yum install --enablerepo=y* --enablerepo=latest* git gcc-c++
    sudo yum --enablerepo=y* --enablerepo=latest* --enablerepo=epel install protobuf \
         protobuf-compiler protobuf-devel
    sudo yum install maven


-  Install cmake:

  .. code-block:: bash

    sudo yum --enablerepo=y* --enablerepo=latest* --enablerepo=epel install cmake3
    mkdir ~/bin
    cd ~/bin
    ln -s /usr/bin/cmake3 cmake


-  Add Maven and cmake3 to ``PATH``:

  .. code-block:: bash

    vim ~/.bash_profile
    export PATH=$HOME/bin:/usr/share/maven/bin:$PATH
    source ~/.bash_profile

Optional Steps for VM
^^^^^^^^^^^^^^^^^^^^^

#. **Install maven with a specific release**

   In some cases the maven installed by `yum` is dated, and some of the plugins will fail during
   hadoop build. In order to insall more recent version:

   * get the download link from the `official Apache maven website <https://maven.apache.org/download.cgi>`_
   * Extract the binaries

     .. code-block:: bash

       cd /usr/local/src
       wget http://www-us.apache.org/dist/maven/maven-3/3.5.4/binaries/apache-maven-3.x.x-bin.tar.gz
       tar -xf apache-maven-3.x.x-bin.tar.gz
       ln -s /usr/local/src/apache-maven-3.x.x /usr/local/maven

   * Configure the environments variables to pre-compiled Apache Maven files on our system by creating a configuration file `maven.sh` in the `/etc/profile.d` directory

     .. code-block:: bash

       cd /etc/profile.d/
       vim maven.sh

   * Add the following configuration in ``maven.sh`` configuration file.

     .. code-block:: bash

       # Apache Maven Environment Variables
       # MAVEN_HOME for Maven 1 - M2_HOME for Maven 2
       export M2_HOME=/usr/local/maven
       export PATH=${M2_HOME}/bin:${PATH}

   * Make the `maven.sh` configuration file executable and then load the configuration by running the `source` command.

     .. code-block:: bash

       chmod +x maven.sh
       source /etc/profile.d/maven.sh

#. **Installing yinst**

  By default, ``yinst`` builds of Red Hat Enterprise Linux Advanced Server 4.x, 5.x, and 6.x on both
  32-bit and 64-bit systems. To install for rehl7:

   * Assuming rpm is setup:

     .. code-block:: bash

       $>rpm -qa |grep yinst
       yinst-8.1.0-53.el7.x86_64
       $>yum i yinst

   * Also

     .. code-block:: bash

       $>yum whatprovides yinst
       Loaded plugins: versionlock
       Repository ygrid is listed more than once in the configuration
       dps-rpms-stable                                                                                                                                   10/10
       oath-rdrs-release                                                                                                                                   8/8
       oath-rpms-stable                                                                                                                                  87/87
       paranoids_rpm-stable                                                                                                                              1367/1367
       ygrid-stable                                                                                                                                    60/60


   * if the output is empty then:

     .. code-block:: bash

       $>yum whatprovides yinst
       #if you don’t already have this file, you can try creating it
       #it should be like that:
       $> cat /etc/yum.repos.d/oath-rpms-stable.repo
       [oath-rpms-stable]
       name=oath-rpms-stable
       baseurl=https://edge.artifactory.ouroath.com:4443/artifactory/oath-rpms/7Server/stable/x86_64
       enabled=1
       gpgcheck=0
       $> yum install yinst



*********************
Contribute to Yhadoop
*********************

Setup github
============

Ask to be added to the hadoop-core team contributors using your username.
You should receive an email confirming you became a member on github.

Follow the `github instructions <https://help.github.com/enterprise/2.15/user/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent>`_ to generate your ssh keys

Add your ssh key to the ssh agent following these instructions (adapted from the
`github (adding your ssh to the agent) page. <https://help.github.com/enterprise/2.15/user/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#adding-your-ssh-key-to-the-ssh-agent>`_

  .. code-block:: bash

    eval "$(ssh-agent -s)"

Add the following to your ssh config file ``~/.ssh/config``:

  .. code-block:: bash

    Host git.ouroath.com
           User jdoe
           AddKeysToAgent yes
           UseKeychain yes
           IdentityFile ~/.ssh/id_rsa

The default name for the private key is ``id_rsa``. Please replace it
appropriately if you are using a different name. Add the SSH private key
to the ssh-agent:


  .. code-block:: bash

   ssh-add -K ~/.ssh/id_rsa


Sign in to the enterprise github and goto the `profile settings <https://git.ouroath.com/settings/keys>`_. Add your ssh key to the ssh agent following `these instructions <https://help.github.com/enterprise/2.15/user/articles/adding-a-new-ssh-key-to-your-github-account/>`_.


Testing your ssh connection as explained on `github manuals <https://help.github.com/enterprise/2.15/user/articles/testing-your-ssh-connection/>`_, using the
correct domain name as illustrated below.

  .. code-block:: bash

    ssh -T git@git.ouroath.com
    # Attempts to ssh to GitHub Enterprise

Set your global git config file ``~/.gitconfig``

  .. code-block:: bash

    [user]
            name = John Doe
            email = john.doe@verizonmedia.com
    [core]
            pager = less -FRX
            autocrlf = input
    [color]
            ui = auto
    [alias]
            co = checkout
    [pull]
            rebase = true
    [branch]
            autosetuprebase = always


Getting the Source Code
=======================

These steps assume that you are working on feature ``YHADOOP-9999``

-  Go to the `hadoop git UI <https://git.ouroath.com/hadoop/Hadoop>`_ and and press the ``fork`` button in GitHub. Let's
   assume the new project is ``https://git.ouroath.com/jdoe/Hadoop.git``

- Create a local copy of the fork using terminal.

  .. code-block:: bash

     git clone git@git.ouroath.com:jdoe/Hadoop.git yhadoop-9999
     cd yhadoop-9999

- Set the upstream repository: Add the haddop/yhadoop repository as a remote in order to be able to
  bring changes into the local copy.

  .. code-block:: bash

     git remote rename origin rjdoe
     git remote add ryahoo git@git.ouroath.com:hadoop/Hadoop.git

- Verify that the remotes are set correctly

  .. code-block:: bash

     git remote -v
     > rjdoe git@git.ouroath.com:jdoe/Hadoop.git (fetch)
     > rjdoe git@git.ouroath.com:jdoe/Hadoop.git (push)
     > ryahoo    git@git.ouroath.com:hadoop/Hadoop.git (fetch)
     > ryahoo    git@git.ouroath.com:hadoop/Hadoop.git (push)

- Create branch: Chose the main branch that used for development.
  In our case, let's assume it is  "y-branch-2.10". Then we create a new file:

  .. code-block:: bash

   $ git checkout  y-branch-2.10
   $ git pull ryahoo  y-branch-2.10 && git push rjdoe y-branch-2.10
   $ git checkout -b yhadoop-9999
   $ echo "[YHADOOP-9999]: Brief description of the issue" > Y-CHANGES/YHADOOP-9999
   $ git add Y-CHANGES/YHADOOP-9999

Pushing Changes and Pull Requests (PRs)
=======================================

- Remember to test your changes before creating a PR.
  See Sec. :ref:`hadoop_team_core_code_testing` for a full guide on running
  Unit tests andtesting patches.

- After you make your changes, it is recommended that you ``rebase`` (see below).

  .. code-block:: bash

    # Push changes to new remote branch
    git push -u rjdoe yhadoop-9999

- If you want to rebase your branch. Assuming you are on branch yhadoop-9999:

  .. code-block:: bash

     $ git add --all
     $ git commit -m "[YHADOOP-9999]: COMMIT MESSAGE"
     $ git checkout y-branch-2.10
     $ git pull ryahoo  y-branch-2.10 && git push jdoe y-branch-2.10
     $ git checkout yhadoop-9999
     $ git rebase -i y-branch-2.10
     $ # interactive console to pick and squash commits
     $ git push -u -f rjdoe yhadoop-9999

- Create Pull request
  - In the Git interface, navigate to your local project. You should find the new branch listed at the top. Click "Compare & pull request".
  - Put the Jira number and brief description as the title of the PR.

*********************
Yhadoop Runtime
*********************

Building YHadoop
=================

From the command line, navigate to the hadoop root directory:

  .. code-block:: bash

    mvn install -Pdist -Dtar -DskipTests -DskipShade -Dmaven.javadoc.skip

If there are errors when running jobs on this compiled version, try
doing a clean build without skipping shade.

  .. code-block:: bash

    mvn clean install -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip

Symptoms might look like the following in the logs:

  .. code-block:: bash

    Exception in thread "main" java.lang.VerifyError:
          Inconsistent stackmap frames at branch target 160

To build native, add the ``-Pnative`` flag. We don’t support running
natively on Mac. If you're annoyed with the new animal-sniffer plugin
slowing down the trunk builds and don't need the JDK signature check for
your build, you can add ``-Danimal.sniffer.skip`` to the mvn command
line to skip the slow signature checking.

Making Changes on local Machine
================================

Importing Project into IDE
--------------------------

In order for the IDE to find all required dependency, it is recommended you build hadoop without ``-DskipShade``.

:guilabel:`Eclipse`


(Taken from `BUILDING.txt <https://git.ouroath.com/hadoop/Hadoop/blob/y-branch-2.10/BUILDING.txt>`_)
file in Hadoop git). When you import the project to eclipse, install ``hadoop-maven-plugins`` at first.

  .. code-block:: bash

    cd hadoop-maven-plugins
    mvn install

Then, generate eclipse project files (from root Hadoop directory).

  .. code-block:: bash

    mvn eclipse:eclipse -DskipTests


At last, import to eclipse by specifying the root directory of the project via
[File] -> [Import] -> [Existing Projects into Workspace].

Also look at `Eclipse page <https://wiki.apache.org/hadoop/EclipseEnvironment>`_.

:guilabel:`IntelliJ`

**Step1:** Import Project and select the directory with the cloned git Hadoop repository. On the Import Project screen, select Maven and click next.

.. figure:: /images/team_onboarding/intellij/intellij-1.png
   :alt:  step 1 intellij import: create personal domain

   step 1 intellij import: create personal domain


**Step2:** Keep all the default options except JDK. For JDK, select the installed version and click next.

.. figure:: /images/team_onboarding/intellij/intellij-2.png
   :alt:  Importing Hadoop projects to IntelliJ - Step 2

   Importing Hadoop projects to IntelliJ - Step 2

**Step3:** For profiles, you do not have to do anything and click next.

.. figure:: /images/team_onboarding/intellij/intellij-3.png
   :alt:  Importing Hadoop projects to IntelliJ - Step 3

   Importing Hadoop projects to IntelliJ - Step 3

**Step4:** On the next screen, hadoop-main:2.8.6-SNAPSHOT will be automatically selected. You do not have to make any changes. Click next.

.. figure:: /images/team_onboarding/intellij/intellij-4.png
   :alt:  Importing Hadoop projects to IntelliJ - Step 4

   Importing Hadoop projects to IntelliJ - Step 4

**Step5:** There should be only one SDK on the next screen (the installed SDK). Click next.

.. figure:: /images/team_onboarding/intellij/intellij-5.png
   :alt:  Importing Hadoop projects to IntelliJ - Step 5

   Importing Hadoop projects to IntelliJ - Step 5

**Step6:** You can keep the default name or change it, but make sure the project file location points to the directory where you cloned the git repository. Click finish to complete the setup process. It will take a few minutes for IntelliJ to update its index and populate the project tree.

.. figure:: /images/team_onboarding/intellij/intellij-6.png
   :alt:  Importing Hadoop projects to IntelliJ - Step 6

   Importing Hadoop projects to IntelliJ - Step 6


Coding Style
------------

* Code must be formatted according to `Sun's conventions <http://www.oracle.com/technetwork/java/javase/documentation/codeconvtoc-136057.html/>`_, with one exception:

   * Indent two spaces per level, not four.

* All public classes and methods should have informative `Javadoc comments <http://java.sun.com/j2se/javadoc/writingdoccomments/>`_

   * Do not use ``@author`` tags.


The easiest way to do that is the following:

#. Import your project into IntelliJ
#. Download `google java style from github <https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml/>`_
#. Open the project in IntelliJ
#. :menuselection:`Preferences --> Editor --> Code Style --> Java --> schema --> Settings`.
#. Select ``import schema`` and point to ``intellij-java-google-style.xml``.
#. :menuselection:`Tabs and Indents` in the same window. Then set ``Tab size``, ``indent`` and ``continuation`` to 2, 2, and 4 respectively.
#. :menuselection:`Wrapping and Braces --> Hard Wrap at`. Set it to 80
#. Apply your changes.


.. seealso:: 	**CheckStyle-Idea**, a plugin that provides both real-time and on-demand scanning of Java files with CheckStyle from within IDEA.
            Usage of this plugin is limited as it does not check Unit test coding style.
            You can install the plugin from the `official plugin page <https://plugins.jetbrains.com/plugin/1065-checkstyle-idea/>`_




Running YHadoop (Single Node)
=============================

The following steps works for both Linux and OS X.
For OS X, make sure that you followed the steps of setting ``ssh localhost``
in :ref:`on_boarding_mac_env_setup`.

**Step1:**

Create hadoop instance folder to extract the hadoop image created
by the build (replace paths as needed)

  .. code-block:: bash

     mkdir -p $HOME/workspace/yhadoop-inst
     cp $HOME/workspace/repo/yhadoop/hadoop-dist/target/hadoop-3.1.0-SNAPSHOT.tar.gz \
        $HOME/workspace/yhadoop-inst
     cd $HOME/workspace/yhadoop-inst
     tar -xzvf hadoop-3.1.0-SNAPSHOT.tar.gz

**Step2:**

In the ``yhadoop-inst`` folder, Create directory for the HDFS
``hdfs-trunk`` and a symbolic link ``hdfs`` pointing to the newly
created directory. Also, Create a symbolic link ``hadoop-root`` pointing
to ``hadoop-3.1.0-SNAPSHOT``. Finally add subfolder checkpoint, data,
name to hdfs-trunk

  .. code-block:: bash

     mkdir -p hdfs-trunk
     ln -s hdfs-trunk hdfs
     ln -s hadoop-3.1.0-SNAPSHOT hadoop-root
     mkdir -p  hdfs-trunk/checkpoint hdfs-trunk/data hdfs-trunk/name

**Step2:**

In the ``yhadoop-inst`` folder, Create directory for the HDFS
``hdfs-trunk`` and a symbolic link ``hdfs`` pointing to the newly
created directory. Also, Create a symbolic link ``hadoop-root`` pointing
to ``hadoop-3.1.0-SNAPSHOT``. Finally add subfolder checkpoint, data,
name to hdfs-trunk

  .. code-block:: bash

     mkdir -p hdfs-trunk
     ln -s hdfs-trunk hdfs
     ln -s hadoop-3.1.0-SNAPSHOT hadoop-root
     mkdir -p  hdfs-trunk/checkpoint hdfs-trunk/data hdfs-trunk/name

**Step3:**

Create configuration folder for Hadoop-fs. Download the following
file, :download:`yhadoop-conf </resources/yhadoop-conf.tar.gz>`,
and untar it to the the conf folder you create.
Make sure that you fix the path in those files: hdfs-site.xml,
mapred-site.xml, yarn-site.xml (say ``$HOME/workspace/yhadoop-inst``).


**Step4:**

Set the following environment variables according to the correct
path

  .. code-block:: bash

     export HADOOP_PREFIX=$HOME/workspace/yhadoop-inst/hadoop-root
     export PATH="$PATH:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin"
     export HADOOP_CONF_DIR=$HOME/workspace/yhadoop-conf
     export HADOOP_PID_DIR=$HOME/workspace/yhadoop-inst/pid
     export HADOOP_LOG_DIR=$HOME/workspace/yhadoop-inst/logs
     export HADOOP_COMMON_HOME=$HADOOP_PREFIX
     export HADOOP_HDFS_HOME=$HADOOP_PREFIX
     export HADOOP_YARN_HOME=$HADOOP_PREFIX
     export HADOOP_MAPRED_HOME=$HADOOP_PREFIX
     export HADOOP_MAPRED_LOG_DIR=$HADOOP_LOG_DIR
     export YARN_CONF_DIR=$HADOOP_CONF_DIR
     export YARN_PID_DIR=$HADOOP_PID_DIR
     export YARN_LOG_DIR=$HADOOP_LOG_DIR


For Hadoop-3 use the following env_variables:

  .. code-block:: bash

    export HADOOP_HOME=$HOME/workspace/hadoop-inst/hadoop-root
    export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
    export HADOOP_CONF_DIR=$HOME/workspace/hadoop-conf
    export HADOOP_PID_DIR=$HOME/workspace/hadoop-inst/pid
    export HADOOP_LOG_DIR=$HOME/workspace/hadoop-inst/logs
    export HADOOP_COMMON_HOME=$HADOOP_HOME
    export HADOOP_HDFS_HOME=$HADOOP_HOME
    export HADOOP_YARN_HOME=$HADOOP_HOME
    export HADOOP_MAPRED_HOME=$HADOOP_HOME
    export HADOOP_MAPRED_LOG_DIR=$HADOOP_LOG_DIR


**Step5:** Runn the HDFS

-  First time, you need to format the namenode

  .. code-block:: bash

     hadoop namenode -format

-  Run hadoop dfs daemons and create home directory in HDFS

  .. code-block:: bash

     start-dfs.sh
     hadoop fs -mkdir -p /user/ahussein

If you get a
``localhost: ssh: connect to host localhost port 22: Connection refused``
on Macs, then go to [System Preferences] -> [Sharing] and check [Remote
Login].

-  Start Yarn

  .. code-block:: bash

     start-yarn.sh

-  Start the History Server

  .. code-block:: bash

     mr-jobhistory-daemon.sh start historyserver

-  Populate the HDFS with a file

  .. code-block:: bash

     hadoop fs -put /etc/services .

**Step6:**
Running Jobs

-  Start up the Wordcount job

  .. code-block:: bash

     hadoop jar \
         $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0-SNAPSHOT.jar \
                wordcount services wcout


-  Start up the wordcount job with a input file format map slit size of 100000

  .. code-block:: bash

     hadoop jar \
         $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0-SNAPSHOT.jar \
                wordcount \
                -Dmapreduce.input.fileinputformat.split.maxsize=100000 \
                services wcout2

**Step7:**
Visit the dashboard

Open this URL in browser: `http://localhost:8088 <http://localhost:8088>`_

**Step8:**
Stopping HDFS


.. code-block:: bash

  mr-jobhistory-daemon.sh stop historyserver
  stop-yarn.sh
  stop-dfs.sh


Deploying YHadoop on Internal Cluster (QE cluster)
==================================================

**Step1:**

Ask Nathan Roberts to assign a QE cluster to you from `yo/flubber <https://yo/flubber>`_: Let's say ``openqe99blue``.

**Step2:**

-  Make sure that you already have access to Oath grid
-  Ask Raj to add your userID to get access to `yo/hadoop-deploy`_.
-  Ask Raj to add you to the group ``ygrid_netgroup_griddev``


**Step3:**

-  Go to the `yo/hadoop-deploy`_
-  Click on ``build with parameters``
-  Fill in the ``CLUSTER`` field with teh name of the cluster you just
   picked (i.e., openqe99blue)
-  Select the Version you want to deploy from the drop down
   ``HADOOP_RELEASE_TAG``
-  [Optional] remove TEZ version
-  [Optional] Set ``RHEL7_DOCKER_DISABLED`` to true if you have
   ``DOCKER_IMAGE_TAG_TO_USE`` set to ``rhel6``
-  Click ``Build``
-  The GUI will create a job with an accessible link. Keep it for
   reference and wait until the build is successful
-  If it fails, Check the ``console output`` and address the problem and
   rebuild.


**Step4:**

This step assumes that the build is successful.

-  you can access hadoop through the browser using url such as

   -  `https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster`_
   -  `https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes`_


-  Go to terminal and login to the cluster.

   -  init the key using the ``yinit`` command

   -  ``ssh -A openqe99blue-n1.blue.ygrid.yahoo.com``

   -  You can get Hadoop path by running ``echo $HADOOP_PREFIX``. It
      should be something similar to ``/home/gs/hadoop/current``

   -  Jar files will be in the ``share`` folder
      ``$HADOOP_PREFIX/share/hadoop/``

   -  use ``scp`` to replace the jar files you have modified, on all the
      nodes listed in the hadoop cluster web page (i.e.,
      `https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes`_)

      .. code-block:: bash

         scp HADOOP_PREFIX/share/hadoop/hdfs/hadoop-hdfs-client-2.8.6-SNAPSHOT.jar \
                    hussein@openqe99blue-n2.blue.ygrid.yahoo.com:/home/
         ssh -A openqe99blue-n2.blue.ygrid.yahoo.com
         @openqe99blue-n2$ sudo mv hadoop-hdfs-client-2.8.6-SNAPSHOT.jar \
                               $HADOOP_PREFIX/share/hadoop/hdfs/


**Step5:**

Restart the services namenode, datanode, resourcemanager, and
nodemanager


  .. code-block:: bash

     yinst stop namenode -root /home/gs/gridre/yroot.openqe99blue
     yinst start namenode -root /home/gs/gridre/yroot.openqe99blue


Ignore the memory error you get while starting the service

  .. code-block:: bash

     Java HotSpot(TM) 64-Bit Server VM warning: Failed to reserve shared \
          memory. (error = 12)``

**Step6:**

Initialize user for Kerberos database


  .. code-block:: bash

     kinit jdoe@Y.CORP.YAHOO.COM


.. _`yo/hadoop-deploy`: https://re100.ygrid.corp.gq1.yahoo.com:4443/jenkins/job/Hadoop-Cluster-Deploy-Grid-VM/
.. _`https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster`: https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster
.. _`https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes`: https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes

If you forget to run ``kinit``, you may see an error like that:

  ::

    19/03/11 20:08:58 WARN ipc.Client: Exception encountered while
    connecting to the server : javax.security.sasl.SaslException: GSS
    initiate failed [Caused by GSSException: No valid credentials
    provided (Mechanism level: Failed to find any Kerberos tgt)]

    java.io.IOException: Failed on local exception:
    javax.security.sasl.SaslException: GSS initiate failed [Caused by
    GSSException: No valid credentials provided (Mechanism level: Failed
    to find any Kerberos tgt)]; Host Details : local host is:
    "openqe99blue-n2.blue.ygrid.yahoo.com/10.215.78.31"; destination host
    is: "openqe99blue-n2.blue.ygrid.yahoo.com":8020;

**Step7:**

-  Run a wordcount job

  .. code-block:: bash

     hadoop jar \
         $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.5.9.1903110101.jar \
                wordcount services wcout

-  Run SleepJob

  .. code-block:: bash

     hadoop jar \
         $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.8.5.9.1903110101-tests.jar \
             sleep -m 1 -r 1 -rt 1200000 -mt 20

Parameters used for the sleepJob:

  ::

     "-m": number of mappers
     "-r": number of reducers
     "-mt": map sleep time
     "-rt": reduce sleepTime
     "-recordt": Record sleepTime

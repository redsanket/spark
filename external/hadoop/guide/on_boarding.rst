..  _on_boarding:

On Boarding
===========

..  _on_boarding_get_source_code:

Get the source code
-------------------

Ask to be added to the hadoop-core team contributors using your username.
You should recieve an email confirming you became a member on github.

Generate new ssh-keys
~~~~~~~~~~~~~~~~~~~~~

**Step1:**
Follow the `github instructions <https://help.github.com/enterprise/2.15/user/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent>`_ to generate your ssh keys

**Step2:**
Add your ssh key to the ssh agent following these instructions (adapted from the `github instructions <https://help.github.com/enterprise/2.15/user/articles/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#adding-your-ssh-key-to-the-ssh-agent>`_

.. code:: console

  eval "$(ssh-agent -s)"

Add the following to your ssh config file ``~/.ssh/config``:

.. code:: console

  Host git.ouroath.com
         User jdoe
         AddKeysToAgent yes
         UseKeychain yes
         IdentityFile ~/.ssh/id_rsa


The default name for the private key is ``id_rsa``. Please replace it
appropriately if you are using a different name. Add the SSH private key
to the ssh-agent:


.. code-block:: console

  ssh-add -K ~/.ssh/id_rsa


**Step3:**
Sign in to the enterprise github and goto the `profile settings <https://git.ouroath.com/settings/keys>`_. Add your ssh key to the ssh agent following `these instructions <https://help.github.com/enterprise/2.15/user/articles/adding-a-new-ssh-key-to-your-github-account/>`_.

**Step4:**
Testing your ssh connection as explained on `github manuals <https://help.github.com/enterprise/2.15/user/articles/testing-your-ssh-connection/>`_, using the
correct domain name as illustrated below.

.. code-block:: console

  ssh -T git@git.ouroath.com
  # Attempts to ssh to GitHub Enterprise


**Step5:** Set your global git config file ```~/.gitconfig``

.. code-block:: console

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



..  _on_boarding_mac_env_setup:

Mac Environment Setup
---------------------

- Install ``brew`` from the self service application.

- Install Java8 using brew or packages from `Oracle website <https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>`_ (java8 had been pulled from brew, not sure if it has been put back in).

- Set Java_HOME:

  .. code-block:: console

    $ vim .bash_profile
    export JAVA_HOME=$(/usr/libexec/java_home)


- Install the following packages using brew

  .. code-block:: console

    brew install gcc autoconf automake libtool \
               cmake snappy gzip bzip2 zlib openssl maven


- To install protobuf v2.5.0 (no longer available on brew), first get the `tarball <https://github.com/protocolbuffers/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2>`_ and untar. Then cd into the new folder, configure, make, and install (can optionally also run ``make check`` after ``make`` to run self-tests):

  .. code-block:: console

    tar xfvj protobuf-2.5.0.tar.bz2
    cd protobuf-2.5.0
    ./configure
    make
    sudo make install

- Add ssh wrapper that skips running ssh when the target is local (~/bin/ssh)

  .. code-block:: console

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
  

Add the new script to the path by modifying ~/.bash_profile

.. code-block:: console

  # bypass ssh on localhost
  export PATH=~/bin:$PATH

..  _on_boarding_set_opehouse:

Setting VM on Openhouse
-----------------------

Create a new VM
~~~~~~~~~~~~~~~~

**Step1:**
Create a new VM by visiting `yo/openhouse <http://yo/openhouse>`_.
In the instances page, click ``launch instance`` and follow the steps on the popup window. The
instance will get a unique name that rhyme. For example, ``combchrome.corp.ne1.yahoo.com``.
This will be the ID used to log on the VM.

**Step2:**
Setup your access to the VM.

- Wait for 15 minutes allowing the ID to be propagated through the system.
- Refresh your ubkey as explained in the [new-memebrs guide](new-members#1-setting-up-your-environment).
- Append the VM name to your ``~/.ssh/config`` as follows:


  .. code-block:: console

    # define VM alias and ssh parameters
    Host johndoe_vm
          HostName combchrome.corp.ne1.yahoo.com
          ForwardAgent yes


Note that the ``ForwardAgent yes`` is necessary to use your local SSH keys instead of leaving keys
(without passphrases!) sitting on your server.

**Step3:**
ssh the VM.


.. code-block:: console

  ssh johndoe_vm


That's it! You are working on the new VM.

Install Prerequisities
~~~~~~~~~~~~~~~~~~~~~~

-  Install Java8 on the system

  .. code-block:: console

    sudo yum install java-1.8.0-openjdk-devel


-  Set ``JAVA_HOME``: The best way to set ``JAVA_HOME`` is to place the
   line below in ``/etc/profile`` which assures that the ``JAVA_HOME``
   will be updated when a different version of Java is selected through
   the alternatives. After adding the line, open a new login shell.


  .. code-block:: console

    export JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")


-  Enable epel

  .. code-block:: console

    cd /tmp
    wget https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
    ls *.rpm
    sudo yum install epel-release-latest-7.noarch.rpm


-  Install Packages:

.. code-block:: console

  sudo yum install --enablerepo=y* --enablerepo=latest* git gcc-c++
  sudo yum --enablerepo=y* --enablerepo=latest* --enablerepo=epel install protobuf \
       protobuf-compiler protobuf-devel
  sudo yum install maven


-  Install cmake:

  .. code-block:: console

    sudo yum --enablerepo=y* --enablerepo=latest* --enablerepo=epel install cmake3
    mkdir ~/bin
    cd ~/bin
    ln -s /usr/bin/cmake3 cmake


-  Add Maven and cmake3 to PATH:

  .. code-block:: console

    vim ~/.bash_profile
    export PATH=$HOME/bin:/usr/share/maven/bin:$PATH
    source ~/.bash_profile


.. _on_boarding_contributing-to-yhadoop:

Contributing to YHadoop
-----------------------


.. _on_boarding_create_pr:

Creating Pull Requests
~~~~~~~~~~~~~~~~~~~~~~


Assuming that you're working on Jira issue "YHADDOP-1818"

**Step1:**

Create a working copy of yhadoop

-  Go to the git UI and and press the “fork” button in GitHub. Let's
   assume the new project is ``ahussein/Hadoop``

-  Create a local copy of the fork using terminal.

  .. code-block:: console

     git clone git@git.ouroath.com:ahussein/Hadoop.git yhadoop-1818
     cd yhadoop-1818


**Step2:**

Set the upstream repository

Add the haddop/yhadoop repository as a remote in order to be able to
bring changes into the local copy.


.. code-block:: console

   git remote rename origin rahussein
   git remote add ryahoo git@git.ouroath.com:hadoop/Hadoop.git


Verify that the remotes are set correctly

.. code-block:: console

   git remote -v
   > rahussein git@git.ouroath.com:ahussein/Hadoop.git (fetch)
   > rahussein git@git.ouroath.com:ahussein/Hadoop.git (push)
   > ryahoo    git@git.ouroath.com:hadoop/Hadoop.git (fetch)
   > ryahoo    git@git.ouroath.com:hadoop/Hadoop.git (push)

**Step3:**

Create branch

Chose the main branch that used for development. In our case, let's assume it is  "y-branch-2.8". Then we create a new file


.. code-block:: console

   $ git checkout  y-branch-2.8
   $ git pull ryahoo  y-branch-2.8 && git push rahussein  y-branch-2.8
   $ git checkout -b yhadoop-1818
   $ echo "[YHADOOP-1818]: Brief description of the issue" > Y-CHANGES/YHADOOP-1818
   $ git add Y-CHANGES/YHADOOP-1818


-  ensure you are on y-branch-2.8r branch.
-  git pull command syncs the local copy with the upstream project
-  git push syncs the changes to the forked project.
-  Create new branch named after the jira number.


**Step4:**

Commit changes and push

After you commit your changes, push to the branch of the local repository:

.. code-block:: console

   git push -u rahussein yhadoop-1818
   
- If you want to rebase your branch. Assuming you are on branch yhadoop-1818:

  .. code-block:: console

     $ git add --all
     $ git commit -m "[YHADOOP-1818]: COMMIT MESSAGE"
     $ git checkout y-branch-2.8
     $ git pull ryahoo  y-branch-2.8 && git push rahussein  y-branch-2.8
     $ git checkout yhadoop-1818
     $ git rebase -i y-branch-2.8
     $ ## interactive console to pick and squash commits
     $ git push -u -f rahussein yhadoop-1818

**Step5:**

Create Pull request

-  In the Git interface, navigate to your local project. You should find the new branch listed at the top. Click "Compare & pull request".
-  Put the Jira number and brief description as the title of the PR.


.. _on_boarding_testing_patch:

Testing patch
~~~~~~~~~~~~~

The following steps are from the hadoop git repository root folder.

**Step1:**

Create a patch with your changes like so:

.. code-block:: console

  git diff commit_1 commit_2 --no-prefix > path_to_patch_file


**Step2:**

Run test-patch

``dev-support/bin/test-patch`` can be used to `test the patch <https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute#HowToContribute-Testingyourpatch>`_ with
compile, unit tests, checkstyle, whitespace, etc. It wants a clean git
tree so stash changes using ``git stash`` before using ``test-patch``:

.. code-block:: console

  git stash
  dev-support/bin/test-patch --run-tests --test-parallel=true path_to_patch_file

It can take quite some time to run all the checks. ``test-patch`` can
also run specific tests, eg, just checkstyle like so:

.. code-block:: console

  dev-support/bin/test-patch --plugins="maven,checkstyle" --test-parallel=true path_to_patch_file


Above example by default builds first and then runs ``checkstyle``.
``checkstyle`` should be able to run without the build step, which takes
quite a bit of time. I found that I can "skip" that by pressing
``Ctrl+C`` when it is building the code, once for clean tree and once
for the patched code. The new errors introduced by the patch are stored
in a txt file printed at the output.


.. _on_boarding_building-yhadoop:

Building YHadoop
----------------

.. _on_boarding_mac-local:

Mac local
~~~~~~~~~

From the command line, navigate to the hadoop root directory:

.. code-block:: console

  mvn install -Pdist -Dtar -DskipTests -DskipShade -Dmaven.javadoc.skip


If there are errors when running jobs on this compiled version, try
doing a clean build without skipping shade.


.. code-block:: console

  mvn clean install -Pdist -Dtar -DskipTests -Dmaven.javadoc.skip

Symptoms might look like the following in the logs:
``Exception in thread "main" java.lang.VerifyError: Inconsistent stackmap frames at branch target 160``

To build native, add the ``-Pnative`` flag. We don’t support running
natively on Mac If you're annoyed with the new animal-sniffer plugin
slowing down the trunk builds and don't need the JDK signature check for
your build, you can add ``-Danimal.sniffer.skip`` to the mvn command
line to skip the slow signature checking.

.. _on_boarding_running-yhadoop:

Running YHadoop
---------------

**Step1:**
Create hadoop instance folder to extract the hadoop image created
by the build (replace paths as needed)

.. code-block:: console

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

.. code-block:: console

   mkdir -p hdfs-trunk
   ln -s hdfs-trunk hdfs
   ln -s hadoop-3.1.0-SNAPSHOT hadoop-root
   mkdir -p  hdfs-trunk/checkpoint hdfs-trunk/data hdfs-trunk/name

**Step3:**

Create configuration folder for Hadoop fs. Download the following
file, :download:`yhadoop-conf <resources/yhadoop-conf.tar.gz>` , and untar it to the the conf folder you create.
Make sure that you fix the path in those files: hdfs-site.xml,
mapred-site.xml, yarn-site.xml (say ``$HOME/workspace/yhadoop-inst``).

**Step4:**

Set the following environment variables accoring to the correct
path

.. code-block:: console

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


For Hadoop 3 use the following env_variables:

.. code-block:: console

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

  .. code-block:: console

     hadoop namenode -format

-  Run hadoop dfs daemons and create home directory in HDFS

  .. code-block:: console

     start-dfs.sh
     hadoop fs -mkdir -p /user/ahussein


If you get a
``localhost: ssh: connect to host localhost port 22: Connection refused``
on Macs, then go to [System Preferences] -> [Sharing] and check [Remote
Login].

-  Start Yarn

  .. code-block:: console

     start-yarn.sh

-  Start the History Server

  .. code-block:: console

     mr-jobhistory-daemon.sh start historyserver

-  Populate the HDFS with a file

  .. code-block:: console

     hadoop fs -put /etc/services .

**Step6:**
Running Jobs

-  Start up the Wordcount job

  .. code-block:: console

     hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0-SNAPSHOT.jar \
                wordcount services wcout


-  Start up the wordcount job with a input file format map slit size of 100000


  .. code-block:: console

     hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0-SNAPSHOT.jar \
                wordcount \
                -Dmapreduce.input.fileinputformat.split.maxsize=100000 \
                services wcout2


**Step7:**
Visit the dashboard

Open this URL in browser: `http://localhost:8088 <http://localhost:8088>`_

**Step8:**
Stopping HDFS


.. code-block:: console

  mr-jobhistory-daemon.sh stop historyserver
  stop-yarn.sh
  stop-dfs.sh


.. _on_boarding_deploying-yhadoop-cluster:

Deploying YHadoop on Internal Cluster
-------------------------------------

**Step1:**

Pick a cluster from yo/flubber: Let's say ``openqe99blue``.

**Step2:**

-  Make sure that you already have access to Oath grid
-  Ask Raj to add your userID to get access to `jenkins build scripts <https://re101.ygrid.corp.gq1.yahoo.com:4443/jenkins/view/Openstack/job/Hadoop-Cluster-Deploy-Grid-VM/>`_.
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

      .. code-block:: console

         scp $WORKDIR/hadoop-dist/target/hadoop-2.8.6-SNAPSHOT/share/hadoop/hdfs/hadoop-hdfs-client-2.8.6-SNAPSHOT.jar \
                    hussein@openqe99blue-n2.blue.ygrid.yahoo.com:/home/
         ssh -A openqe99blue-n2.blue.ygrid.yahoo.com
         @openqe99blue-n2$ sudo mv hadoop-hdfs-client-2.8.6-SNAPSHOT.jar $HADOOP_PREFIX/share/hadoop/hdfs/


**Step5:**

Restart the services namenode, datanode, resourcemanager, and
nodemanager


.. code-block:: console

   yinst stop namenode -root /home/gs/gridre/yroot.openqe99blue
   yinst start namenode -root /home/gs/gridre/yroot.openqe99blue


Ignore the memory error you get while starting the service

   ``Java HotSpot(TM) 64-Bit Server VM warning: Failed to reserve shared
   memory. (error = 12)``

**Step6:**

Initialize user for Kerberos database


.. code-block:: console

   kinit ahussein@Y.CORP.YAHOO.COM


.. _`yo/hadoop-deploy`: https://re100.ygrid.corp.gq1.yahoo.com:4443/jenkins/job/Hadoop-Cluster-Deploy-Grid-VM/
.. _`https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster`: https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster
.. _`https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes`: https://openqe99blue-n1.blue.ygrid.yahoo.com:50505/cluster/nodes

If you forget to run ``kinit``, you may see an error like that:

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

.. code-block:: console

   hadoop jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.5.9.1903110101.jar \
              wordcount services wcout

-  Run SleepJob

.. code-block:: console

    jar $HADOOP_PREFIX/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.8.5.9.1903110101-tests.jar \
           sleep -m 1 -r 1 -rt 1200000 -mt 20

Parameters used for the sleepJob:

::

   "-m": number of mappers
   "-r": number of reducers
   "-mt": map sleep time
   "-rt": reduce sleepTime
   "-recordt": Record sleepTime

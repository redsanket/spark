.. _swp:

Using PySpark with Python
=========================
This section details the information required to run PySpark with Python 2.7, Python 3.6, Anaconda, Ipython, Hive and Pypy.

Please note that if you are using python with Spark, the python process uses off heap memory.  The way to configure
off heap memory on Spark is with the overhead configurations ``spark.driver.memoryOverhead`` and ``spark.executor.memoryOverhead``.  Please see the configuration docs on specifics about those.

.. _swp_grid_python:

PySpark + Grid Python 2.7/3.6
-----------------------------
Python 2.7 and 3.6 are being installed on the grid gateways and in HDFS. They contain a few extra packages including: "setuptools", "requests", "numpy", "scipy", "pandas", "scikit-learn", "matplotlib"

.. _swp_grid_python_spark2.2+:

Spark 2.2 and 2.3
~~~~~~~~~~~~~~~~~
With Spark 2.2 and 2.3 you automatically get Python 3.6 and no action is required.

If you want to use Python 2.7 with Spark 2.2 or 2.3 just set the following configs: ``spark.yarn.pythonZip=hdfs:///sharelib/v1/python27/python27.tgz``

For example:

::

  $SPARK_HOME/bin/pyspark  --master yarn --deploy-mode client  --conf spark.yarn.pythonZip=hdfs:///sharelib/v1/python27/python27.tgz

.. _swp_grid_python_spark2.1:

Spark 2.1
~~~~~~~~~

You can use either python 2.7 or python 3.6.

PySpark Client mode 3.6:

.. code-block:: console

  $SPARK_HOME/bin/pyspark --master yarn --deploy-mode client --archives hdfs:///sharelib/v1/python36/python36.tgz#python36 --conf spark.pyspark.python=./python36/bin/python3.6 --conf spark.executorEnv.LD_LIBRARY_PATH=./python36/lib --driver-library-path /home/y/var/python36/lib --conf spark.pyspark.driver.python=/home/y/var/python36/bin/python3.6

PySpark Client mode 2.7:

.. code-block:: console

  $SPARK_HOME/bin/pyspark --master yarn --deploy-mode client --archives hdfs:///sharelib/v1/python27/python27.tgz#python27 --conf spark.pyspark.python=./python27/bin/python2.7 --conf spark.executorEnv.LD_LIBRARY_PATH=./python27/lib --driver-library-path /home/y/var/python27/lib --conf spark.pyspark.driver.python=/home/y/var/python27/bin/python2.7 

Pyspark Client mode batch 3.6:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --archives hdfs:///sharelib/v1/python36/python36.tgz#python36 --conf spark.pyspark.python=./python36/bin/python3.6 --conf spark.executorEnv.LD_LIBRARY_PATH=./python36/lib --driver-library-path /home/y/var/python36/lib --conf spark.pyspark.driver.python=/home/y/var/python36/bin/python3.6  ~/piexecutors.py

Pyspark Client mode batch 2.7:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --archives hdfs:///sharelib/v1/python27/python27.tgz#python27 --conf spark.pyspark.python=./python27/bin/python2.7 --conf spark.executorEnv.LD_LIBRARY_PATH=./python27/lib --driver-library-path /home/y/var/python27/lib --conf spark.pyspark.driver.python=/home/y/var/python27/bin/python2.7  ~/piexecutors.py

PySpark cluster mode 3.6:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --archives hdfs:///sharelib/v1/python36/python36.tgz#python36 --conf spark.pyspark.python=./python36/bin/python3.6 --conf spark.pyspark.driver.python=./python36/bin/python3.6 --conf spark.executorEnv.LD_LIBRARY_PATH=./python36/lib --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./python36/lib ~/piexecutors.py

PySpark cluster mode 2.7:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --archives hdfs:///sharelib/v1/python27/python27.tgz#python27 --conf spark.pyspark.python=./python27/bin/python2.7 --conf spark.pyspark.driver.python=./python27/bin/python2.7 --conf spark.executorEnv.LD_LIBRARY_PATH=./python27/lib --conf spark.yarn.appMasterEnv.LD_LIBRARY_PATH=./python27/lib ~/piexecutors.py


.. _swp_grid_python_jupyter:

Overriding python with Jupyter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Spark with Jupyter currently automatically ships with Python 3.6, if you want to change it to 2.7 or another version self installed you can:

.. code-block:: console

  %%configure -f
  {
     “archives”: [“hdfs:///sharelib/v1/python27/python27.tgz#python27”],
     “conf”:
     {
         “spark.pyspark.python” : “./python27/bin/python2.7",
         “spark.executorEnv.LD_LIBRARY_PATH” : “./python27/lib”,
         “spark.yarn.appMasterEnv.LD_LIBRARY_PATH” : “./python27/lib”,
         “spark.yarn.appMasterEnv.PYSPARK_PYTHON” : “./python27/bin/python2.7",
         “spark.pyspark.driver.python” : “./python27/bin/python2.7"
     }
  }

.. _swp_addon_packages:

Adding additional python packages
---------------------------------
Using the python packages above you can create a tgz with addition python packages you want to use. Use a VM to create the tgz. Instructions below are with python36, if you are using python 27 just change the paths from /home/y/var/python36 to /home/y/var/python27 and use python27_grid package.

- in Linux rhel6 vm
- ``yinst i python36_grid -br current``
- See what is installed already: ``/home/y/var/python36/bin/pip3.6 list``
- Install any additional packages or packages that you want to fix in a custom directory under your home directory.
- ``/home/y/var/python36/bin/pip3.6 install --target=~/addons/site-packages/ [your_package]``
- You can also use the ``--ignore-installed`` option with pip to avoid overriding any existing installed packages
- install any other packages
- ``cd ~/addons/site-packages``
- ``zip -r python36addon.zip [your package list]``. Note: You can also use a tarball instead. Do not include any packages already in the python zip provided like "setuptools", "requests", "numpy", "scipy", "pandas", "scikit-learn", "matplotlib"
- copy to the grid gateway
- copy to hdfs for cluster mode ``hadoop fs -put python36addon.zip``
- send the zip file along with your job ``--py-files hdfs://user/youruserid/python36addon.zip``, if client mode it has to be on local disk ``--py-files python36addon.zip``
.. note:: Python doesn't allow importing dynamic modules - .pyd and .so from zip files. So if your python module has dependencies on .pyd or .so files, you need to use a workaround to accomplish this. You need to create a tarball (.tgz) file and pass it with the --archives and --py-files options simultaneously so the contents are extracted and added to the PYTHONPATH. Example: ``--py-files python36addon.tgz --archives python36addon.tgz``

.. _swp_manuall_install:

Manual Python Installation
--------------------------

This is required by some of the ML python libraries.

.. _swp_manual_python2.7:

Python 2.7
~~~~~~~~~~

You can grab a working Python 2.7 zip file that has python2.7, numpy, pandas, sklearn, scipy, and matplotlib from here: http://dist.corp.yahoo.com/by-package/yspark_yarn_python/. Make sure to put the Python.zip file into hdfs so it gets reused on the nodes, otherwise it will cause issues with running out of inodes.

If you need Python with more modules than just numpy, pandas, sklearn, scipy, and matplotlib you should create your own Python.zip file following the instructions at: :ref:`swp_addon_packages`


- Get Python2.zip

.. code-block:: console

  mkdir tmpfetch; cd tmpfetch
  yinst fetch yspark_yarn_python-2.7.10.1 -br current (choose whichever is the desired version)
  tar -zxvf yspark_yarn_python-*.tgz share/spark_python/__spark_python.zip
  hadoop fs -put share/spark_python/__spark_python.zip Python2.zip #(puts into hdfs:///user/YOURUSER/Python2.zip)
  cd ../; rm -r tmpfetch

Running:

.. note:: Spark > 2.1 has added new configuration parameters "spark.pyspark.driver.python" and "spark.pyspark.python" to be used instead of the environment variables "PYSPARK_DRIVER_PYTHON" and "PYSPARK_PYTHON" respectively.

**Spark > 2.1**

- Cluster Mode

  - Add the spark.pyspark.python and spark.driver.pyspark.python config parameters

    - --conf spark.pyspark.driver.python=./Python2.7.10/bin/python
    - --conf spark.pyspark.python=./Python2.7.10/bin/python

  - Add the --archives option to specify the Python2.zip be distributed with your application and put into a directory path named Python2.7.10

    - --archives hdfs:///user/YOURUSERID/Python2.zip#Python2.7.10

For Example:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --num-executors 10 \
    --driver-memory 2G \
    --conf spark.pyspark.driver.python=./Python2.7.10/bin/python \
    --conf spark.pyspark.python=./Python2.7.10/bin/python \
    --archives hdfs:///user/YOURUSERID/Python2.zip#Python2.7.10 \
  sample_spark.py

Client Mode:

- You need Python locally as well so you have to unzip Python.zip and point to it (assuming you are in /homes/YOURUSER)

  - mkdir Python2.7.10; cd Python2.7.10

    - hadoop fs -get Python2.zip
    - unzip Python2.zip

  - cd /homes/YOURUSERID (or wherever ./Python2.7.10 would be)

- Add the spark.pyspark.python and spark.driver.pyspark.python config parameters

  - --conf spark.pyspark.driver.python=/homes/YOURUSERID/Python2.7.10/bin/python
  - --conf spark.pyspark.python=./Python2.7.10/bin/python

- Add the --archives option to specify the Python2.zip be distributed with your application and put into a directory path named Python2.7.10

  - --archives hdfs:///user/YOURUSERID/Python2.zip#Python2.7.10

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --queue default \
    --num-executors 10 \
    --driver-memory 2G \
    --conf spark.pyspark.driver.python=/homes/YOURUSERID/Python2.7.10/bin/python \
    --conf spark.pyspark.python=./Python2.7.10/bin/python \
    --archives hdfs:///user/YOURUSERID/Python2.zip#Python2.7.10 \
  sample_spark.py

.. _swp_anaconda:

PySpark + Anaconda 
------------------

These are instructions for you to package and and use anaconda with pyspark. This in general is not recommend as anaconda is huge, you are better off to use python and just the packages you require.


.. _swp_anaconda_install:

Install Anaconda-2.2.0
~~~~~~~~~~~~~~~~~~~~~~

Download Anaconda-2.2.0-Linux-x86_64.sh from https://repo.continuum.io/archive/index.html

.. code-block:: console

  bash Anaconda-2.2.0-Linux-x86_64.sh (point the installation to ~/anaconda)
  export PATH=~/anaconda/bin:$PATH

`Additional Update and Installation Details <http://twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fdocs.continuum.io%2Fanaconda%2Finstall.html%23updating-from-older-anaconda-versions&SIG=11ihk2gqf>`_

.. _swp_anaconda_install_zip:

Zip anaconda installation
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

  cd ~/anaconda
  zip -r anaconda.zip .
  mv anaconda.zip ~/ (moving the zip back to home directory)
  Copy ~/anaconda.zip to HDFS

.. _swp_anaconda_spark_settings:

Use spark.pyspark.driver.python and spark.pyspark.python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For Spark versions > 2.1 you would want to pass the following configs as a part of spark-submit

.. code-block:: console

   --conf spark.pyspark.driver.python=./anaconda/bin/python
   --conf spark.pyspark.python=./anaconda/bin/python

For Spark versions <= 2.1 you would want to set PYSPARK_PYTHON?(deprecated), although the latest and current versions on the grid are > 2.1.

.. code-block:: console

    export PYSPARK_PYTHON=./anaconda/bin/python

You also need to set the PYSPARK_PYTHON env variable on the executor nodes. Pass:

.. code-block:: console

    --conf spark.executorEnv.PYSPARK_PYTHON=./anaconda/bin/python

to spark-submit

If you are running in cluster mode for Spark <= 2.1 you also have to export PYSPARK_PYTHON? on the application master so also add:

.. code-block:: console

    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./anaconda/bin/python


.. _swp_anaconda_spark_usage:

Running with Anaconda
~~~~~~~~~~~~~~~~~~~~~

- Add the location of your zipped Anaconda on HDFS to your PySpark command using the "--archives" option. For example, to compute the value `pi <https://github.com/apache/spark/blob/master/examples/src/main/python/pi.py>`_ , run the following script:
- Add any configs you want via normal spark configuration: :ref:`soy_configs`
- Run it:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --queue default \
    --num-executors 5 \
    --driver-memory 2G \
    --conf spark.pyspark.driver.python=./anaconda/bin/python  \
    --conf spark.pyspark.python=./anaconda/bin/python \
    --archives 'hdfs:///user/USER/anaconda.zip#anaconda' \
    pi.py \
    10




.. _swp_pypy:

Running With Pypy
-----------------

.. note:: The minimum version of yspark required to run pypy is 2.2.0.32.

Follow the instructions stated below if you want to run a spark job using pypy version 2.6.1.x:
- Log into any grid and run the following commands:

.. code-block:: console

  yinst fetch pypy-2.6.1.16
  mkdir pypy
  mv pypy-2.6.1.16-rhel-6.x.tgz pypy/
  cd pypy/
  tar -xvf pypy-2.6.1.16-rhel-6.x.tgz
  cd share/pypy/
  tar -xvf python_build_pypy.tgz
  zip -r pypy-2.6.1.zip *
  hadoop fs -put pypy-2.6.1.zip

- If all goes well then we are now ready to run the spark job. The command to run a spark job using pypy is given below for cluster mode, client mode and pyspark respectively:

Cluster Mode:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode cluster --queue default --num-executors 5 --driver-memory 2G --conf spark.pyspark.driver.python=./Pypy/bin/pypy --conf spark.pyspark.python=./Pypy/bin/pypy --archives hdfs:///user/YOUR_USERNAME/pypy-2.6.1.zip#Pypy ~/YOURPYTHONFILE.py

Client Mode:

.. code-block:: console

  $SPARK_HOME/bin/spark-submit --master yarn --deploy-mode client --queue default --num-executors 5 --driver-memory 2G --conf spark.pyspark.driver.python=/homes/YOUR_USERNAME/pypy/share/pypy/bin/pypy --conf spark.pyspark.python=./Pypy/bin/pypy --archives hdfs:///user/YOUR_USERNAME/pypy-2.6.1.zip#Pypy ~/YOURPYTHONFILE.py

Pyspark:

.. code-block:: console

  $SPARK_HOME/bin/pyspark --master yarn --conf spark.pyspark.driver.python=/homes/YOUR_USERNAME/pypy/share/pypy/bin/pypy --conf spark.pyspark.python=./Pypy/bin/pypy --archives hdfs:///user/YOUR_USERNAME/pypy-2.6.1.zip#Pypy

.. _swp_packages:

Spark Python Packages
---------------------
With Hue 3.10+ you can use pyspark and it automatically loads Python 2.7.10 with numpy and pandas for you. If you need to ship other packages you can follow these instructions to create an archive that you can upload with your spark job. If you are just using pyspark you should go back and see the instructions on using Ipython/anaconda.
Instructions are from a Gateway or VM, note most gateways might not have access anymore and you need to run from a vm:

.. code-block:: console

  export IPYTHON_ROOT=~/Python2.7.10 #Change this directory to install elsewhere.
  export http_proxy=`hostname | sed -r 's/([^\.])*.(.*)/httpproxy-res.\2:4080/'`
  export HTTP_PROXY=”${http_proxy}”
  curl -O https://www.python.org/ftp/python/2.7.10/Python-2.7.10.tgz
  tar -xvf Python-2.7.10.tgz
  rm Python-2.7.10.tgz
  pushd Python-2.7.10 >/dev/null
  ./configure --prefix="${IPYTHON_ROOT}"
  make
  make install
  popd >/dev/null
  rm -rf Python-2.7.10
  pushd "${IPYTHON_ROOT}" >/dev/null
  curl -O https://bootstrap.pypa.io/get-pip.py
  bin/python get-pip.py
  rm get-pip.py
  # install any other packages you need at this point
  For example we install numpy and pandas
  bin/pip install numpy
  bin/pip install pandas
  # now zip it up
  pushd Python2.7.10/lib/python2.7/site-packages >/dev/null
  tar -zcvf ~/python27sitepackages.tgz *
  popd > /dev/null

Then to use the packages with Hue send them along as an archive. Upload the tgz into hdfs: hadoop fs -put python27sitepackages.tgz

For using it on Hue

- Open a pyspark notebook
- In the upper right corner, open the "Context" menu
- Select "Archives" under the "Add a property.." menu
- Press the "+" button on right
- Type in where you put it in hdfs, ``hdfs:///user/myuser/python27sitepackages.tgz``
- Hit the "Recreate" button

For using it on Jupyter

- use the %%configure option with jupyter to send it as an archive, see: https://jetblue-jupyter.blue.ygrid.yahoo.com:9999/nb/notebooks/projects/jupyter/demo/samples/Jupyter_Reference__Magics.ipynb

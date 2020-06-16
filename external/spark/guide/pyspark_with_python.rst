.. _swp:

PySpark
==========

Using PySpark requires spark to be compiled with python support and python to be available on the nodes.

.. _swp_grid_python:

VMG/VCG Grid Install
---------------------

For VMG/VCG we automatically handle shipping Python for you for both current and latest (yspark_yarn-2.4.x).
Current Python version supported is python3.6 for rhel7 docker container and the way to set the docker container is by specifying `--conf spark.oath.dockerImage=hadoop/rhel7:current`
during launch on your respective gateways or launcher boxes.

Getting started on VMG/VCG and available services and gateways please visit: https://git.ouroath.com/pages/developer/Bdml-guide/

.. _swp_examples:

Spark has Python API support for all its modules.
See the Spark documentation for examples. https://spark.apache.org/docs/latest/.

PySpark cluster mode example
----------------------------

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue default \
    --num-executors 10 \
    --driver-memory 2G \
    --conf spark.oath.dockerImage=hadoop/rhel7:current \
  sample_spark.py

PySpark client mode example
----------------------------

.. code-block:: console

  $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --queue default \
    --num-executors 10 \
    --driver-memory 2G \
    --conf spark.oath.dockerImage=hadoop/rhel7:current \
  sample_spark.py

PySpark Interative shell mode example
-------------------------------------

.. code-block:: console

  $SPARK_HOME/bin/pyspark \
    --master yarn \
    --queue default \
    --num-executors 10 \
    --driver-memory 2G \
    --conf spark.oath.dockerImage=hadoop/rhel7:current

.. _swp_jupyter:

PySpark with Jupyter on Grid
----------------------------
Jupyter has a Pyspark kernel available which enables users to have a shell based interaction framework similar to pyspark shell.
We highly recommend browsing through the demos_ for sample pyspark kernels for general usage and for ML applications on grid.

.. _demos: https://git.ouroath.com/pages/developer/Bdml-guide/migrated-pages/Jupyter_User_Guide/

.. _swp_hue:

For using it on Hue
-------------------
- Open a pyspark notebook (Query -> Editor -> Pyspark)
- In the upper right corner, open the Notebook menu (3 vertical dots) and select "Session"
- Select "SparkConf", under the "Add a property.." menu
- Press the "+" button on right
- Type spark.oath.dockerImage as Key and hadoop/rhel7:current as value
- Hit the "Recreate" button

.. _swp_custom_pkg:

Custom Python libraries
-----------------------
RHEL6
-----
- In Linux rhel6 vm: yinst i python36_grid -br current
- See what is installed already: /home/y/var/python36/bin/pip3.6 list
- Install any additional packages or packages that you want to fix in a custom directory under your home directory. /home/y/var/python36/bin/pip3.6 install --ignore-installed --target=$HOME/addons/site-packages/ [your_package] Note: The --ignore-installed option in pip avoids overriding any existing installed packages
- Create an archive: cd $HOME/addons/site-packages zip -r python36addon.zip [your package list]. Note 1: You can also use a tarball instead. Do this if your packages include .so's (see Shared Libries section below). Note 2: Do not include any packages already in the python zip provided like "setuptools", "requests", "numpy", "scipy", "pandas", "scikit-learn", "matplotlib".
- Copy to the grid gateway
- Copy to hdfs for cluster mode hadoop fs -put python36addon.zip
- Send the zip file along with your job --py-files hdfs:///user/youruserid/python36addon.zip, if client mode it has to be on local disk --py-files python36addon.zip

RHEL7
-----
The recommendation is to use default ML docker image ml/rhel8_mlbundle:2020.05.1 for advanced ML applications or the default hadoop docker image hadoop/rhel7:current
for basic pyspark usage as they include most recent python libraries required for applications. The below steps should be used only when you need additional python packages not available
in the docker images.

(from the Dockerfile steps: https://git.ouroath.com/hadoop/docker_configs/blob/2e5b51dfd7983399027f4c6443a68bf531febce6/rhel7/Dockerfile#L15)

- In Linux rhel7 vm: ``yum-config-manager --add-repo https://edge.artifactory.yahoo.com:4443/artifactory/python_rpms/python_rpms.repo``
- Then install the python36 distribution: ``sudo yum install -y yahoo_python36``
- And install default packages present in the grid: ``sudo /opt/python/bin/pip3.6 install numpy scipy pandas requests setuptools scikit-learn matplotlib``
- Python 3.6 is now installed under ``/opt/python``
- See what is installed already: ``/opt/python/bin/pip3.6 list``
- Install any additional packages or packages that you want to fix in a custom directory under your home directory.
  ``/opt/python/bin/pip3.6 install --ignore-installed --target=$HOME/addons/site-packages/ [your_package]``
  Note: The ``--ignore-installed`` option in pip avoids overriding any existing installed packages
- Create an archive:
  ``cd $HOME/addons/site-packages``
  ``zip -r python36addon.zip [your package list]``.
  Note 1: You can also use a tarball instead. Do this if your packages include .so's (see Shared Libries section below).
  Note 2: Do not include any packages already in the python zip provided like "setuptools", "requests", "numpy", "scipy", "pandas", "scikit-learn", "matplotlib".
- Copy to the grid gateway
- Copy to hdfs for cluster mode ``hadoop fs -put python36addon.zip``
- Send the zip file along with your job ``--py-files hdfs:///user/youruserid/python36addon.zip``, if client mode it has to be on local disk ``--py-files python36addon.zip``

Does Your Python Module Have Shared Libraries?
----------------------------------------------
- In the ``$HOME/addons/site-packages`` directory created in the prior section, you can inspect the files in each of your modules to find .so's. For example, this is the PIL module, which happens to have many such dynamic libraries:

.. code-block:: console

  find . |grep ".so"
  ./PIL/.libs/libfreetype-3e240bcb.so.6.16.1
  ./PIL/.libs/libjpeg-3fe7dfc0.so.9.3.0
  ./PIL/.libs/liblzma-6cd627ed.so.5.2.4
  ./PIL/.libs/libwebp-baad113c.so.7.0.4
  ./PIL/.libs/liblcms2-a6801db4.so.2.0.8
  ./PIL/.libs/libpng16-9e58a7b0.so.16.36.0
  ./PIL/.libs/libwebpmux-75695800.so.3.0.4
  ./PIL/.libs/libwebpdemux-60cc0b6d.so.2.0.6
  ./PIL/.libs/libtiff-8267adfe.so.5.4.0
  ./PIL/.libs/libz-a147dcb0.so.1.2.3
  ./PIL/.libs/libopenjp2-e366d6b0.so.2.1.0
  ./PIL/_imagingmorph.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_imagingmath.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_webp.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_imagingtk.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_imagingcms.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_imagingft.cpython-36m-x86_64-linux-gnu.so
  ./PIL/_imaging.cpython-36m-x86_64-linux-gnu.so


Any .so could cause issues when loading your Python module from Spark. For example:

 1. Python doesn't allow importing dynamic modules (.so) from zip files. So if your python module depends on .so files, you need to use a workaround to import. You need to create a tarball (Example: ``tar -czvf python36addon.tgz PIL``) file, and have Spark extract it in the target containers by passing the ``--archives``, ``--py-files``, and ``--conf spark.yarn.includeArchivesPythonPath=true`` options simultaneously so the contents are extracted and added to the PYTHONPATH. Example: ``--py-files python36addon.tgz --archives python36addon.tgz --conf spark.yarn.includeArchivesPythonPath=true``.

    .. note:: In client mode, the driver will not extract the tarball (as opposed to cluster mode where the driver and the executors extract), so if you are looking to run an interactive session with a custom module, you will have to add the module to the PYTHONPATH in other ways, or run pyspark from the directory where your modules are (in the example above, inside the ``$HOME/addons/site-packages`` directory)
 2. Dynamic libraries depend on other native dynamic libraries to run. If the versions of these dependencies don't match with what is installed in the Yarn containers, you may get a runtime error in your job. When you see errors like this, the library may need to be compiled for the specific Linux and Python version that is being executed in the container.

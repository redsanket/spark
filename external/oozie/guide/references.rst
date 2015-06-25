References
==========

.. 04/24/15: Rewrote.
.. 05/12/15: Edited.

Yahoo Configurations for Oozie
------------------------------

.. list-table:: Oozie Configurations
   :widths: 15, 10, 30
   :header-rows: 1 

   * - Actions
     - Shared Library
     - Shared Library Tags 
   * - Hive
     - ``oozie.action.sharelib.for.hive`` 
     - ``hcat_current``,``hive_current``
   * - Pig 
     -  ``oozie.action.sharelib.for.pig``
     - ``hcat_current``,``pig_current``
   * - HBase/Java
     - ``oozie.action.sharelib.for.java``
     - ``hbase_current``
   * - HBase/MapReduce
     - ``oozie.action.sharelib.for.map-reduce``
     - ``hbase_current``
   * - HBase/Pig
     - ``oozie.action.sharelib.for.pig
     - ``hbase_current``,``pig_current``
   
.. _references-oozie_servers:

Oozie Servers on Clusters
-------------------------

.. list-table:: Oozie Servers on Clusters
   :widths: 15, 30
   :header-rows: 1 

   
   * - Grid Cluster 
     - Oozie Server URL
   * - Axonite Red
     - ``http://axonitered-oozie.red.ygrid.yahoo.com:4080/oozie/``, ``https://axonitered-oozie.red.ygrid.yahoo.com:4443/oozie/``
   * - Bassnium Red
     - ``http://bassniumred-oozie.red.ygrid.yahoo.com:4080/oozie/``, ``https://bassniumred-oozie.red.ygrid.yahoo.com:4443/oozie/``
   * - Bassnium Tan
     - ``http://bassniumtan-oozie.tan.ygrid.yahoo.com:4080/oozie/``,  ``https://bassniumtan-oozie.tan.ygrid.yahoo.com:4443/oozie/``
   * - Cobalt Blue
     - ``http://cobaltblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``,  ``https://cobaltblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``
   * - Dilithium Blue
     - ``http://dilithiumblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``, ``https://dilithiumblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``  
   * - Dilithium Red
     - ``http://dilithiumred-oozie.red.ygrid.yahoo.com:4080/oozie/``, ``https://dilithiumred-oozie.red.ygrid.yahoo.com:4443/oozie/``
   * - Kryptonite Red  
     - ``http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/``, ``https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/`` 
   * - Mithril Blue
     - ``http://mithrilblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``,  ``http://mithrilblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``
   * - Mithril Red
     - ``http://mithrilred-oozie.red.ygrid.yahoo.com:4080/oozie/``,  ``https://mithrilred-oozie.red.ygrid.yahoo.com:4443/oozie/``
   * - Nitro Blue
     - ``http://nitroblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``, ``https://nitroblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``
   * - Oxium Blue
     - ``http://oxiumblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``, ``https://oxiumblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``
   * - Phazon Tan
     - ``http://phazontan-oozie.tan.ygrid.yahoo.com:4080/oozie/``, ``https://phazontan-oozie.tan.ygrid.yahoo.com:4443/oozie/``
   * - Tiberium Tan
     - ``http://tiberiumtan-oozie.tan.ygrid.yahoo.com:4080/oozie/``, ``https://tiberiumtan-oozie.tan.ygrid.yahoo.com:4443/oozie/``
   * - Uranium Blue 
     - ``http://uraniumblue-oozie.blue.ygrid.yahoo.com:4080/oozie/``, ``https://uraniumblue-oozie.blue.ygrid.yahoo.com:4443/oozie/``
   * - Uranium Tan 
     - ``http://uraniumtan-oozie.tan.ygrid.yahoo.com:4080/oozie/``, ``https://uraniumtan-oozie.tan.ygrid.yahoo.com:4443/oozie/``
   * - Zanium Tan
     - ``http://zaniumtan-oozie.tan.ygrid.yahoo.com:4080/oozie/``,  ``https://zaniumtan-oozie.tan.ygrid.yahoo.com:4443/oozie/``


Expression Language (EL) Functions
----------------------------------

The list below contains links to the `Yahoo Oozie documentation <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/index.html>`_ 
that's based on the Apache Oozie documentation. See also the `EL Expression Language Quick Reference <http://docs.oracle.com/javaee/6/tutorial/doc/gjddd.html>`_.

- Expression Language (EL) Constants

  - `Basic EL Constants <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.1_Basic_EL_Constants>`_
  - `Hadoop EL Constants <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.4_Hadoop_EL_Constants>`_

- `Expression Language (EL) Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2_Expression_Language_Functions>`_

  - `Basic EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.2_Basic_EL_Functions>`_
  - `Hadoop EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.5_Hadoop_EL_Functions>`_
  - `Hadoop Jobs EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.6_Hadoop_Jobs_EL_Function>`_
  - `HDFS EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.7_HDFS_EL_Functions>`_
  - `Workflow EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.3_Workflow_EL_Functions>`_
  - `HCatalog EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/WorkflowFunctionalSpec.html#a4.2.8_HCatalog_EL_Functions>`_

- `Coordinator EL Functions <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6._Parameterization_of_Dataset_Instances_in_Input_and_Output_Events>`_

  - `coord:current(int n) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.1._coord:currentint_n_EL_Function_for_Synchronous_Datasets>`_
  - `coord:current(int n) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.1._coord:currentint_n_EL_Function_for_Synchronous_Datasets>`_
  - `coord:offset(int n, String timeUnit) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.2._coord:offsetint_n_String_timeUnit_EL_Function_for_Synchronous_Datasets>`_
  - `coord:hoursInDay(int n) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.3._coord:hoursInDayint_n_EL_Function_for_Synchronous_Datasets>`_
  - `coord:daysInMonth(int n) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.4._coord:daysInMonthint_n_EL_Function_for_Synchronous_Datasets>`_
  - `coord:tzOffset() EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.5._coord:tzOffset_EL_Function_for_Synchronous_Datasets>`_
  - `coord:latest(int n) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.6._coord:latestint_n_EL_Function_for_Synchronous_Datasets>`_
  - `coord:future(int n, int limit) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.7._coord:futureint_n_int_limit_EL_Function_for_Synchronous_Datasets>`_
  - `coord:absolute(String timeStamp) EL Function for Synchronous Datasets <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.6.8._coord:absoluteString_timeStamp_EL_Function_for_Synchronous_Datasets>`_
  - `coord:dataIn(String name) EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.7.1._coord:dataInString_name_EL_Function>`_
  - `coord:dataOut(String name) EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.7.2._coord:dataOutString_name_EL_Function>`_
  - `coord:nominalTime() EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.7.3._coord:nominalTime_EL_Function>`_
  - `coord:actualTime() EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.7.4._coord:actualTime_EL_Function>`_
  - `coord:user() EL Function (since Oozie 2.3) <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.7.5._coord:user_EL_Function_since_Oozie_2.3>`_
  - `coord:databaseIn(String name), coord:databaseOut(String name) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.1_coord:databaseInString_name_coord:databaseOutString_name_EL_function>`_
  - `coord:tableIn(String name), coord:tableOut(String name) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.2_coord:tableInString_name_coord:tableOutString_name_EL_function>`_
  - `coord:dataInPartitionFilter(String name, String type) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.3_coord:dataInPartitionFilterString_name_String_type_EL_function>`_
  - `coord:dataOutPartitions(String name) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.5_coord:dataInPartitionMinString_name_String_partition_EL_function>`_
  - `coord:dataInPartitionMin(String name, String partition) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.6_coord:dataInPartitionMaxString_name_String_partition_EL_function>`_
  - `coord:dataInPartitionMax(String name, String partition) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.7_coord:dataOutPartitionValueString_name_String_partition_EL_function>`_
  - `coord:dataOutPartitionValue(String name, String partition) EL function http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.7_coord:dataOutPartitionValueString_name_String_partition_EL_function<>`_
  - `coord:dataInPartitions(String name, String type) EL function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.8.8_coord:dataInPartitionsString_name_String_type_EL_function>`_
  - `coord:dateOffset(String baseDate, int instance, String timeUnit) EL Function <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.9.1._coord:dateOffsetString_baseDate_int_instance_String_timeUnit_EL_Function>`_
  - `coord:formatTime(String ts, String format) EL Function (since Oozie 2.3.2) <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/CoordinatorFunctionalSpec.html#a6.9.2._coord:formatTimeString_ts_String_format_EL_Function_since_Oozie_2.3.2>`_
 

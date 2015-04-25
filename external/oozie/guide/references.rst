References
==========
.. 04/24/15: Rewrote.

Yahoo Configurations for Oozie
------------------------------

.. csv-table:: Oozie Configurations
   :header: "Actions", "Shared Library", "Credential"
   :widths: 15, 10, 30

   "Hive", "``oozie.action.for.hive``, "``hcat_current, hive_current``"
   "Pig", "``oozie.action.for.pig``, "``hcat_current, pig_current``"
   "HBase", "``oozie.action.for.hbase``, "``pig_current, hbase_current``"


.. http://twiki.corp.yahoo.com/view/CCDI/OozieShareLib
.. For pig: pig_current . For pig with hcat: pig_current,hcat_current For piig with 
.. hbase: pig_current,hbase_current
.. Mention sharelib and how to setup credentials
.. Document what to specify in sharelib for each action (mapreduce, streaming, distcp, etc)
.. http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs


Expression Language (EL) Functions
----------------------------------

TBD 






====================
HBase Shell Commands
====================

This chapter describes the shell commands and gives usage examples.
For the purpose of organization, we have grouped the commands into
the categories proposed by the `Learn HBase blog <http://learnhbase.wordpress.com/2013/03/02/hbase-shell-commands/>`_.

.. _hbase_shell-general:

General
=======

.. _shell_general-status:

status
------

Shows the cluster status. The default command is ``summary``.

.. _general_status-commands:

Commands
########

.. option:: summary

.. option:: simple

.. option:: detailed

.. _general_status-usage:

Usage Examples
##############

::

   hbase> status
   hbase> status 'simple'
   hbase> status 'summary'
   hbase> status 'detailed'

.. _shell_general-version:

version
-------

Outputs the HBase version.

.. _general_version-commands:

Commands
########

None.

.. _general_version-usage:

Usage Examples
##############

::
  
   hbase> version

.. _shell_general-whoami:

whoami
------

Shows the current user.

.. _general_whoami-commands:

Commands
########

None.

.. _general_whoami-usage:

Usage Examples
##############

:: 

   hbase> whoami

.. _hbase_shell-table_mgmnt:

Tables Management 
=================

.. _shell_table_mgmnt-alter:

alter
-----

Alters the column family schema. You pass the table name 
and a dictionary specifying new column family schema. 
Dictionaries are described on the main help command output. 
The dictionary must include name of column family to alter it. 

.. _table_mgmnt_alter-commands:

Commands
########

None.

.. _table_mgmnt_alter-usage:

Usage Examples
##############

To change or add the ``f1`` column family in table ``t1`` 
from current value to keep a maximum of 5 cell VERSIONS, do: 

:: 

   hbase> alter 't1', NAME => 'f1', VERSIONS => 5

To operate on several column families:

::

   hbase> alter 't1', 'f1', {NAME => 'f2', IN_MEMORY => true}, {NAME => 'f3', VERSIONS => 5}


To delete the ``f1`` column family in table ``t1``, use one
of following:

::

   hbase> alter 't1', NAME => 'f1', METHOD => 'delete'
   hbase> alter 't1', delete => 'f1'

To change table-scope attributes like ``MAX_FILESIZE``,
``READONLY``, ``MEMSTORE_FLUSHSIZE``, ``DEFERRED_LOG_FLUSH``, etc. 
These can be put at the end. For example, to change the max size of a 
region to 128MB, do:

:: 

   hbase> alter 't1', MAX_FILESIZE => '134217728'

To add a table coprocessor by setting a table coprocessor
attribute:

::

   hbase> alter t1, 'coprocessor'=>'hdfs:///foo.jar|com.foo.FooRegionObserver|1001|arg1=1,arg2=2'


Since you can have multiple coprocessors configured for a table, a
sequence number will be automatically appended to the attribute name
to uniquely identify it.

The coprocessor attribute must match the pattern below in order for
the framework to understand how to load the coprocessor classes:

[coprocessor jar file location] | class name | [priority] | [arguments]

You can also set configuration settings specific to this table or column
family:

::

   hbase> alter 't1', CONFIGURATION => {'hbase.hregion.scan.loadColumnFamiliesOnDemand' => 'true'}
   hbase> alter 't1', {NAME => 'f2', CONFIGURATION => {'hbase.hstore.blockingStoreFiles' => '10'}}


To remove a table-scope attribute:

:: 

   hbase> alter 't1', METHOD => 'table_att_unset', NAME => 'MAX_FILESIZE'
   hbase> alter 't1', METHOD => 'table_att_unset', NAME => 'coprocessor$1'


There can also be more than one alteration in one command:

::

   hbase> alter 't1', { NAME => 'f1', VERSIONS => 3 },
   { MAX\_FILESIZE => '134217728' }, { METHOD => 'delete', NAME => 'f2' },
   OWNER => 'johndoe', METADATA => { 'mykey' => 'myvalue' }


.. _shell_table_mgmnt-create:

create
------

Creates a table. You pass a table name, a dictionary of specifications per
column family, and optionally a dictionary of table
configuration.

.. _table_mgmnt_create-commands:

Commands
########

None.

.. _table_mgmnt_create-usage:

Usage Examples
##############

::

   hbase> create 't1', {NAME => 'f1', VERSIONS => 5}
   hbase> create 't1', {NAME => 'f1'}, {NAME => 'f2'}, {NAME => 'f3'}
  
To create the same thing above, you can use the shorthand below.


::

   hbase> create 't1', 'f1', 'f2', 'f3'
   hbase> create 't1', {NAME => 'f1', VERSIONS => 1, TTL => 2592000, BLOCKCACHE => true}
   hbase> create 't1', {NAME => 'f1', CONFIGURATION => {'hbase.hstore.blockingStoreFiles' => '10'}}

.. _shell_table_mgmnt-describe:

describe
--------

Describes the named table.

.. _table_mgmnt_describe-commands:

Commands
########

None.

.. _table_mgmnt_describe-usage:

Usage Examples
##############

::

   hbase> describe 't1'

.. _shell_table_mgmnt-disable:

disable
-------

Disables the named table.

.. _table_mgmnt_disable-commands:

Commands
########

None.

.. _table_mgmnt_disable-usage:

Usage Examples
##############

::

   hbase> disable 't1'


.. _shell_table_mgmnt-disable_all:

disable_all
-----------

Disables all of the tables matching the given regular expression.

.. _mgmnt_disable_all-commands:

Commands
########

None.

.. _mgmnt_disable_all-usage:

Usage Examples
##############

:: 

   hbase> disable_all 't.*'

.. _shell_table_mgmnt-is_disabled:

is_disabled
-----------

Verifies that the named table is disabled.

.. _table_mgmnt_is_disabled-commands: 

Commands
########

None.

.. _table_mgmnt_is_disabled-usage: 

Usage Examples
##############

:: 

   hbase> is_disabled 't1'


.. _shell_table_mgmnt-drop:

drop
----

Drops the named table. The table must first be disabled.

.. _table_mgmnt_drop-commands:

Commands
########

None.

.. _table_mgmnt_drop-usage:

Usage Examples
##############

:: 

   hbase> drop 't1'


.. _shell_table_mgmnt-drop_all:

drop_all
--------

Drops all of the tables matching the given regular expression.

.. _table_mgmnt_drop_all-commands:

Commands
########

None.

.. _table_mgmnt_drop_all-usage:

Usage Examples
##############

::

   hbase> drop_all 't.\*'

.. _shell_table_mgmnt-enable:

enable
------

Enables the named table.

.. _table_mgmnt_enable-commands:

Commands
########

None.

.. _table_mgmnt_enable-usage:

Usage Examples
##############

::

   hbase> enable 't1'


.. _shell_table_mgmnt-enable_all:

enable_all
----------

Enables all of the tables matching the given regular expression.

.. _table_mgmnt_enable_all-commands:

Commands
########

None.

.. _table_mgmnt_enable_all-usage:

Usage Examples
##############

::

   hbase> enable_all 't.\*'

.. _shell_table_mgmnt-is_enable_all:

is_enabled
----------

Verifies the named table is enabled.

.. _table_mgmnt_is_enable_all-commands:

Commands
########

None.

.. _table_mgmnt_is_enable_all-usage:

Usage Examples
##############

:: 

   hbase> is_enabled 't1'

exists
------

Verifies that the named table exists.

Commands
########

None.

Usage Examples
##############

:: 

   hbase> exists 't1'

list
----


Lists all tables in HBase. An optional regular expression parameter
can be passed to filter the output.

Commands
########

None.

Usage Examples
##############

::

   hbase> list
   hbase> list 'abc.\*'

show_filters
------------

Shows all the filters in HBase.

Commands
########

None.

Usage Examples
##############

::

   hbase> show_filters

alter_status
------------

Gets the status of the alter command. Indicates the number of regions of
the table that have received the updated schema Pass table name.

Commands
########

None.

Usage Examples
##############

::

   hbase> alter_status 't1'

alter_async
-----------

Alters the column family schema and does not wait for all regions to receive
the schema changes. You pass the table name and a dictionary specifying new 
column family schema. Dictionaries are described on the main help command
output. The dictionary must include name of column family to alter.

Commands
########

None.

Usage Examples
##############


To change or add the ``f1`` column family in table ``t1``
to keep a maximum of 5 cell VERSIONS, do:

::

   hbase> alter_async 't1', NAME => 'f1', VERSIONS => 5


To delete the ``f1`` column family in table ``t1``, do:

::

   hbase> alter_async 't1', NAME => 'f1', METHOD => 'delete' 

Shorter version:

::

   hbase> alter_async 't1', 'delete' => 'f1'

You can also change table-scope attributes like ``MAX_FILESIZE``
``MEMSTORE_FLUSHSIZE``, ``READONLY``, and ``DEFERRED_LOG\FLUSH``.

For example, to change the max size of a family to 128MB:

::

   hbase> alter 't1', METHOD => 'table_att', MAX_FILESIZE => '134217728'


There could be more than one alteration in one command:

::

   hbase> alter 't1', {NAME => 'f1'}, {NAME => 'f2', METHOD => 'delete'}


To check if all the regions have been updated, use ``alter_status <table_name>``


Data Manipulation Commands
==========================

count
-----

Counts the number of rows in a table. The return value is the number of
rows. This operation may take a LONG time (Run ``$HADOOP_HOME/bin/hadoop jar hbase.jar rowcount``
to run a counting mapreduce job). The current count is shown every 1000 rows by default. 
Count interval may be optionally specified. Scan caching is enabled on count scans by default. 
The default cache size is 10 rows.

Commands
########

None.

Usage Examples
##############

To increase the size of your rows:

::

   hbase> count 't1'
   hbase> count 't1', INTERVAL => 100000
   hbase> count 't1', CACHE => 1000
   hbase> count 't1', INTERVAL => 10, CACHE => 1000

The same commands also can be run on a table reference. Suppose you
had a reference ``t`` to table ``t1``, the corresponding commands would be
the following:

::

   hbase> t.count
   hbase> t.count INTERVAL => 100000
   hbase> t.count CACHE => 1000
   hbase> t.count INTERVAL => 10, CACHE => 1000

delete
------

Puts a delete cell value at specified table/row/column and optionally
timestamps the coordinates. Deletes must match the deleted cell's
coordinates exactly. When scanning, a deleted cell suppresses older
versions. 

To delete a cell from ``t1`` at row ``r1`` under column ``c1``
marked with the time ``ts1``:

::

   hbase> delete 't1', 'r1', 'c1', 'ts1'

The same command can also be run on a table reference. Suppose
you had a reference t to table ``t1``, the corresponding command would be
the following:

::

   hbase> t.delete 'r1', 'c1', 'ts1'



deleteall
---------

Deletes all the cells in a given row. You pass a table name, row, and
optionally a column and timestamp. 


Commands
########

None.

Usage Examples
##############

::

   hbase> deleteall 't1', 'r1''
   hbase> deleteall 't1', 'r1', 'c1'
   hbase> deleteall 't1', 'r1', 'c1', 'ts1'

The same commands also can be run on a table reference. Suppose you
had a reference ``t`` to table ``t1``, the corresponding command would be
the following:

::

   hbase> t.deleteall 'r1'
   hbase> t.deleteall 'r1', 'c1'
   hbase> t.deleteall 'r1', 'c1', 'ts1'

get
---

Gets the contents of a row or a cell contents. You pass table name, row, 
and optionally a dictionary of column(s), timestamp, timerange and versions.

Commands
########

None.

Usage Examples
##############

::

   hbase> get 't1', 'r1'
   hbase> get 't1', 'r1', {TIMERANGE => [ts1, ts2]}
   hbase> get 't1', 'r1', {COLUMN => 'c1'}
   hbase> get 't1', 'r1', {COLUMN => ['c1', 'c2', 'c3']}
   hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1}
   hbase> get 't1', 'r1', {COLUMN => 'c1', TIMERANGE => [ts1, ts2], VERSIONS => 4}
   hbase> get 't1', 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
   hbase> get 't1', 'r1', {FILTER => "ValueFilter(=, 'binary:abc')"}
   hbase> get 't1', 'r1', 'c1'
   hbase> get 't1', 'r1', 'c1', 'c2'
   hbase> get 't1', 'r1', ['c1', 'c2']

Besides the default ``toStringBinary`` format, ``get`` also supports
custom formatting by column. A user can define a ``FORMATTER`` by 
adding it to the column name in the get specification. 

The FORMATTER can be stipulated in the following ways:

#. As a ``org.apache.hadoop.hbase.util.Bytes`` method name (e.g., ``toInt``, ``toString``)
#. As a custom class followed by method name: ``c(MyFormatterClass).format``.
  

For example, to format ``cf:qualifier1`` and
``cf:qualifier2`` both as Integers:

::

   hbase> get 't1', 'r1' {COLUMN => ['cf:qualifier1:toInt',
   'cf:qualifier2:c(org.apache.hadoop.hbase.util.Bytes).toInt'] }

Note, you can also specify a FORMATTER by column only (``cf:qualifer``).
You cannot specify a FORMATTER for all columns of a column family.
The same commands also can be run on a reference to a table (obtained via ``get_table`` or
``create_table``). Suppose you had a reference ``t`` to table ``t1``, the
corresponding commands would be:

::

   hbase> t.get 'r1'
   hbase> t.get 'r1', {TIMERANGE => [ts1, ts2]}
   hbase> t.get 'r1', {COLUMN => 'c1'}
   hbase> t.get 'r1', {COLUMN => ['c1', 'c2', 'c3']}
   hbase> t.get 'r1', {COLUMN => 'c1', TIMESTAMP => 'ts1'}
   hbase> t.get 'r1', {COLUMN => 'c1', TIMERANGE => ['ts1', 'ts2'], VERSIONS => 4}
   hbase> t.get 'r1', {COLUMN => 'c1', TIMESTAMP => ts1, VERSIONS => 4}
   hbase> t.get 'r1', {FILTER => "ValueFilter(=, 'binary:abc')"}
   hbase> t.get 'r1', 'c1'
   hbase> t.get 'r1', 'c1', 'c2'
   hbase> t.get ``r1``, ['c1', 'c2']

get_counter
-----------

Returns a counter cell value at specified table/row/column coordinates.
A cell should be managed with atomic increment function on HBase
and the data should be binary encoded. 

Commands
########

None.

Usage Examples
##############

::

   hbase> get_counter 't1', 'r1', 'c1'

The same commands also can be run on a table reference. Suppose
you had a reference ``t`` to table ``t1``, the corresponding command 
would be:

::

   hbase> t.get\_counter 'r1', 'c1'

incr
----

Increments a cell ``value`` at the specified table/row/column coordinates.

Commands
########

None.

Usage Examples
##############

To increment a cell value in table ``t1`` at row ``r1`` under column
``c1`` by 1 (can be omitted) or 10 do:

::

   hbase> incr 't1', 'r1', 'c1'
   hbase> incr 't1', 'r1', 'c1', 1
   hbase> incr 't1', 'r1', 'c1', 10

The same commands also can be run on a table reference. Suppose you
had a reference ``t`` to table ``t1``, the corresponding command would be
the following:

::

   hbase> t.incr 'r1', 'c1'
   hbase> t.incr 'r1', 'c1', 1
   hbase> t.incr 'r1', 'c1', 10

put
---

Puts a cell ``value`` at a specified table/row/column and optionally
timestamp coordinates. 

Commands
########

None.

Usage Examples
##############

To put a cell value into table ``t1`` at
row ``r1`` under column ``c1`` marked with the time ``ts1``:

:: 

   hbase> put 't1', 'r1', 'c1', 'value', ts1

The same commands can also be run on a table reference. Suppose you
had a reference ``t`` to table ``t1``, the corresponding command would be:

::

   hbase> t.put 'r1', 'c1', 'value', ts1

scan
----

Scans a table. You pass table name and optionally a dictionary of scanner
specifications. Scanner specifications may include one or more of
the following: ``TIMERANGE``, ``FILTER``, ``LIMIT``, ``STARTROW``, 
``STOPROW``, ``TIMESTAMP``, ``MAXLENGTH``, ``COLUMNS``, or ``CACHE``.
If no columns are specified, all columns will be
scanned.

Commands
########

None.

Usage Examples
##############

To scan all members of a column family, leave the qualifier empty as
in ``'col_family:'``.

The filter can be specified in two ways:

- Using a ``filterString`` – more information on this is available in
the Filter Language document attached to the HBASE-4176 JIRA.
- Using the entire package name of the filter.
  Some examples:
  
     hbase> scan '.META.'
     hbase> scan '.META.', {COLUMNS => 'info:regioninfo'}
     hbase> scan 't1', {COLUMNS => ['c1', 'c2'], LIMIT => 10, STARTROW => 'xyz'}
     hbase> scan 't1', {COLUMNS => 'c1', TIMERANGE => [1303668804, 1303668904]}
     hbase> scan 't1', {FILTER => "(PrefixFilter ('row2') AND
     (QualifierFilter (>=, 'binary:xyz'))) AND (TimestampsFilter ( 123, 456))"}
     hbase> scan ``t1``, {FILTER => org.apache.hadoop.hbase.filter.ColumnPaginationFilter.new(1, 0)}

For experts, there is an additional option—``CACHE_BLOCKS``— which
switches block caching for the scanner on (true) or off (false). By
default it is enabled. 

Examples:

   hbase> scan 't1', {COLUMNS => ['c1', 'c2'], CACHE_BLOCKS => false}


Also for experts, there is an advanced option—``RAW``—which instructs
the scanner to return all cells (including delete markers and uncollected
deleted cells).  This option cannot be combined with requesting specific
COLUMNS and is disabled by default.

Example:

:: 

   hbase> scan 't1', {RAW => true, VERSIONS => 10}

Besides the default ``toStringBinary`` format, ``scan`` supports custom
formatting by column. A user can define a ``FORMATTER`` by adding it to the column
name in the scan specification. The ``FORMATTER`` can be stipulated:

#. as an `org.apache.hadoop.hbase.util.Bytes`` method name (e.g., ``toInt``, ``toString``)
#. as a custom class followed by method name. For example: ``c(MyFormatterClass).format``


Example formatting ``cf:qualifier1`` and ``cf:qualifier2`` both as Integers:

:: 

   hbase> scan 't1', {COLUMNS => ['cf:qualifier1:toInt', 'cf:qualifier2:c(org.apache.hadoop.hbase.util.Bytes).toInt'] }


Note that you can specify a FORMATTER by column only (``cf:qualifer``).
You cannot specify a ``FORMATTER`` for all columns of a column family.

Scan can also be used directly from a table, by first getting a
reference to a table, like such:

::

   hbase> t = get_table 't'
   hbase> t.scan

Note in the above situation, you can still provide all the filtering,
columns, options, etc as described above.


truncate
--------


Disables, drops, and recreates the specified table.

Commands
########

None.

Usage Examples
##############

Examples:

::

   hbase>truncate 't1'


HBase Surgery Tools
===================

assign
------

Assigns a region. Use with caution. If region already assigned,
this command will do a force reassign. For experts only.

Commands
########

None.

Usage Examples
##############

::

   hbase> assign 'REGION_NAME'

balancer
--------

Triggers the cluster balancer. Returns true if balancer ran and was
able to tell the region servers to unassign all the regions to balance 
(the re-assignment itself is async). Otherwisek, returns false 
(and will not run if regions in transition).

Commands
########

::

   hbase> balancer

balance_switch
--------------

Enables/disables balancer. Returns the previous ``balancer`` state.

Commands
########

None.

Usage Examples
##############

::

   hbase> balance_switch true
   hbase> balance_switch false

close_region
------------

Closes a single region and asks the master to close a region out on the
cluster or if ``'SERVER_NAME'`` is supplied, asks the designated hosting
regionserver to close the region directly. 

Closing a region, the master expects ``'REGIONNAME'``
to be a fully qualified region name. When asking the hosting
regionserver to directly close a region, you pass the regions' 
encoded name only. A region name looks like
this: ``TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.``

The trailing period is part of the regionserver name. A region's 
encoded name is the hash at the end of a region name. For example:
``527db22f95c8a9e0116f0cc13c680396`` (without the period). 

A ``'SERVER_NAME'`` is its host, port plus startcode. 
For example: ``host187.example.com,60020,1289493121758`` 
(find servername in master ui or when you do detailed status in shell). 
This command will end up running close on the region hosting regionserver. 
The close is done without the master's involvement. (It will not know of the close.) 
Once closed, region will stay closed. Use ``assign`` to reopen/reassign. 
Use ``unassign`` or move to assign
the region elsewhere on cluster. Use with caution. For experts only.

Commands
########

None.

Usage Examples
##############

::

   hbase> close_region 'REGIONNAME'
   hbase> close_region 'REGIONNAME', 'SERVER_NAME'

compact
-------

Compacts all regions in the passed table or passes a region row
to compact an individual region. You can also compact a single-column
family within a region.

Commands
########

None.

Usage Examples
##############

Compact all the regions in a table:

::

   hbase> compact 't1'

Compact an entire region:

::

   hbase> compact 'r1'

Compact only a column family within a region:

::  

   hbase> compact 'r1', 'c1'

Compact a column family within a table:

::

   hbase> compact 't1', 'c1'

flush
-----


Flushes all the regions in a passed table or passes a region row to
flush an individual region. 

Commands
########

None.


Usage Examples
##############

::

   hbase> flush 'TABLENAME'
   hbase> flush 'REGIONNAME'

major_compact
-------------

Runs major compaction on a passed table or passes a region row
to major compact an individual region. 

Commands
########

None.

Usage Examples
##############

To compact a single-column family within a region specify 
the region name followed by the column family name.

Compact all regions in a table:

::

   hbase> major_compact 't1'

Compact an entire region:

::

   hbase> major_compact 'r1'

Compact a single column family within a region:

::

   hbase> major_compact 'r1', 'c1'

Compact a single-column family within a table:

::

   hbase> major_compact 't1', 'c1'

move
----

Moves a region. Optionally, you can specify a target regionserver 
or one is chosen at random.  NOTE: You pass the encoded region name, 
not the region name so this command is a little different to the others. 
The encoded region name is the hash suffix on region names. For example,
if the region name were ``TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.``,
then the encoded region name portion is ``527db22f95c8a9e0116f0cc13c680396``.

Command
#######

None.

Usage Examples
##############

A server name consists of its hostname, port, and startcode. 
For example: ``host187.example.com,60020,1289493121758``

::

   hbase> move 'ENCODED_REGIONNAME'
   hbase> move 'ENCODED_REGIONNAME', 'SERVER_NAME'

split
-----

Splits the entire table or passes a region to split individual regions. With
the second parameter, you can specify an explicit split key for the
region.

Commands
########

None.

Usage Examples
##############


::
   
   hbase> split 'tableName'
   hbase> split 'regionName' 

Use the following format: ``'tableName,startKey,id'``

::

   hbase> split 'tableName', 'splitKey'
   hbase> split 'regionName', 'splitKey'

unassign
--------

Unassigns a region. Unassigning will close the region in current location and
then reopen it again. Pass ``true`` to force the unassignment (``force`` will
clear all in-memory state in master before the reassign. If results in
double assignment use ``hbck -fix`` to resolve. To be used by experts).
Use with caution. For expert use only. 

Commands
########

None.

Usage Examples
##############

::

   hbase> unassign 'REGIONNAME'
   hbase> unassign 'REGIONNAME', true

hlog_roll
---------

Rolls the log writer. That is, it starts writing log messages to a new
file. The name of the regionserver should be given as the parameter. 
A ``server_name`` is the host, port, and  startcode of a regionserver.

For example: ``host187.example.com,60020,1289493121758`` (Find ``servername`` in
master ui or when you do detailed status in shell.)

Commands
########

None.

Usage Examples
##############

::

   hbase>hlog_roll

zk_dump
-------

Dumps the status of HBase cluster as seen by ZooKeeper. 

Commands
########

None.

Usage Examples
##############

::

   hbase>zk_dump

Cluster Replication Tools
=========================

add_peer
--------

Adds a peer cluster to replicate to, the ID must be short and
the cluster key is composed in the following way:
``hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent``

Commands
########

None.

Usage Examples
##############

This gives a full path for HBase to connect to another cluster.

:: 

   hbase> add_peer '1', "server1.cie.com:2181:/hbase"
   hbase> add_peer '2', "zk1,zk2,zk3:2182:/hbase-prod"

remove_peer
-----------

Stops the specified replication stream and deletes all the meta
information kept about it. 

Commands
########

None.


Usage Examples
##############

::

   hbase> remove_peer '1'

list_peers
----------

Lists all replication peer clusters.

Commands
########

None.


Usage Examples
##############

::

   hbase> list_peers

enable_peer
-----------

Restarts the replication to the specified peer cluster,
continuing from where it was disabled.

Commands
########

None.

Usage Examples
##############

::

   hbase> enable_peer '1'

disable_peer
------------

Stops the replication stream to the specified cluster, but still
keeps track of new edits to replicate.

Commands
########

None.

Usage Examples
##############

::

   hbase> disable_peer '1'

start_replication
-----------------

Restarts all the replication features. The state in which each
stream starts in is undetermined.
**WARNING:** start/stop replication is only meant to be used in critical load
situations.

Commands
########

None.

Usage Examples
##############

::

   hbase> start_replication

stop_replication
----------------

Stops all the replication features. The state in which each
stream stops in is undetermined.
**WARNING:** start/stop replication is only meant to be used in critical load
situations.

Commands
########

None.

Usage Examples
##############

::

   hbase> stop_replication

Security Tools
==============

grant
-----

Grants users specific rights.

**Syntax:** grant permissions is either zero or more letters from the set
"RWXCA".

For example: READ('R'), WRITE('W'), EXEC('X'), CREATE('C'), ADMIN('A')

Commands
########

None.

Usage Examples
##############

::

   hbase> grant 'bobsmith', 'RWXCA'
   hbase> grant 'bobsmith’, 'RW’, 't1', 'f1', 'col1'

revoke
------

Revokes a user's access rights.

Commands
########

None.

Usage Examples
##############

::

   hbase> revoke 'bobsmith', 't1', 'f1', 'col1'

user_permission
---------------

Shows all permissions for the particular user.

Commands
########

None.

Usage Examples
##############

::

   hbase> user_permission
   hbase> user_permission 'table1'

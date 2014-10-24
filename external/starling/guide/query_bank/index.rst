==========
Query Bank
==========

Hive
====

Number of Jobs Run by a User
----------------------------

Query
#####

``SELECT COUNT(job_id) FROM starling_jobs WHERE user-'dfsload' and grid-'MG' and dt-'2011_12_03';``

Sample Result
#############

::

    16003

Number of Jobs Run Each Day
---------------------------

Query
#####

``SELECT COUNT(1), dt FROM starling_jobs WHERE grid-'MB' and dt>-'2011_07_11' and dt <- '2011_07_13' GROUP BY dt;``

Sample Result
#############

::

    12178       2011_07_11
    8816        2011_07_12
    8983        2011_07_13

Wait Times for Jobs in a Date#Range
-----------------------------------

Query
#####

::

    SELECT COUNT(1) AS job_count, t.wait_time
    FROM (SELECT ROUND(wait_time/1000)
    AS wait_time, job_id FROM starling_jobs WHERE grid-'MB'
    AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') >- UNIX_TIMESTAMP('2011_07_11','yyyy_MM_dd'))
    AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') <- UNIX_TIMESTAMP('2011_07_13','yyyy_MM_dd'))) t
    GROUP BY t.wait_time;


Sample Result
#############

::

    74      0
    1001    1
    2112    2
    3321    3
    4402    4
    5503    5
    [...]
    1       2781
    1       2832
    1       3563
    1       4058
    1       7604

Total HDFS Bytes Read (in GB) by Non#dfsload Jobs in a Date#Range
-----------------------------------------------------------------

Query
#####

::

    SELECT ROUND((SUM(total_counters['HDFS_BYTES_READ']))/1073741824) AS bytes_read_in_gb, starling_job_counters.dt
    FROM starling_job_counters
    JOIN (SELECT job_id FROM starling_jobs WHERE user <> 'dfsload') t ON (starling_job_counters.job_id-t.job_id)
    WHERE starling_job_counters.grid-'MB'
    AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') >- UNIX_TIMESTAMP('2011_07_11','yyyy_MM_dd'))
    AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') <- UNIX_TIMESTAMP('2011_07_13','yyyy_MM_dd'))
    GROUP BY starling_job_counters.dt;

Sample Result
#############

::

    190532      2011_07_11
    336164      2011_07_12
    219601      2011_07_13




Data Locality
-------------

How much data is being read local to a rack (from a data node in the same rack) vs. data read from off rack.


Query
#####

::

    select  T.grid, T.dt, round(avg(T.datalocal)), round(avg(T.racklocal)), round(avg(T.others))
    from (
            select
                    J.grid grid, J.dt dt, J.jobid,
                    (J.datalocal * 100)/J.total datalocal,
                    (J.rack * 100)/J.total racklocal,
                    ((J.total # J.datalocal # J.rack) * 100)/J.total others
            from (
                select
                    grid, dt, jobid,
                    cast(counters['Job Counters/Launched map tasks'] as bigint)  total,
                    cast(counters['Job Counters/Data#local map tasks'] as bigint) datalocal,
                    cast(counters['Job Counters/Rack#local map tasks'] as bigint) rack
                from job
        ) J 
        where J.total is not null and J.datalocal is not null and J.rack is not null and
          J.total > 0 and J.datalocal > 0 and J.rack > 0
    ) T
    group by T.grid, T.dt;

Sample Result
#############

??

Instances Read on Dilithium Gold
--------------------------------

For the directories ``/data/SDS/data`` and ``/data/FETL/*``, what were the oldest, newest 
instances read and how many times were individual pieces read on Dilithium Gold.

If you want to save these results to import into excel or other program, 
save this query in a file and execute: ``/home/y/bin/hive #f foobar.file >results.csv``. 
You can then import the results.csv file into excel using tab as the delimiter.

.. note:: ``INSERT OVERWRITE LOCAL DIRECTORY 'test.csv'`` won't do what you think it might do. 
          The ```test.csv`` directory will contain a single hadoop compressed file that isn't human readable.

Query
#####

:: 

    select F.grid as GRID, F.dt as DT, F.ugi as USER,
    regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 4) as DATASET,
       min(regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 5)) as FIRST_INSTANCE,
       max(regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 5)) as LAST_INSTANCE,
       count(1) as COUNT
    from (
        select src.grid as grid,
          src.dt as dt,
          src.ugi as ugi,
          src.src_path as src_path
        from  starling_fs_audit src 
    where 
        src.grid-'DG' and src.dt-'2011_11_08'
        and regexp_extract(src.src_path,'(/data/SDS/data)/([^/]*)/([^/]*)', 1) -- '/data/SDS/data'
    union all 
    select dest.grid as grid,
          dest.dt as dt,
          dest.ugi as ugi,
          dest.dest_path as src_path
    from  starling_fs_audit dest 
    where 
        dest.grid-'DG' and dest.dt-'2011_11_08'
        and regexp_extract(dest.dest_path,'(/data/SDS/data)/([^/]*)', 1) -- '/data/SDS/data'
        ) F
    group by F.grid, F.dt, F.ugi, 
        regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 4)
    order by GRID, DT,
        DATASET, USER;

Now do the same for ``/data/FETL/{ABF,LL_Web}/``::

    INSERT OVERWRITE LOCAL DIRECTORY 'DGabfusage20111108.csv'
    select F.grid as GRID, F.dt as DT, F.ugi as USER,
        regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 4) as DATASET,
        min(regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 5)) as FIRST_INSTANCE,
        max(regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 5)) as LAST_INSTANCE,
        count(1) as COUNT
    from (
    select src.grid as grid,
         src.dt as dt,
         src.ugi as ugi,
         src.src_path as src_path
    from starling_fs_audit src 
    where 
        src.grid-'DG' and src.dt-'2011_11_08'
        and regexp_extract(src.src_path,'(/data/FETL/[^/]*)/([^/]*)/([^/]*)', 1) -- '/data/SDS/data'
    union all 
    select dest.grid as grid,
          dest.dt as dt,
          dest.ugi as ugi,
          dest.dest_path as src_path
    from  starling_fs_audit dest 
    where 
        dest.grid-'DG' and dest.dt-'2011_11_08'
        and regexp_extract(dest.dest_path,'(/data/FETL/[^/]*)/([^/]*)', 1) -- '/data/SDS/data'
        ) F
    group by F.grid, F.dt, F.ugi, 
        regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 4),
    order by GRID, DT,
        DATASET, USER;

Sample Result
#############

TBD


Pig
===

Number of Jobs Run by a User
----------------------------

Pig Statements
##############

TBD


Sample Result
#############

TBD

MapReduce
=========

We currently do not have examples for MapReduce, but needed,
write to yahoo#hcatalog#dev@yahoo#inc.com.

==========
Query Bank
==========

To help Starling users (product managers, application developers and service engineers), we've 
aggregated some commonly used queries. We have to categorize
the queries by the type of user in the future, and also provide examples of other
ways to access Starling data such as Pig commands, MapReduce examples, etc.


Number of Jobs Run by a User
============================

Query
-----

``SELECT COUNT(job_id) FROM starling_jobs WHERE user='dfsload' and grid='MG' and dt='2011_12_03';``

Sample Result
-------------

::
    16003

Total Node Hours Per Hour For A User
==================================== 

The following will produce the number of mapper and reducer hours per hour for the headless user ngdg for the first 9 days in August for all grids that ngdg runs jobs on.  The output format is (date, cluster, mapper hours, reducer hours).

Query
-----

``select dt, grid, ROUND(SUM(map_slot_seconds/3600/24),2), ROUND(SUM(reduce_slots_seconds/3600/24),2) from starling_job_summary where user="ngdg" and dt like "2012_08_0%" group by dt, grid order by dt;``

Sample Result
-------------

::

    2012_08_01      DG      2392.44 834.51
    2012_08_01      DB      2347.42 753.75
    2012_08_02      DB      2301.6  818.7
    2012_08_02      DG      2250.99 798.89
    2012_08_03      DB      2246.12 880.06
    2012_08_03      DG      2291.35 808.22
    2012_08_04      DB      2071.67 747.37
    2012_08_04      DG      2025.18 757.98
    2012_08_05      DG      1983.91 755.87
    2012_08_05      DB      1967.28 767.83
    2012_08_06      DB      2345.86 886.28
    2012_08_06      DG      2337.93 865.73
    2012_08_07      DB      2431.53 895.75
    2012_08_07      DG      2442.62 978.21
    2012_08_08      DB      2389.24 905.97
    2012_08_08      DG      2479.75 1030.1
    2012_08_09      AB      0.01    0.0
    2012_08_09      DB      2384.97 893.53
    2012_08_09      DG      2530.56 1015.28


Total Node Hours Per Hour For a User By Queue
=============================================

The following will produce the number of mapper and reducer hours per hour per queue 
for the headless user ``ngdg`` for August 27th for the Dilithium-Gold (DG) cluster.  
The output format is (date, cluster, queue, mapper hours, reducer hours).

Query
-----


``select dt, grid, queue, ROUND(SUM(map_slot_seconds/3600/24),2), ROUND(SUM(reduce_slots_seconds/3600/24),2) from starling_job_summary where user="ngdg" and grid like "DG" and dt like "2012_08_27" group by dt, grid, queue order by dt, queue, grid;``

Sample Result
-------------

::

   2012_08_27      DG      apg_dailylow_p6 21.66   0.02
   2012_08_27      DG      apg_hourlyhigh_p1       1177.7  389.97
   2012_08_27      DG      apg_hourlylow_p4        347.26  30.81
   2012_08_27      DG      apg_hourlymedium_p2     1429.49 720.99
   

Total Node Hours Per Hour for Each Job for a User
=================================================


The following will produce the number of mapper and reducer hours per hour for each 
job for the headless user jagpip for August 15th for the Phazon-Tan (PT) cluster.  
Note that dashes and underscores are removed from the job names (but the query 
could be adjusted to leave them in).  The output format is (job name, mapper hours, reducer hours).

Query
-----

::

    SELECT regexp_replace(regexp_replace(regexp_replace(starling_jobs.job_name, "\\d", ""), "PigLatin:", ""), "-", ""), 
    round(SUM(map_slot_seconds/3600/24)), round(SUM(reduce_slots_seconds/3600/24)) from starling_job_summary JOIN starling_jobs ON (starling_job_summary.job_id = starling_jobs.job_id) WHERE starling_jobs.user == 'jagpip' and starling_job_summary.user == 'jagpip' and starling_jobs.dt == '2012_08_15'  and starling_job_summary.dt == '2012_08_15'  and starling_job_summary.grid == 'PT' GROUP BY regexp_replace(regexp_replace(regexp_replace(starling_jobs.job_name, "\\d", ""), "PigLatin:", ""), "-", "");

Sample Result
-------------


::

    AMGD    1       0
    DEDUPEDGDTIMEBASED      0       0
    DHTransformerJagCGETLJob        158     0
    DefaultJobName  11      1
    FIXEDCOSTCAMPAIGNSTATE  0       0
    FIXEDCOSTPREAGG 0       0
    GDPREAGG        7       33
    GDQUOTASERVER   0       0
    GHOSTADS        0       0
    IMSQUOTASERVER  0       0
    IMS_MOROCCO_HOURLY      30      0
    JAGCGACTAPEXCLICKSHOURLY        0       0
    JAGCGACTAPEXSRVTGTSRVHOURLY     1       0
    JAGCGAPTSCGDSCSIPARTSELECT      1       2
    JAGCGAPTUAD     16      8




Number of Jobs Run Each Day
===========================

Query
-----

``SELECT COUNT(1), dt FROM starling_jobs WHERE grid='MB' and dt>='2011_07_11' and dt <= '2011_07_13' GROUP BY dt;``

Sample Result
-------------

::

    12178       2011_07_11
    8816        2011_07_12
    8983        2011_07_13

Wait Times for Jobs in a Date-Range
===================================

Query
-----

::
    SELECT COUNT(1) AS job_count, t.wait_time
      FROM (SELECT ROUND(wait_time/1000)
        AS wait_time, job_id FROM starling_jobs WHERE grid='MB'
        AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') >= UNIX_TIMESTAMP('2011_07_11','yyyy_MM_dd'))
        AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') <= UNIX_TIMESTAMP('2011_07_13','yyyy_MM_dd'))) t
        GROUP BY t.wait_time;


Sample Result
-------------

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

Total HDFS Bytes Read (in GB) by Non-dfsload Jobs in a Date-Range
=================================================================

Query
-----

::

    SELECT ROUND((SUM(total_counters['HDFS_BYTES_READ']))/1073741824) AS bytes_read_in_gb, starling_job_counters.dt
      FROM starling_job_counters
      JOIN (SELECT job_id FROM starling_jobs WHERE user <> 'dfsload') t ON (starling_job_counters.job_id=t.job_id)
      WHERE starling_job_counters.grid='MB'
      AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') >= UNIX_TIMESTAMP('2011_07_11','yyyy_MM_dd'))
      AND (UNIX_TIMESTAMP(dt,'yyyy_MM_dd') <= UNIX_TIMESTAMP('2011_07_13','yyyy_MM_dd'))
      GROUP BY starling_job_counters.dt;

Sample Result
-------------

::

    190532      2011_07_11
    336164      2011_07_12
    219601      2011_07_13




Data Locality
=============

How much data is being read local to a rack (from a data node in the same rack) vs. data read from off rack.


Query
-----

::

    select 
        T.grid, T.dt, round(avg(T.datalocal)), round(avg(T.racklocal)), round(avg(T.others))
        from (
            select
                    J.grid grid, J.dt dt, J.jobid,
                    (J.datalocal * 100)/J.total datalocal,
                    (J.rack * 100)/J.total racklocal,
                    ((J.total - J.datalocal - J.rack) * 100)/J.total others
            from (
                select
                    grid, dt, jobid,
                    cast(counters['Job Counters/Launched map tasks'] as bigint)  total,
                    cast(counters['Job Counters/Data-local map tasks'] as bigint) datalocal,
                    cast(counters['Job Counters/Rack-local map tasks'] as bigint) rack
                from job
        ) J 
        where J.total is not null and J.datalocal is not null and J.rack is not null and
          J.total > 0 and J.datalocal > 0 and J.rack > 0
    ) T
    group by T.grid, T.dt;

Sample Result
-------------

??

Instances Read on Dilithium Gold
================================

For the directories ``/data/SDS/data`` and ``/data/FETL/*``, what were the oldest, newest 
instances read and how many times were individual pieces read on Dilithium Gold.

.. note:: If you want to save these results to import into Excel or other program, save this 
          query in a file and execute ``/home/y/bin/hive -f foobar.file >results.csv``. You can 
          then import the ``results.csv`` file into excel using tab as the delimiter.

          Also, ``INSERT OVERWRITE LOCAL DIRECTORY 'test.csv'`` won't do what you think it might do. 
          The ``test.csv`` directory will contain a single Hadoop compressed file not usable by mere mortals.

Query
-----

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
       src.grid='DG' and src.dt='2011_11_08'
       and regexp_extract(src.src_path,'(/data/SDS/data)/([^/]*)/([^/]*)', 1) == '/data/SDS/data'
    union all 
       select dest.grid as grid,
          dest.dt as dt,
          dest.ugi as ugi,
          dest.dest_path as src_path
       from  starling_fs_audit dest 
       where 
          dest.grid='DG' and dest.dt='2011_11_08'
          and regexp_extract(dest.dest_path,'(/data/SDS/data)/([^/]*)', 1) == '/data/SDS/data'
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
       from  starling_fs_audit src 
       where 
          src.grid='DG' and src.dt='2011_11_08'
          and regexp_extract(src.src_path,'(/data/FETL/[^/]*)/([^/]*)/([^/]*)', 1) == '/data/SDS/data'
       union all 
       select dest.grid as grid,
          dest.dt as dt,
          dest.ugi as ugi,
          dest.dest_path as src_path
       from  starling_fs_audit dest 
       where 
          dest.grid='DG' and dest.dt='2011_11_08'
          and regexp_extract(dest.dest_path,'(/data/FETL/[^/]*)/([^/]*)', 1) == '/data/SDS/data'
    ) F
    group by F.grid, F.dt, F.ugi, 
       regexp_extract(F.src_path,'/([^/]*)/([^/]*)/([^/]*)/([^/]*)', 4),
    order by GRID, DT,
       DATASET, USER;


Advanced Examples
=================


Using Starling With MySQL
-------------------------

This example is from the Slingshot team who are using 
Starling to get a list of jobs with both the expected
and actual grid performance. The team can
then compare to find those jobs that are
bottlenecks. 

Process for Collecting Starling Data
####################################

To get data from Starling to MySQL, the Slingshot
team runs an Oozie job that runs a Pig script grid_monitor_hourly
that simply dumps data from the
``starling_job_summary`` table into HDFS.
They then use a shell script (``grid_monitor_fetch.sh``)
to import the data into their MySQL database ``grid``.


MySQL Database Structure
########################

The ``grid`` database has the following three tables:

- ``job_expect`` - contains job lists and each job's expected performance.
- ``job_summary`` - stores a dump of the Yahoo grid meta data.
- ``job_actual`` - shows the actual resource usage and performance. 

The ``job_actual`` table is created with the following query::

   SELECT "2014-09-30", grid, queue, user, job_name, ROUND(SUM(GB_Hours)) as TOT_GB_Hours, count(*) as NUM_jobs,  ROUND (SUM(GB_Hours)/count(*), 0)  as AVG_GB_Hours_per_job
   FROM (
       SELECT grid, queue, user, job_name,(
         map_slot_seconds * resources_per_map + reduce_slots_seconds * resources_per_reduce
         ) / ( 1000 *3600 ) AS GB_Hours
       FROM `starling_job_summary`
       WHERE date = '2014-09-30'
   ) t
   group by grid, queue, user, job_name
   order by TOT_GB_Hours desc;


Metric for Comparing Performance
################################

The metric that they use to measure the performance is
called a *GB hour*. The GB hour represents the number
of Gigabytes of memory allocation per hour for MapReduce
jobs and is calculated from Starling
data in the following way:

#. look at map slot seconds and reduce slot seconds.
#. convert to hours
#. look at allocated memory for map and reduce for each job.
#. (map_slot_sec/3600) * (map_mb/1000) = GB hour Map + GB hour Reduce 

Example Query
#############

The Slingshot team can use the ``job_actual`` table to identify problematic
jobs and then drill down to get more information about a particular job
with a query like the one below.

::

    SELECT grid, job_name, run_time, num_maps, num_reduces, (
       map_slot_seconds * resources_per_map + reduce_slots_seconds * resources_per_reduce
    ) / ( 1000 *3600 ) AS GB_Hours
    FROM `starling_job_summary` 
    WHERE job_name LIKE '%hattrick_gen_train_dataset.pig%'
    AND date = '2014_09_27'
    LIMIT 0 , 30







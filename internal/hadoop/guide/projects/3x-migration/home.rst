.. _projects_3x-migration:

####################
Hadoop 3.x Migration
####################

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Migration Plan
==============


Upmerge
-------

In order to minimize the upmerge of internal JIRAs to 3.x, it would be best to
push as many internal JIRAs to Apache trunk, 3.3, and 3.2 as possible.

  YARN
    `GDoc - Y-Hadoop-2.10-YARN <https://docs.google.com/spreadsheets/d/1flCClAx9iJvsc8nOUeIn3aurwIS5ieWacGb_ffZXfJ8/edit?usp=sharing>`_

  HDFS
    `GDoc - Y-Hadoop-2.10-HDFS <https://docs.google.com/spreadsheets/d/1GMDEK84D715OoYrKbeirIi1rmTbAl3_-7PpQ9eNyWLE/edit?usp=sharing>`_



Questions and Concerns
----------------------

Team Questions
^^^^^^^^^^^^^^^

.. rubric:: Daryn’s concerns about 3.X HDFS

- EC
- overhaul of the block schedulers
- Negative block IDs cause incompatibilities with EC. Do we have any?
  
  - We have some. It looks manageable though. (approx 5M files). Almost all of
    it is old crufty stuff in ``/mapred`` (vintage 2012).

    .. code-block:: bash

      hive -e 'select grid, regexp_extract(path,"(/[^/]+).*",1),count(*)
               from starling_fs_blocks
               where ( block_id<0 and DT="2020_08_01")
               group by grid,regexp_extract(path,"(/[^/]+).*",1)'


Community Questions
^^^^^^^^^^^^^^^^^^^

Sunil
  - For Cloudera customers, RU was not possible due to edit log changes.
    
    - `HDFS-13596: NN restart fails after RollingUpgrade from 2.x to 3.x <https://issues.apache.org/jira/browse/HDFS-13596>`_
    - `HDFS-6440: Support more than 2 NameNodes <https://issues.apache.org/jira/browse/HDFS-6440>`_

WeiChu
  - Cloudera customers had problems with dependeincies such as guava.
  - Their customers have done lots of upgrades between 2.x and 3.x, but not rolling.
  - Lots of cases where DN failed because it used more memory.
  - They had to change the max heapsize before upgrade.
  - Sometimes their customers had to roll back if failed in middle to fix the edit logs.
  - Edit logs were fine after upgrades if the upgrade succeeded."

Brahma
  - DD has done RU upgrades and given presentation on their bottlenecks.
    There is a presentation somewhere. The problems DD encountered are similar to Brahma's.
  - Brahma's company had 8k nodes runnin 2.7 and upgraded to 3.1.
  - They pushed back all of their changes to Apache
    (`HDFS-6350 <https://issues.apache.org/jira/browse/HDFS-6350>`_).
    
    - They Disabled EC and other new features ([#f1]_) during RU.
    - Token secrets fields are different between 2.x and 3.x. Token verification
      is different. They fixed issue to ignore some fields so writes would be
      successful. ([#f2]_)
    - They used distribted cache in cross-version job submission ([#f3]_)
    - ACL needed to use a 3.1 client ([#f3]_)
    - Files won't be deleted during RU, so they made Trash bigger ([#f4]_)
    - Yahoo Japan is trying to upgrade from 2.6 to 3.3.

    .. note:: This was the link they shared to the HDFS Meeting Notes.
              DD's presentation was on 1/02/2020: |br|
              GDoc - `Storage: HDFS, Cloud connectors Community Sync Up -
              1/02/2020 <https://docs.google.com/document/d/1jXM5Ujvf-zhcyw_5kiQVx6g-HeKe-YGnFS_1-qFXomI/edit?pli=1#heading=h.irqxw1iy16zo>`_


Action Items
------------


+---------------------------------------------------------------------------+--------------+-------+----------+
|                                Action Item                                |     State    | Owner | Comments |
+===========================================================================+==============+=======+==========+
| Set up meeting with Target                                                |   |check_|   | EricP |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| Determine if we can use the YARN call next week to discuss 3.x migration. |   |check_|   | EricP |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| Who are the major users of on-prem Hadoop?                                |  |uncheck_|  |       |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| What are the versions they are running?                                   |  |uncheck_|  |       |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| What scale are they running at?                                           |  |uncheck_|  |       |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| What versions are AWS, GCP at?                                            |  |uncheck_|  |       |          |
+---------------------------------------------------------------------------+--------------+-------+----------+
| Is anyone running the threaded scheduler?                                 |  |uncheck_|  |       |          |
+---------------------------------------------------------------------------+--------------+-------+----------+


Case Studies
============

Other companies’ experiences with migrating from 2.x to 3.x

DiDi
----

- `Presentation (English) <https://cloudera.zoom.us/rec/share/7MF_dLX0339OY5391xvkZP8NLrXieaa8gyZK-fYJnUkGOUUXvaUh5cl_6AVYetQl>`_
- `Slides (Chinese) <https://drive.google.com/open?id=1iwJ1asalYfgnOCBuE-RfeG-NpSocjIcy>`_

Yahoo Japan
-----------

- `Yahoo! JAPAN Tech Blog (Japanese Version) <https://techblog.yahoo.co.jp/entry/20191206786320/>`_
- `Yahoo! JAPAN Tech Blog (English Version) <https://techblog.yahoo.co.jp/entry/20191206786320/#Compatibility%20Analysis%20of%20Running%20HDP%202.6.4%20Execution%20Frameworks%20on%20HDFS%20of%20Apache%20Hadoop%203.2.1>`_

Target
------

- They moved from storage based autentication to ranger based authentication
- 2.7 clusters have about 900 nodes, 3.2 clusters have about 400 nodes.
- 300TB - 400TB RAM in clusters
- They run about 20000 jobs per day
- No RU between 2.7 and 3.1.
- 3.1 clusters were newly built-out. They did RU from 3.1 to 3.2.
- 3.3 has several version upgrades of dependent components: `protobuf`, `guava`
- Classpath isolation in nodemanager doesn't work properly

.. rubric:: Q&As

- Is Target using 2.7 HDFS and 3.2 YARN? `Ans:` **No**
- Has Target done RU from 2.7 to 3.2?    `Ans:` **No**
       

Cloud Services Versions
=======================

- `AWS is 2.8 or 3.2 <https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop.html>`_
- GCS

  + `2.10 <https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-1.5>`_
  + `3.2 <https://cloud.google.com/dataproc/docs/concepts/versioning/dataproc-release-preview>`_


Related HDFS JIRAs
==================

- `HDFS-13596: NN restart fails after RollingUpgrade from 2.x to 3.x <https://issues.apache.org/jira/browse/HDFS-13596>`_
- `HDFS-14396: Failed to load image from FSImageFile when downgrade from 3.x to 2.x <https://issues.apache.org/jira/browse/HDFS-14396>`_
- `HDFS-14509: DN throws InvalidToken due to inequality of password when upgrade NN 2.x to 3.x <https://issues.apache.org/jira/browse/HDFS-14509>`_
- `HDFS-6984: Serialize FileStatus via protobuf <https://issues.apache.org/jira/browse/HDFS-6984>`_

  .. note:: [`HDFS-6984 <https://issues.apache.org/jira/browse/HDFS-6984>`_]
            jira is incompatible for ACL commands. Only hadoop-3 clients will work
            against hadoop-3 server during the upgrade.

Readings And Documentations
===========================

.. include:: /common/team/projects/3x-migration/3x-migration-reading-resources.rst

.. rubric:: Footnotes

.. [#f1] didn't elaborate
.. [#f2] I think they pushed this back?
.. [#f3] I'm not sure what this means
.. [#f4] Not sure how they did that--removed quotas? or did they do manual deletion of Trash?

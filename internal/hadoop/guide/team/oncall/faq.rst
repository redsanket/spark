.. _hadoop_team_oncall_faq:

***
FAQ
***

.. contents:: Table of Contents
  :local:
  :depth: 3



.. admonition:: Readings...
   :class: readingbox

    Regarding grid-ops runbooks,
    `yo/gridopsrunbook <https://yo/gridopsrunbooks>`_ is a big library of docs.
    One of the interesting ones is
    `Run Book / Operations Manual - Hadoop (YARN) <https://docs.google.com/document/d/1Db1Vz3pdv562Iei7w2l14GmWxv48yBYxi81uGu-eJR4/edit>`_
    and `Run Book / Operations Manual - Hadoop (HDFS) <https://docs.google.com/document/d/1-AHk-ePioUb2tXRedQSozLoDKYSgsHoYIo6B7daU9_M/edit>`_.
    |br|
    In the YARN runbook:
    
    * section "6 Operational Tasks," subsection "Troubleshooting",
      there is a lot of good stuff about tracking down bad/slow nodes and bad disks.
    * subsection "How to see which jobs are shuffling on a node", there are
      scripts to run for each framework (Tez, MapRed, Spark) that will grep
      through the NodeManager logs and provide counts of the busiest jobs.


How to verify Busy Machines
===========================

Check Disk business
-------------------

ssh to that box. Then use the following `ysar` tool. For more details about the
ysar, see :ref:`ysar tools <cmds_tools_ysar>`.

  .. code-block:: bash

    ysar -interval 1

For example, in :yahoo_jira:`HADOOPPF-51887: Time out due to unresponsive nodes on JB <HADOOPPF-51887>`,
the above command shows by minute you by the minute how busy the disks were.

  .. code-block:: bash

    # Output of ysar
    Time         req/s    msec %util %util  %pgs   /pkt  /pkt %busy %busy %busy %busy %busy %busy %full %full %full %full %full %full %full %full in-kbps outkbps
    10/20-02:00      -       -  89.1  64.1   0.0    0.0   3.1  36.3  44.5   0.0  13.7   0.0  38.1  26.1  31.0  79.5  79.4  80.8  79.3  76.7  16.4 439165.7 112459.3
    10/20-02:01      -       -  80.6  57.4   0.0    0.0   2.3  60.7  83.3   0.0  24.1   0.0  65.6  26.1  31.0  79.5  79.4  80.8  79.3  76.7  16.4 512761.7 218112.2
    10/20-02:02      -       -  77.9  60.0   0.0    0.0   1.3  63.2  83.3   0.1  32.4   0.0  50.5  26.1  31.0  79.5  79.5  80.8  79.3  76.8  16.4 948225.3 561348.2
    10/20-02:03      -       -  68.4  55.6   0.0    0.0   2.1  58.5 100.4   0.0  32.1   0.0  51.2  26.1  31.0  79.5  79.5  80.8  79.3  76.8  16.4 549848.6 548429.8
    10/20-02:04      -       -  60.6  57.4   0.0    0.0   3.0  60.5  85.1   0.0  53.0   0.0  63.3  26.1  31.0  79.6  79.5  80.8  79.4  76.8  16.4 639324.5 475464.5
    10/20-02:05      -       -  53.2  58.2   0.0    0.0   4.8  68.5 100.4   0.0  63.8   0.0  58.2  26.1  31.0  79.6  79.6  80.8  79.4  76.9  16.4 655913.0 206064.9
    10/20-02:06      -       -  23.4  54.8   0.0    0.0   6.0  77.6  98.7   0.0  90.3   0.0  89.0  26.1  31.0  79.6  79.6  80.8  79.5  76.9  16.4 324412.0 155907.6
    10/20-02:07      -       -  12.9  48.6   0.0    0.0   1.6  67.7 101.2   0.0  58.0   0.0  96.0  26.1  31.0  79.6  79.7  80.8  79.5  76.9  16.4 885812.5 536485.4
    10/20-02:08      -       -  27.6  49.0   0.0    0.0   1.6  60.1 100.3   0.1  23.7   0.0  29.5  26.1  31.0  79.3  79.6  80.8  79.4  76.9  16.4 1854428.5 358843.6
    10/20-02:09      -       -  62.0  58.3   0.0    0.0   3.5  43.6  99.9   0.0  20.2   0.0  26.4  26.1  31.0  79.3  79.6  80.8  79.4  76.9  16.4 320832.8 229727.1
    10/20-02:10      -       -  63.3  59.7   0.0    0.0   4.3  45.0  99.3   0.0  18.1   0.0  23.7  26.1  31.0  79.3  79.7  80.8  79.3  76.8  16.4 175236.1 223551.8
    10/20-02:11      -       -  67.1  58.7   0.0    0.0   6.1  37.9  99.9   0.0  17.1   0.0  23.3  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 97545.4 147142.1
    10/20-02:12      -       -  88.1  60.8   0.0    0.0   2.0  74.0 100.2   0.0  21.1   0.0  37.6  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 657132.0 284495.4
    10/20-02:13      -       -  89.1  62.4   0.0    0.0   1.3  39.5  91.0   0.4  18.8   0.0  38.5  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 1389978.9 221720.5
    10/20-02:14      -       -  88.0  61.1   0.0    0.0   2.4  62.2  99.0   0.0  54.9   0.0  54.6  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 842150.5 262190.2
    10/20-02:15      -       -  88.7  57.0   0.0    0.0   1.2  90.8 100.9   0.0  66.4   0.0  56.5  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 1252303.7 637949.4
    10/20-02:16      -       -  67.3  56.3   0.0    0.0   1.2  84.8  99.5   0.3  57.0   4.2  51.2  26.1  31.0  79.2  79.8  80.8  79.3  76.8  16.4 1125598.2 770905.2
    10/20-02:17      -       -  40.6  55.4   0.0    0.0   1.7  79.1 100.3   0.0  41.6   0.0  50.2  26.1  31.0  79.2  79.8  80.8  79.3  76.8  16.4 509814.4 1048711.3
    10/20-02:18      -       -  37.1  54.6   0.0    0.0   2.0  55.0 100.3   0.0  56.6   0.0  53.8  26.1  31.0  79.2  79.7  80.8  79.3  76.7  16.4 437812.2 1401159.2
    10/20-02:19      -       -  40.8  47.4   0.0    0.0   2.0  57.9  96.1   0.0  53.3   0.0  66.2  26.1  31.0  79.2  79.7  80.8  79.3  76.7  16.4 785201.6 688001.1
    10/20-02:20      -       -  52.0  51.0   0.0    0.0   1.5  59.5  80.3   0.0  49.3   0.0  93.8  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 1807494.6 617955.9
    10/20-02:21      -       -  66.5  62.6   0.0    0.0   1.6  67.4  91.6   0.0  65.8   0.0  81.0  26.1  31.0  79.2  79.7  80.8  79.3  76.8  16.4 2109142.2 529589.9


High ``%busy`` could be a reason for slow or irresponsive node.


You can also check the Yamas metrics charts (:numref:`monitoring_charts`).

* Disk IO MBps
* Disk IOPS
* Disk Space  




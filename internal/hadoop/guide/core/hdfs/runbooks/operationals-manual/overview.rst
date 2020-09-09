********
Overview
********

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Documents and Resources
=======================

* `HDFS Architecture-r2.10.0: <https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>`_
* `Design Specification: HDFS Append and Truncates <https://issues.apache.org/jira/secure/attachment/12370562/Appends.doc/>`_. :download:`Download link </resources/hdfs-appends.doc>`
* `design specification: Append, Hflush, and Read <https://issues.apache.org/jira/secure/attachment/12445209/appendDesign3.pdf/>`_. :download:`Download link </resources/appendDesign3.pdf>`
* `Roadmap to Support Significantly Larger Storage Namespace <https://docs.google.com/document/d/1tGvNhJb43kQpdPbf26cSu5Md4UZgPCkEb0Y1XMe8QlY/>`_
* `Run Book / Operations Manual - HDFS <https://docs.google.com/document/d/1-AHk-ePioUb2tXRedQSozLoDKYSgsHoYIo6B7daU9_M/>`_


Service Introduction - Infrastructure and Network Design
========================================================

Architecture overview diagram
-----------------------------


For details about HDFS architecture, see `HDFS Architecture-r2.10.0: <https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html>`_

.. figure:: /images/hdfs/hdfs-operations-manual-architecture.jpg
   :alt:  HDFS- Architecture overview diagram

   HDFS-2.10 Architecture overview diagram


Upstream dependencies
^^^^^^^^^^^^^^^^^^^^^

DNS
"""

LDAP
""""

Filer NFS
"""""""""

KDC
"""

Downstream dependencies
^^^^^^^^^^^^^^^^^^^^^^^

HDFSProxy
"""""""""

For more information see the `HDFSProxy confluence page <https://confluence.vzbuilders.com/display/HPROX/HDFS+Proxy>`_

HttpfsProxy
"""""""""""

* `Httpfs Proxy Dev Guide <https://confluence.vzbuilders.com/display/HPROX/Httpfs+Proxy+Dev+Guide>`_
* `Configuring Oath HttpFS for CertificateBased Auth <https://docs.google.com/document/d/1mjLerhHZeiOLChNyP33yZDsCB6AC8X6geqLbjrlxi00>`_

Hue
"""

Jupyter
"""""""

Roles
^^^^^

.. code-block:: bash

    sourceGrid.set.clusters_compute
		grid.$color.$clustername.datanode
		grid.$color.$clustername.namenode

HostNames
^^^^^^^^^^

#. Datanodes
   
   .. code-block:: bash
   
       gs$shortcolor${rackishnumber}n$node.$color.ygrid.yahoo.com

#. Namenodes
   
   .. code-block:: bash
   
       nn${shortcolor}${rackishnumber}n$node.$color.ygrid.yahoo.com
       $cluster$color-nn1-ha{1,2}.$color.ygrid.yahoo.com


.. figure:: /images/hdfs/hdfs-operations-manual-nn-convention.jpg
   :alt:  Namenode

   HDFS-2.10 Namenodes


Charcteristics
^^^^^^^^^^^^^^

OS
	Linux (6+)

Minimum Hardware Requirements
	None

Software platform
	Java

Utilization Metric
	* HDFS Capacity
	* Info on namespace usage impact on namenode memory

Performance Metric
	.. todo:: list metrics

Benchmarks
	None

Target utilization
	80% for HDFS Capacity

Clusters & Colo distribution
	`grid.set.clusters_compute <https://roles.corp.yahoo.com/ui/role?action=view&id=603814&bycrumb=Np_YGtMc6rIgCUOi_RF6Dwo-_gGR_x71aYECBXPkVek>`_

ACLs
^^^^

BF1 (RED)
	* `GRID::BF1::PROD_AUTH_UI_AND_REST <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/f50faf90-8f94-4ea9-811a-9f82fe6507fb>`_
	* `GRID::BF1::PROD_RFC1918 <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/bd3e9e8a-2111-39be-9e41-4258ea5b0796>`_

BF2 (RED)
	* `GRID::BF2::PROD_AUTH_UI_AND_REST <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/0484b79d-68fb-44d3-a7f0-e433cb63cd3d>`_
	* `GRID::BF2::PROD_RFC1918 <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/9ed04324-6379-33c4-a07e-e782922529b1>`_

GQ1 (BLUE)
	* `GRID::GQ1::PROD_AUTH_UI_AND_REST <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/c5295391-f4c0-48ce-89aa-63739c246447>`_
	* `GRID::GQ1::PROD_RFC1918 <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/8e1fdf6a-e8c7-3a2d-9b24-ef6050333cc5>`_

NE1 (TAN)
	* `GRID::NE1::PROD_AUTH_UI_AND_REST <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/5d7e3e81-f0c8-48ea-b60e-153703df8862>`_
	* `GRID::NE1::PROD_RFC1918 <https://pes-ui.corp.yahoo.com/pes/domain/hadoop/workloadgroup/9a20b658-2199-3aed-a4d5-a31a2f408b71>`_



Services Required to Run the application
----------------------------------------

* Namenode

	 * Active
	 * Standby

* Balancer
* Datanode
* KMS
* Zookeeper
* NFS


Expected Traffic and Load
-------------------------

* Peak
* Average
* Low Peak


Tools Used to manage the Service
--------------------------------

ABFT
^^^^

* How ABFT operates for this service,
* what it does and does not do.

Service Controller
^^^^^^^^^^^^^^^^^^

* How its integrated,
* how to start and stop services

Patch Management
^^^^^^^^^^^^^^^^

YAMAS
^^^^^^

More information in the Logging, Monitoring and Alerting section below

Doppler
^^^^^^^^

* Service Customers - who are they?
* How are users onboarded & how do they request capacity
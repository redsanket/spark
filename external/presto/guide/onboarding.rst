Onboarding
##########

::

        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        * DISCLAIMER: We are currently not onboarding new users/projects onto Presto except in VCG. *
        * Onboarding will be available for YGRID starting sometimes in Mar 2020.                    *
        * This information is for planning purposes only.                                           * 
        * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
        
Deployment Model
****************

We run Presto in a separate cluster that can access data in any of the regular compute clusters within the same colo.

Our plan is to have two multitenant clusters in each colo: one for Staging, and one for Production.

The **Staging** cluster is intended for use case evaluation, testing, and resource estimation needed for production onboarding.

The **Production** cluster will have a dedicated queue for each onboarded project.

Presto Clusters
***************

The list of currently available clusters is :doc:`here <deployment>`.

Onboarding Process
******************

Onboarding will be done via `yo/doppler <https://yo/doppler>`_. The steps to the process are

-  User determining if Presto is the solution for their use case by running experiments
   on staging cluster and discussing with Presto team
-  Determine capacity needs by running representative queries with desired concurrency on the cluster.
-  Procure hardware via the `CAR <http://yo/carwash>`_ process.
-  Create a Doppler project for Presto and raise a request for a Resource Group (queues in Hadoop terminology)
   in the Environment (Presto Cluster) you want.
-  Once the hardware is received and configured, a dedicated queue in the production cluster will be created.


Capacity Estimation
===================

Currently the memory usage captured for a query by the Presto Coordinator is very inaccurate and on the lower
side due to sampling intervals being in seconds and query times being in milliseconds or seconds. Since
each query is run in separate thread and not in a jvm container, that poses more challenge as well
to capturing it accurately. Since increasing the sampling comes at a cost to the performance, we do not
plan to change this in the near future till we work on more performance fine tuning.

To circumvent the problem of inaccurate metrics, our solution is to run experiments on the staging cluster
of the same colo with fixed size resource groups to better determine the resources needed by the project.
The Presto node specification is 384GB RAM and we run the process with 200GB heap size.
Rest of the memory is utilized for Presto JVM non-heap memory (30-40GB), page cache, buffers, etc.
Currently the minimum size we provision for a resource group is 10 nodes.
The capacity of that would be 2TB (10*200GB = 2048 GB).

To estimate capacity:

- Run :doc:`Presto CLI from gateway or launcher <connectivity/cli>` with ``--client-tags`` option specifying name of the resource group.
  For example:

  .. code-block:: text

     presto --client-tags 10nodes
     
   
  Valid values for resource groups for each staging cluster are as below.

  +--------+------+-----------------+-----------------+-----------------+
  | Domain | Colo | Staging Cluster | Resource Groups | Max Concurrency |
  +========+======+=================+=================+=================+
  | YGRID  | gq1  | Yoda Blue       | 10nodes         | 20              |
  |        |      |                 |                 |                 |
  |        |      |                 | 20nodes         |                 |
  |        |      |                 |                 |                 |
  |        |      |                 | 30nodes         |                 |
  +--------+------+-----------------+-----------------+-----------------+
  | YGRID  | ne1  | Yoda Tan        | 10nodes         | 20              |
  |        |      |                 |                 |                 |
  |        |      |                 | 20nodes         |                 |
  +--------+------+-----------------+-----------------+-----------------+
  | YGRID  | bf1  | Yoda Red        | 10nodes         | 20              |
  |        |      |                 |                 |                 |
  |        |      |                 | 20nodes         |                 |
  +--------+------+-----------------+-----------------+-----------------+
  | VCG    | gq2  | Hoth GQ         | 10nodes         | 10              |
  |        |      |                 |                 |                 |
  |        |      |                 | 15nodes         | 15              |
  |        |      |                 |                 |                 |
  |        |      |                 | 20nodes         | 20              |
  |        |      |                 |                 |                 |
  |        |      |                 | 30nodes         | 20              |
  +--------+------+-----------------+-----------------+-----------------+

  Some staging clusters are small (40 nodes) and only have *10nodes* and *20nodes* resource groups
  with rest set aside for the *default* resource group.
  If you have larger capacity requirements than what is available to test,
  please file a support request in http://yo/prestosupport.

- Run some of your largest queries to check for performance.
  If data volumes are large and queries are complex, switch to a larger queue.

- Run a set of representative queries in parallel, to check for concurrency performance.
  The below example assumes that there is a ``~/capacitytest/samplequeries`` directory
  with ten representative queries to run.

  .. code-block:: text

   mkdir -p ~/capacitytest/output
   for querynum in {1..10};
   do export QUERY=`cat ~/capacitytest/samplequeries/$querynum.sql`;
   # Launch queries in background in parallel
   time presto --client-tags 10nodes --catalog kessel --debug --execute "$QUERY" --output-format TSV_HEADER > ~/capacitytest/$qnum.tsv &
   done


.. _doppler:

Doppler
=======

Steps:

1. Create the Doppler Project for Presto. This will be similar to the Hadoop project you created earlier.

   - Got to `Presto Projects <https://doppler.cloud.corp.yahoo.com:4443/doppler/presto>`_
     by clicking on ``Platforms`` and selecting ``Presto``
   - Click on ``Create a new project``.
   - Fill in the details and click on ``Submit New Project``. If you already have a corresponding
     `Hadoop project <https://doppler.cloud.corp.yahoo.com:4443/doppler/search/hadoop>`_,
     you can save time by copying over the details.

   .. image:: images/presto_new_project.png
      :height: 316px
      :width: 883px
      :scale: 80%
      :alt:
      :align: left


2. Once the project is created, add an ``Environment`` (Presto Cluster) to it, by clicking on
   ``New Environment``

   .. image:: images/presto_environments.png
      :height: 516px
      :width: 883px
      :scale: 80%
      :alt:
      :align: left

3. Fill in the details for the environment.

   - For the ``Resource Group ID`` field, prefix your project name with ``prod_``
     to indicate that it is a production usage queue. If you are provisioning for
     adhoc usage, prefix with ``adhoc_``
   - For the ``RAM Quota``, convert TB to GB. Currently the Presto node specification is
     384GB RAM and we run the process with 200GB heap size. If you procured 10 nodes, then
     the capacity would be 2TB (2048 GB).
   - For ``Max Concurrency``, keep the values between 10 (large queries) to 20 (smaller queries).
     Anything more than that will impact performance.
     You can set the ``Max Queued`` to a higher value like ``200`` to avoid queries being rejected.
   - For the ``Headless Users``, only add the project's headless users. Other headless users,
     can be added to the OpsDB Group used to control access to the resource group.

   .. image:: images/presto_new_environment.png
      :height: 516px
      :width: 883px
      :scale: 80%
      :alt:
      :align: left

4. You can edit the New Environment Request and add the ``Capital Allocation Request`` details.

   .. image:: images/presto_capital_allocation_request.png
      :height: 516px
      :width: 883px
      :scale: 80%
      :alt:
      :align: left

5. Once the request is approved and provisioned which may take time depending on the hardware situtation,
   you can give access to more regular or headless users by adding them to the resource group.

   -  Click on ``View in OpsDB``. This will take you to the OpsDB group to which you can add users.
   -  Members of the OpsDB group are actually synced to a LDAP Netgroup in the background.
      Presto checks against the Netgroup to see if a particular user has access to that resource group.
      So for a newly added user, give an hour for the sync to take effect and user be allowed to
      submit to that resource group.

   .. image:: images/presto_resource_group_view.png
      :height: 516px
      :width: 883px
      :scale: 80%
      :alt:
      :align: left

6. Doppler shows metrics on memory usage, concurrent queries and queued queries. This is usage sampled
   every second by Doppler. While the concurrent and queued queries are close approximations, the memory usage is usually
   way off and we request you to not rely on that. This is due to the fact that many Presto queries run in milliseconds/seconds
   and the capture interval of both Presto and Doppler is in seconds. While the number of queries
   is given accurately by Presto Coordinator at any time, the memory usage it gets from workers is
   very low due to the sampling intervals and the challenge of capturing usage at thread level.
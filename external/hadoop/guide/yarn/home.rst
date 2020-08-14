********
Overview
********

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------


The fundamental idea of YARN is to split up the functionalities of resource management and job scheduling/monitoring into separate daemons. The idea is to have a global :term:`ResourceManager` (RM) and per-application `ApplicationMaster (AM)`. An application is either a single job or a DAG of jobs.


.. topic:: Definitions
   :class: definitionbox

   .. glossary::

     ResourceManager
       Is the ultimate authority that arbitrates resources among all the
       applications in the system. It has two main components: `Scheduler` and
       `ApplicationsManager`. The `ResourceManager` and the :term:`NodeManager`
       form the data-computation framework.
     
     NodeManager
       Is the per-machine framework agent who is responsible for containers,
       monitoring their resource usage (cpu, memory, disk, network) and
       reporting the same to the ResourceManager/Scheduler. See
       :numref:`yarn-nm` for more details.

     Scheduler
       Is responsible for allocating resources to the various running
       applications subject to familiar constraints of capacities, queues etc.
       The Scheduler is pure scheduler in the sense that it performs no
       monitoring or tracking of status for the application. Also, it offers
       *no guarantees* about restarting failed tasks either due to application
       failure or hardware failures. |br|
       The Scheduler performs its scheduling
       function based on the resource requirements of the applications; it does
       so based on the abstract notion of a resource Container which
       incorporates elements such as memory, cpu, disk, network etc. The
       Scheduler has a pluggable policy which is responsible for partitioning
       the cluster resources among the various queues, applications etc (See
       :numref:`yarn_scheduling`). |br|
       The current schedulers such as the `CapacityScheduler` and the
       `FairScheduler` would be some examples of plug-ins.

     ApplicationsManager
       Is responsible for accepting job-submissions, negotiating the first
       container for executing the application specific `ApplicationMaster` and
       provides the service for restarting the `ApplicationMaster` container on
       failure. |br|
       The per-application `ApplicationMaster` has the responsibility
       of negotiating appropriate resource containers from the Scheduler,
       tracking their status and monitoring for progress.


.. _yarn-commands:

YARN Commands
=============

YARN commands are invoked by the bin/yarn script. Running the yarn script
without any arguments prints the description for all commands.

  .. code-block:: bash

    yarn [--config confdir] COMMAND [--loglevel loglevel] \
         [GENERIC_OPTIONS] [COMMAND_OPTIONS]

  .. table:: `YARN has an option parsing framework that employs parsing generic options as well as running classes.`
    :widths: auto
    :name: table-yarn-command-options

    +---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |             Options             |                                                                                               Description                                                                                               |
    +=================================+=========================================================================================================================================================================================================+
    | ``--config`` `confdir`          | Overwrites the default Configuration directory. Default is ``${HADOOP_PREFIX}/conf``.                                                                                                                   |
    +---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``--loglevel`` `loglevel`       | Overwrites the log level. Valid log levels are `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, and `TRACE`. Default is `INFO`.                                                                               |
    +---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``GENERIC_OPTIONS``             | The common set of options supported by multiple commands. See the :hadoop_rel_doc:`Hadoop Commands Manual<hadoop-project-dist/hadoop-common/CommandsManual.html#Generic_Options>` for more information. |
    +---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``COMMAND`` ``COMMAND_OPTIONS`` | Various commands with their options are described below                                                                                                                                                 |
    +---------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

:token:`Application commands`

  .. code-block:: bash

    yarn application [options]

  .. table:: `YARN User Commands: application options.`
    :widths: auto
    :name: table-yarn-command-application-options

    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |              Options              |                                                                                                                           Description                                                                                                                           |
    +===================================+=================================================================================================================================================================================================================================================================+
    | ``-appId`` <ApplicationId>        | Specify Application Id to be operated                                                                                                                                                                                                                           |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-appStates`` <States>           | Works with ``-list`` to filter applications based on input comma-separated list of application states. The valid application state can be one of the following:  `ALL`, `NEW`, `NEW_SAVING`, `SUBMITTED`, `ACCEPTED`, `RUNNING`, `FINISHED`, `FAILED`, `KILLED` |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-appTypes`` <Types>             | Works with ``-list`` to filter applications based on input comma-separated list of application types.                                                                                                                                                           |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-help``                         | Help                                                                                                                                                                                                                                                            |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-list``                         | Lists applications from the RM. Supports optional use of ``-appTypes`` to filter applications based on application type, and ``-appStates`` to filter applications based on application state.                                                                  |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-kill`` <ApplicationId>         | Kills the application.                                                                                                                                                                                                                                          |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-movetoqueue`` <Application Id> | Moves the application to a different queue.                                                                                                                                                                                                                     |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-queue`` <Queue Name>           | Works with the movetoqueue command to specify which queue to move an application to.                                                                                                                                                                            |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-status`` <ApplicationId>       | Prints the status of the application.                                                                                                                                                                                                                           |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-updateLifetime`` <Timeout>     | Update application timeout (from the time of request) in seconds. `ApplicationId` can be specified using `appId` option.                                                                                                                                        |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``-updatePriority`` <Priority>    | Update priority of an application. `ApplicationId` can be passed using `appId` option.                                                                                                                                                                          |
    +-----------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

:token:`Application Attempt`

  .. code-block:: bash

    yarn applicationattempt [options]

  .. table:: `YARN User Commands: yarn applicationattempt options.`
    :widths: auto
    :name: table-yarn-command-appattempt-options

    +---------------------------+--------------------------------------------------------+
    |          Options          |                       Description                      |
    +===========================+========================================================+
    | ``-help``                 | Help                                                   |
    +---------------------------+--------------------------------------------------------+
    | ``-list`` <ApplicationId> | Lists applications attempts for the given application. |
    +---------------------------+--------------------------------------------------------+
    | ``-status`` <AttemptId>   | Prints the status of the application attempt.          |
    +---------------------------+--------------------------------------------------------+

:token:`Yarn Container`

  .. code-block:: bash

    yarn container [options]

  .. table:: `YARN User Commands: yarn container options.`
    :widths: auto
    :name: table-yarn-command-container-options

    +----------------------------+----------------------------------------------+
    |           Options          |                  Description                 |
    +============================+==============================================+
    | ``-help``                  | Help                                         |
    +----------------------------+----------------------------------------------+
    | ``-list`` <App Attempt Id> | Lists containers for the application attempt |
    +----------------------------+----------------------------------------------+
    | ``-status`` <ContainerId>  | Prints the status of the container           |
    +----------------------------+----------------------------------------------+

:token:`Node`

  .. code-block:: bash

    yarn node [options]

  .. table:: `YARN User Commands: yarn node options.`
    :widths: auto
    :name: table-yarn-command-node-options

    +----------------------+------------------------------------------------------------------------------------------------------------------------------------+
    |        Options       |                                                             Description                                                            |
    +======================+====================================================================================================================================+
    | ``-all``             | Works with ``-list`` to list all nodes.                                                                                            |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------+
    | ``-list``            | Lists all running nodes. Supports optional use of ``-states`` to filter nodes based on node state, and ``-all`` to list all nodes. |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------+
    | ``-states`` <States> | Works with ``-list`` to filter nodes based on input comma-separated list of node states.                                           |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------+
    | ``-status`` <NodeId> | Prints the status report of the node.                                                                                              |
    +----------------------+------------------------------------------------------------------------------------------------------------------------------------+

:token:`logs`

  .. include:: /common/yarn/yarn-commands-logs.rst


.. _yarn-nm:

NodeManager
===========

NodeManager is responsible for launching and managing containers on a node.
Containers execute tasks as specified by the `AppMaster`.

The NodeManager runs services to determine the health of the node it is
executing on. The services perform checks on the disk as well as any user
specified tests. If any health check fails, the :term:`NodeManager` marks the
node as unhealthy and communicates this to the :term:`ResourceManager`, which
then stops assigning containers to the node. Communication of the node status is
done as part of the `heartbeat` between the :term:`NodeManager` and the
:term:`ResourceManager`. |br|
The intervals at which the disk checker and health
monitor run don’t affect the `heartbeat` intervals. When the heartbeat takes
place, the status of both checks is used to determine the health of the node.

Disk Checker
------------
   
The disk checker checks the state of the disks that the :term:`NodeManager` is
configured to use(`local-dirs` and `log-dirs`, configured using
``yarn.nodemanager.local-dirs`` and ``yarn.nodemanager.log-dirs`` respectively).
|br| The checks include permissions and free disk space. It also checks that the
filesystem isn’t in a read-only state. The checks are run at 2 minute intervals
by default but can be configured to run as often as the user desires. 

If a disk fails the check, the :term:`NodeManager` stops using that particular
disk but still reports the node status as healthy. However if a number of disks
fail the check, then the node is reported as unhealthy to the
:term:`ResourceManager` and new containers will not be assigned to the node.

  .. include:: /common/yarn/yarn-nm-disk-checker-conf.rst

.. _yarn-nm-health-scripts:

Health Script
-------------

A custom Health check can be submitted by the Health checker service.

  .. include:: /common/yarn/yarn-nm-health-conf.rst



Resources
=========

.. include:: /common/yarn/yarn-reading-resources.rst

.. admonition:: Related...
   :class: readingbox

   Check the FAQ section in :ref:`mapreduce_compression_faq`

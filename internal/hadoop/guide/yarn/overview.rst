..  _yarn_overview:

*********
Overview
*********

The fundamental idea of YARN is to split up the functionalities of resource management and job
scheduling/monitoring into separate daemons. The idea is to have a global ResourceManager (RM)
and per-application ApplicationMaster (AM). An application is either a single job or a DAG of jobs.

The ResourceManager and the NodeManager form the data-computation framework. The ResourceManager is
the ultimate authority that arbitrates resources among all the applications in the system.
The NodeManager is the per-machine framework agent who is responsible for containers, monitoring
their resource usage (cpu, memory, disk, network) and reporting the same to the ResourceManager/Scheduler.

The per-application ApplicationMaster is, in effect, a framework specific library and is tasked
with negotiating resources from the ResourceManager and working with the NodeManager(s) to execute
and monitor the tasks.

..  _yarn_runbook_memory:


Monitoring the Memory of Your Container |nbsp| |green-badge|
============================================================


.. admonition:: See also...
   :class: readingbox

    * :yahoo_grid_guide:`Grid user guide - Memory Running on Yarn <yarn/troubleshooting/part-02.html#memory-running-on-yarn>`
    * Increasing Memory for Job (:yahoo_grid_guide:`Grid user guide - How to give your tasks more memory <mapreduce/faq/runtime/runtime-qa-part-02.html#mapreduce-faq-runtime-increase-job-memory>`)

NodeManager periodically (every 3 seconds by default, which can be changed via ``yarn.nodemanager.container-monitor.interval-ms``) cycles through all the currently running containers, calculates the process tree (all child processes for each container), and for each process examines the ``/proc/<PID>/stat`` file (where `PID` is the process ID of the container) and extracts the physical memory (aka `RSS`) and the virtual memory (aka `VSZ` or `VSIZE`).

If virtual memory checking is enabled (configured via ``yarn.nodemanager.vmem-check-enabled``), then YARN compares the summed `VSIZE` extracted from the container process (and all child processes) with the maximum allowed virtual memory for the container. The maximum allowed virtual memory is basically the configured maximum physical memory for the container multiplied by ``yarn.nodemanager.vmem-pmem-ratio``. For example if `vmem-pmem-ratio` is set to `2.1` and your YARN container is configured to have a maximum of 2 GB of physical memory, then this number is multiplied by `2.1` which means you are allowed to use `4.2GB` of virtual memory.

If physical memory checking is enabled (`true` by default, overridden via ``yarn.nodemanager.pmem-check-enabled``), then YARN compares the summed RSS extracted from the container process (and all child processes) with the maximum allowed physical memory for the container.

If either the virtual or physical utilization is higher than the maximum permitted, YARN will kill the container, as shown in the error
:yahoo_grid_guide:`yarn trouble shooting section <yarn/troubleshooting/part-02.html#memory-running-on-yarn>`.


  .. table:: `YARN NodeManager Configurations to Monitor Container`
    :widths: auto
    :name: table-yarn-runbook-memory


    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |                           Property                           |                                                                                                                Description                                                                                                               |
    +==============================================================+==========================================================================================================================================================================================================================================+
    | ``yarn.nodemanager`` |br| ``.container-monitor.enabled``     | Enable container monitor                                                                                                                                                                                                                 |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.container-monitor.interval-ms`` | How often to monitor containers. If not set, the value for `yarn.nodemanager.resource-monitor.interval-ms` will be used. If 0 or negative, container monitoring is disabled.                                                             |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.vmem-check-enabled``            | Whether virtual memory limits will be enforced for containers.                                                                                                                                                                           |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.vmem-pmem-ratio``               | Ratio between virtual memory to physical memory when setting memory limits for containers. Container allocations are expressed in terms of physical memory, and virtual memory usage is allowed to exceed this allocation by this ratio. |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | ``yarn.nodemanager`` |br| ``.pmem-check-enabled``            | Whether physical memory limits will be enforced for containers.                                                                                                                                                                          |
    +--------------------------------------------------------------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
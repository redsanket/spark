..  _yarn_runbook_queue-operations:

Deleting, Renaming, and Moving Queues |nbsp| |green-badge|
==========================================================

In Hadoop 2.10, you can `rename`, `delete`, or `move` a queue *without*
restarting the resource manager.


Each of these procedures require first putting the specified queue in the
``STOPPED`` state:

- Edit the `capacity-scheduler` xml file that contains the queue configs for the
  specified queue and change the value of
  ``yarn.scheduler.capacity.<QUEUEPATH>.state`` to ``STOPPED``.

  .. code-block:: bash
  
     yarn rmadmin -refreshQueues
     
- Wait for the queue to drain.
  
  - While the queue is draining, it will be in the ``DRAINING`` state in the UI.
  - Once the queue is drained, it will be in the ``STOPPED`` state in the UI.


Rename
------

- Put the queue in the ``STOPPPED`` state (see above).
- In the `capacity-scheduler` xml file that contains the queue configs for the
  specified queue, rename every instance of the queue to the new name.
- Refresh the capacity scheduler queues:
  
  .. code-block:: bash
  
     yarn rmadmin -refreshQueues


Delete
------

- Put the queue in the ``STOPPPED`` state (see above).
- In the `capacity-scheduler` xml file that contains the queue configs for the
  specified queue, delete all properties with ``<QUEUEPATH>`` in the name.
- Delete the queue name from the list in the
  ``yarn.scheduler.capacity.<PARENTQUEUEPATH>.queues`` property.
- Make sure that the sibling queues' capacities add up to 100%.
  
  - For example, if root has `a`, `b`, and `c` as children, and I want to
    remove `b`, I must make sure that the values for
    ``yarn.scheduler.capacity.root.a.capacity`` and
    ``yarn.scheduler.capacity.root.c.capacity`` add up to 100.

- Refresh the capacity scheduler queues:

  .. code-block:: bash
  
     yarn rmadmin -refreshQueues

Move
----

- Put the queue in the ``STOPPPED`` state (see above).
- Delete the specified queue as described above.
- Add the specified queue to the new hierarchy as you would for any new queue.
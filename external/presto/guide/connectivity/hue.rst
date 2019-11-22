Hue
###

You can login to the `Hue instance <https://git.ouroath.com/pages/hadoop/docs/hue/reference/index.html>`_
for a Hadoop cluster and run Presto queries from there.

Below steps show how to run queries on Xandar Blue Presto cluster from Jet Blue Hue.

1. Go to `yo/hue.jb <https://yo/hue.jb>`_ in your browser.

2. Click on ``Query -> Editor -> Presto XandarBlue`` to chose the Presto Editor

  .. image:: images/hue_editor_presto.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

3. Ignore ``An unknown error occurred`` popup or ``Error loading databases`` message on
the ``Databases`` panel on the left. This is a known bug which happens the first time.

4. Run a dummy query like ``show tables`` or ``show schemas``. \
NOTE: Queries in CLI must end with ``;``, but queries using JDBC driver (like Hue) must _not_ end with ``;``.

5. Click on the refresh button on the ``Databases`` panel to load the list of databases.
You can then select a database to set that as the default database and proceed
with actual queries.

  .. image:: images/hue_select_database.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left

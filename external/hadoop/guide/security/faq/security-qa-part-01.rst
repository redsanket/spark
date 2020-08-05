Kerberos Tickets and Passwords
==============================

**Q**: I have a long running job that involves ssh to a gateway to submit jobs hundreds of times over the course of several days. It is not a recurring job so headless account is inappropriate. How do I setup Kerberos tickets so that I don't have to type password for each ssh?

**Ans**: Get a ticket before you start. Save the ticket in a file you choose.
Modify your scripts to run`` kinit -R -c <your_ticket_file>`` and set the environment variable ``KRB5CCNAME`` to your ticket file for every ssh session. This way you can run your job as long as it completes within 7 days (tickets have a maximum life of 7 days) and you run at least one ssh for every 10 hours (tickets expire after 10 hours and become not renewable). For example, first time you run

  .. code-block:: bash

    % kinit -c krb_tck_session_12345 @Y.CORP.YAHOO.COM
    <enter password>
    % klist -c krb_tck_session_12345

Now everytime after you ssh to the gateway, you can do:

  .. code-block:: bash

    kinit -R -c krb_tck_session_12345; \
    export KRB5CCNAME=krb_tck_session_12345; \
    $HADOOP_HOME/bin/hadoop ...


.. note:: you must set the environment variable. Otherwise Hadoop doesn't know where to find the ticket and will fail.


HBase Blog on Secure Hadoop
===========================

.. todo:: Fixme link Up and running with Secure Hadoop 

`Up and running with Secure Hadoop <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com:8080/?url=http%3A%2F%2Fhbaseblog.com%2F2010%2F07%2F21%2Fup-and-running-with-secure-hadoop%2F&SIG=1264e1uua/>`_

FileSystem API and Hadoop 20s
=============================

.. todo:: Fixme move the "Topher's Guide to Hadoop 0.20s HDFS Access" to sphinx

For anybody who is trying to use the FileSystem API from a Gateway or Launcher with Hadoop 0.20s:

`Topher's Guide to Hadoop 0.20s HDFS Access <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Yst/TophersGuideTo20sHdfsAccess/>`_

This does NOT cover accessing FileSystem from inside a map-reduce job.


How to authenticate and run jobs across the Grid (Proxy)
========================================================

.. note:: this content has replaced the original legacy `Grid Invoke Web Request Via Proxy <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Sandbox/GridInvokeWebRequestViaProxy.html>`_


HTTP Proxy
----------

Grid `HttpProxy` allows user jobs/processes running on ygrid to access services outside of ygrid.
There are two types of HttpProxy:

* Internal HttpProxy allows access to internal services in `.yahoo.com domain`.
* External HttpProxy allows access to external services. Athenz authentication with External `HttpProxy` is required for accessing external services.

For more information see the `HTTP Proxy confluence page <https://confluence.vzbuilders.com/display/HPROX/HTTP+Proxy>`_



HDFS Proxy
----------

`HDFSProxy` provides authenticated encrypted access to a particular cluster's Hadoop file system (HDFS) from *outside a cluster*:
e.g. cross-colo, or when trying to access HDFS outside of the grid backplane. (Within the grid backplane, use HDFS).

For more information see the `HDFSProxy confluence page <https://confluence.vzbuilders.com/display/HPROX/HDFS+Proxy>`_


HTTPFS Proxy
------------

* `Httpfs Proxy Dev Guide <https://confluence.vzbuilders.com/display/HPROX/Httpfs+Proxy+Dev+Guide>`_
* `Configuring Oath HttpFS for CertificateBased Auth <https://docs.google.com/document/d/1mjLerhHZeiOLChNyP33yZDsCB6AC8X6geqLbjrlxi00>`_
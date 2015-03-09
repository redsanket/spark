Architecture
============

Overview
--------

The Yahoo implementation of Hue shown below is slightly different than
that of `Cloudera's Hue <http://cloudera.github.io/hue/docs-3.7.0/user-guide/introducing.html>`_. On the backend, we replace `Spawning <https://pypi.python.org/pypi/Spawning>`_ 
and `CherryPy <http://cherrypy.org/>`_ with `yApache <http://developer.corp.yahoo.com/product/yApache>`_, 
and use `ATS <http://developer.corp.yahoo.com/product/ATS>`_ for proxying requests. 

Our Hue frontend is the same, and as of yet, we do not offer the SDK to create your own
Hue applications.

.. image:: images/hue_arch.png
   :height: 686 px 
   :width: 850 px
   :scale: 95 %
   :alt: Diagram of the Yahoo Hue Architecture 
   :align: left


Tiers of Hue
------------

HTTP based and stateless (uses async queries)

Frontend and backend separation (e.g. different servers, pagination)

Resources (e.g. img, js, callbacks, css, json)

Support multiple browsers and technologies

Supports multiple DB backend (SQLite, MySQL)

Supports i18n


.. image:: images/hue_arch_levels.jpg
   :height: 686 px 
   :width: 850 px
   :scale: 95 %
   :alt: Diagram of the different tiers for the Yahoo Hue Architecture 
   :align: left


Deployment
----------

.. image:: images/deployment_arch.jpg   
   :height: 462 px 
   :width: 850 px
   :scale: 95 %
   :alt: Diagram of the Hue deployment architecture
   :align: left


Hue Applications and Their Dependencies
#######################################

.. csv-table:: Frozen Delights!
   :header: "Applications", "Dependencies"
   :widths: 15, 10, 30

   "Query Editor->Pig", "Oozie server to submit Pig scripts"
   "Query Editor->Hive", "HiveServer2 to submit Hive queries"
   "Query Editors->Job Designer", "Oozie access through REST APIs"
   "Data Browsers->Metastore Tables", "HiveServer2 to access HCatalog metastore"
   "Workflows->Dashboards", "Oozie access through REST APIs"
   "Workflows->Editors", "Oozie access through REST APIs"
   "File Browser", "WebHDFS or HttpFS (similar to HDFSProxy)"
   "Job Browser", "ResourceManager (RM) access through ``hue-plugins``"



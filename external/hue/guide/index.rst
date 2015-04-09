==============
Hue User Guide 
==============

-----------------
Developer Preview
-----------------

.. important:: This is a preliminary document intended for developers 
               working at Yahoo. If you plan to use the contents of 
               this document to implement applications on the grid, please note 
               that the Hue UI may be subject to change and modification. 
               When the final documentation is released, you will be notified 
               of all changes and updates via yahoo-hue-users@yahoo-inc.com.

.. toctree::
   :maxdepth: 2
   :hidden:

   overview/index
   getting_started/index
   ui/index
   howtos/index 
   architecture/index
   reference/index


This document is **Yahoo Proprietary/Confidential**. Do not release the contents of this document outside the company.

About This Guide
================

The goal of this guide is to help developers use a custom version of Hue for Yahoo. 

Keep in the mind that Cloudera version of Hue has some different features, and that some similar features described in the `Hue User Guide <http://cloudera.github.io/hue/docs-3.7.0/user-guide/index.html>`_ behave differently in Yahoo's Hue UI.

.. _onboard:

Accessing Hue
=============

Each Grid cluster has a dedicated Hue instance, which do not share information among themselves in any way. You have access to the Hue instance on any cluster, but you can only access and modify data that you have access permission.

.. _using_socks_proxy:

Using SOCKS Proxy
-----------------

To access Hue instances, you are required to use the SOCKS proxy in the browser.
The SOCKS host is ``socks.yahoo.com`` and the port is ``1080``, which will allow
the Hadoop Job Browser to access the ResourceManager (RM) jobs logs.

Hue URLs
--------

To access the Hue Web UI of a particular instance, use an URL with the 
following syntax: ``https://<cluster_name>-hue.<colo_name>.ygrid.yahoo.com:<port>``

For example, to access the Hue UI on Cobalt Blue, you would go to
``https://cobaltblue-hue.blue.ygrid.yahoo.com:9999/``.

You can also use ``yo`` links using the following syntax: ``http://yo/hue.<cluster name><color>``
For example, to access the Hue instance for the Cobalt Blue Hadoop, you would use
the ``yo`` link ``http://yo/hue.cb``.

Getting Help
============

General Questions / Support
---------------------------

- yahoo-hue-users@yahoo-inc.com 

Filing Jira Tickets
-------------------

#. View `existing Hue issues <https://jira.corp.yahoo.com/browse/GRIDUI-696?jql=component%20%3D%20Hue%20AND%20project%20%3D%20GRIDUI>`_ 
   to confirm that the issue you want to report has not been filed.
#. If your issue has already been filed, watch the issue to track its progress.
#. To report new issues, fill out the `Hue Defect <https://jira.corp.yahoo.com/servicedesk/customer/portal/112/create/1913>>`_ form. 
#. For new features, fill out the `Hue Feature Request <https://jira.corp.yahoo.com/servicedesk/customer/portal/112/create/1914>`_ form.


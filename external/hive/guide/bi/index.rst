======================================
Business Intelligence (BI) / Analytics
======================================

.. _bi-overview:

Overview
========

You can use business intelligence (BI) and data analytics tools such as
`Tableau <http://www.tableausoftware.com/>`_ and `MicroStrategy <https://www.microstrategy.com/us/>`_ 
with data on Hadoop. These tools access grid data with Hive through
the `Hortonworks Hive ODBC driver <http://hortonworks.com/wp-content/uploads/2013/04/Hortonworks-Hive-ODBC-Driver-User-Guide.pdf>`_. 

For those who are not familiar with Tableau and MicroStrategy, please read the next section that
discusses the differences and strengths of each so you can use the tool that best meets your needs.
If you know which tool is right for you, continue to :ref:`Visualizing Data and Ad Hoc Reporting With Tableau <getting_started-tableau>` or :ref:`Standard Reporting With MicroStrategy <gs_bi-ms>`.

.. _bi_overview-components:

Grid Stack for BI
-----------------


The diagram below shows how client software such as MicroStrategy and Tableau use the
ODBC driver to communicate with HiveServer2, which in turn, forwards queries to Hive
that ultimately are executed as MapReduce functions on Hadoop. 

.. image:: images/bi_grid.jpg
       :height: 516 px
       :width: 739 px
       :scale: 100%
       :alt:  BI on the Grid
       :align: left

Grid Environments for BI
------------------------

In the instructions for MicroStrategy and Tableau in the sections below,
we'll be using the Kryptonite Red cluster. 
Although we cover the details in the instructions below, you should
know the URI and port to the HiveServer 2 as well as the principal
on Krytponite Red for MicroStrategy and Tableau:


- **Host:**  ``http://kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``
- **Port:** 50514
- **Principal:** ``hive/kryptonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com`` 


To use Hive Server 2 with Tez, you need to use the Axonite Red (AR) cluster.
Note that the host, port, and principal for Axonite Red are different for
Tableau and MicroStrategy.

**MicroStrategy**

- **Host:**  ``axonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com``
- **Port:** 50515
- **Principal:** ``hive/axonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com`` 

**Tableau**

- **Host:**  ``axonitered-hs2.ygrid.vip.bf1.yahoo.com``
- **Port:** 50514
- **Principal:** ``hive/axonitered-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM`` 


For other grid environments, see :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>`.

.. _bi-tableau_ms:

MicroStrategy Versus Tableau 
============================

The general rule is to use MicroStrategy for standardized reporting
or Tableau for visualizing data. The decision to use one over the 
other isn't quite that simple, but we hope you feel confident about making a decision after 
reading the strengths and weaknesses (from user feedback) of each listed below.

.. _tableau_ms-ms:

MicroStrategy
-------------

**Strengths**

- Great for reporting that has stabilized (not changing much over time).
- Drill-down/through is great for facilitating an interactive discussion with business leaders.
- Report-sharing helps keep the data on the back-end, reducing the emailing of large Excel files.
- Allows for programmatic usage through a Web service API.
- Supports standard reporting.
- Provides access through a Web interface.
- Developer suite for generating reports.
- The license is inexpensive, and Yahoo has six thousand developer licenses.

**Weaknesses**

- Requires greater technical knowledge, and the process of 
  creating a new view is clumsy and time-consuming.
- Provides no capability of combining grid data with data outside the grid.
- Learning to use MicroStrategy is more difficult.

.. _tableau_ms-tableau:

Tableau 
-------

**Strengths**

- Lets you quickly make sense of millions of rows of data through visualization. 
- Does an excellent job of recognizing facts and dimensions in de-normalized data files 
  (say CSV or Excel) as well as connecting to larger databases. 
- The learning curve is low, especially if you are comfortable 
  with Excel Pivot Tables or something similar.
- Allows for ad hoc reporting.


**Weaknesses**

- For broader enterprise needs, such as data security and segmentation, heavy duty 
  report customization, or data transformation, Tableau is a less comprehensive tool than 
  MicroStrategy. 
- The license for using Tableau is expensive.
- You can only use Tableau through the desktop application.


.. _bi-ms:

Standard Reporting With MicroStrategy
=====================================

**Audience:** Developers, Administrators, and Architects

The following shows you how to start using the MicroStrategy to access Yahoo grid data, either through
the Web interface or the `MicroStrategy Analytics Desktop <https://www.microstrategy.com/us/free/desktop>`_. 
Most people use the Web interface because it's accessible from any OS and no software 
installation is required. **Administrators** and **architects**, however, will need to use the 
MicroStrategy Analytics Desktop, which can be run locally or remotely.

The Web interface requires no installation and can be accessed from any OS. The
desktop software has more features, but requires `Windows 7 <http://windows.microsoft.com/en-us/windows7/products/system-requirements>`_ 
(Yahoo corporate Win7 64-bit image), installation of software, and making sure that the desktop 
version matches the version of the MicroStrategy Server.

.. _bi_ms-req_se:

Requesting a Service Engineer (SE) to Set Up the MicroStrategy Server
---------------------------------------------------------------------

Write to mstr-admin@verizonmedia.com to get the MicroStrategy Server license and software.
The MicroStrategy Admininstration team will ask you for information about your request and 
then give you instructions as to the next steps after they have set up the MicroStrategy 
Server.

.. 

   - ACL to access data set on grid:
     - host
     - physical location
     - port 
   - Provide username/password
   - MicroStrategy Client Format Web interface, local desktop application, or remote desktop application
   - Specify the database that you want to access
   - (Optional) Specify the queue (cost center) used for your team.

.. _bi_ms-use:


.. TBD: Thiruvel is going to send me a link to the ODBC driver, review the documentation.
   
..

   The process of setting up the MicroStrategy Server is fairly complicated, so we 
   recommend working with a SE to help you. Once the MicroStrategy
   Server has been set up, the service engineer will provide you with a URI to the Web
   interface or for MicroStrategy Analytics Desktop to use.

..

   After your request is processed, the SEs will send you one of the following based
   on the format that you wanted:

   - URL to the MicroStrategy Web interface or the remote console
   - Link to the binary of the MicroStrategy Analytics Desktop to install locally. The installation
   comes with a script that will help you configure the application. 

   To learn how to use MicroStrategy, see the `MicroStrategy Analytics Desktop: User Guide <http://www.microstrategy.com/Strategy/media/downloads/free/analytics-desktop_user-guide.pdf>`_
   and the `MicroStrategy Suite: Quick Start Guide <https://www.microstrategy.com/Strategy/media/downloads/free/MicroStrategy-Suite-Quick-Start-Guide.pdf>`_.


.. _bi-setup_server:

Setting Up the MicroStrategy Server
===================================

**Audience:** Service Engineers

This following sections provide a general guideline for service engineers (SE) setting up 
MicroStrategy Server for users. 

We'll be going over the following:

- :ref:`Requirements <ms_setup_server-reqs>`
- :ref:`Get a Paranoid Review <ms_setup-paranoid>`
- :ref:`Configure Kerberos <gs_ms-setup-kerberos>`
- :ref:`Install/Configure the ODBC Driver <gs_ms-setup-odbc>`
- :ref:`Request Access to the Grid Cluster and HiveServer2 <gs_ms-setup-access>`
- :ref:`Install/Configure the MicroStrategy Server <gs_ms-install>`
 
.. note:: When you decide to use a different grid, you will again need to set up a 
          MicroStrategy Server that runs in the same colo has the grid instance.

.. _ms_setup_server-reqs: 

Requirements
------------

- Red Hat Enterprise Linux (RHEL) 6.4 or greater (64-bit) in the same colo.

.. _ms_setup-paranoid:

Get a Paranoid Review
---------------------

All data sets that will be accessed must be reviewed by a Paranoid.

To request a paranoid review:

#. Go to `http://yo/securityHelpdesk <http://yo/securityHelpdesk>`_.
   for details.
#. From the **Security Request** page, select **Service->Project Support** and **Project Type->Product/Platform**.
#. Enter a project ID and a summary.
#. Click **Send to Bugzilla**. 

.. _gs_ms-setup-kerberos:

Configure Kerberos
------------------
 
Users working with MicroStrategy from the Web interface or the desktop software 
authenticate with their username and password. The MicroStrategy Server, however,
requires  Kerberos to communicate with HiveServer2. To do this, a keytab representing
a UNIX headless user must be set up. The headless user will act as a proxy,
so that the MicroStrategy Server can then act on the user's behalf to access 
Hive through the HiveServer2.

#. You need to ensure that the Kerberos settings on the MicroStrategy Server are 
   identical to the settings on the requested cluster. Ideally, copy ``/etc/krb5.conf`` from 
   the appropriate cluster’s gateway to the MicroStrategy Server host.

#. Create a keytab for the Kerberos principal to be used by the MicroStrategy Server. 
   (File a ticket with SE on the ``BZ`` colo, if required.) 

#. Copy the keytab file to MicroStrategy Server host, and make it readable (only) 
   by the Unix-account that MicroStrategy Server will use.

#. Run ``kinit -kt <path_to_keytab> <MicroStrategy Server_principal>`` before connecting to 
   HiveServer2. If you want to keep the MicroStrategy Server running, you will need to run the
   command above in a cron job at least twice per day.

.. _gs_ms-install:

Install the ODBC Driver
-----------------------

Install the Hive ODBC Driver on the MicroStrategy Server using the instructions below.

#. Install the dependency ``cyrus-sasl``. (On a 32-bit machine one has to install 
   ``cyrus-sasl.i386`` with ``sudo yum install cyrus-sasl.i386``.)
#. Install the  yinst package for your environment. If your application is 32-bit, 
   you should install `hive_odbc_native_32 <http://dist.corp.yahoo.com/by-package/hive_odbc_native_32/>`_. 
   The driver for 64-bit is unavailable right now.
#. Add ``/home/y/lib/hive_odbc_native_32/Linux-i386-32`` (in case of 32-bit driver) to ``LD_LIBRARY_PATH``.
#. Once the driver is installed, you will need to configure it.

.. _ms_odbc-config:

Configure the ODBC Driver
#########################

#. In your home directly, create an ``.odbc.ini`` file with the following::

      [ODBC]
      - Specify any global ODBC configuration here such as ODBC tracing.
      
      [ODBC Data Sources]
      Sample Hive DSN 32=Hive ODBC Driver 32-bit
      
      [Sample Hive DSN 32]
      
      - Description: DSN Description.
      - This key is not necessary and is only to give a description of the data source.
      Description=Hive ODBC Driver (32-bit) DSN
      
      - Driver: The location where the ODBC driver is installed to.
      Driver=/home/y/lib/hive_odbc_native_32/Linux-i386-32/libhortonworkshiveodbc32.so
      
      - The DriverUnicodeEncoding setting is only used for SimbaDM
      - When set to 1, SimbaDM runs in UTF-16 mode.
      - When set to 2, SimbaDM runs in UTF-8 mode.
      -DriverUnicodeEncoding=2
      
      - Values for HOST, PORT, HS2HostFQDN, and HS2KrbServiceName should be set here.
      - They can also be specified on the connection string.
      - PORT information can be obtained from https://git.ouroath.com/GridSE/docs/wiki/Grid-Port-Numbers
      HOST=gsbl90047.blue.ygrid.yahoo.com
      PORT=50514
      Schema=default
      DefaultStringColumnLength=255
      RowsFetchedPerBlock=500
      FastSQLPrepare=0
      UseNativeQuery=0
      HiveServerType=2
      HS2AuthMech=1
      HS2HostFQDN=gsbl90047.blue.ygrid.yahoo.com
      HS2KrbServiceName=hive
      HS2KrbRealm=YGRID.YAHOO.COM

#. In your home directory, create a ``.hortonworks.hiveodbc.ini`` file with the content below.
   (Be sure to use the appropriate paths/host/principals for your environment.)

   .. code-block:: bash

      [Driver]

      -- - Note that this default DriverManagerEncoding of UTF-32 is for iODBC.
      -- - unixODBC uses UTF-16 by default.
      -- - If unixODBC was compiled with -DSQL_WCHART_CONVERT, then UTF-32 is the correct value.
      -- - SimbaDM can be used with UTF-8 or UTF-16.
      --   The DriverUnicodeEncoding setting will cause SimbaDM to run in UTF-8 when set to 2 or UTF-16 when set to 1.

      -- When using MicroStrategy, please set DriverManagerEncoding=UTF-8.
      -- Otherwise, string properties will not be reported correctly. (They will seem to be reported only as single-characters.)
      DriverManagerEncoding=UTF-8
      DSILogging=0
      ErrorMessagesPath=/home/y/lib/hive_odbc_native_32/hiveodbc/ErrorMessages

      -- - Uncomment the ODBCInstLib corresponding to the Driver Manager being used.
      -- - Note that the path to your ODBC Driver Manager must be specified in LD_LIBRARY_PATH (LIBPATH for AIX).
      -- - Note that AIX has a different format for specifying its shared libraries.

      - Generic ODBCInstLib
      -   iODBC
      -ODBCInstLib=libiodbcinst.so

      -   SimbaDM / unixODBC
      - We'll be using unixODBC. Thus, encoding above is also set to UTF-16
      ODBCInstLib=libodbcinst.so

      - AIX specific ODBCInstLib
      -   iODBC
      -ODBCInstLib=libiodbcinst.a(libiodbcinst.so.2)

      -   SimbaDM
      -ODBCInstLib=libodbcinst.a(odbcinst.so)
 
      -   unixODBC
      -ODBCInstLib=libodbcinst.a(libodbcinst.so.1)

   
#. Your drive should be installed and configured at this point. You'll need to install
   the ``unixODBC`` and connect to the HiveServer2 in the next sections.

   .. note:: Custom Configuration
         
             To use a custom configuration for ``.odbc.ini`` and ``.hortonworks.hiveodbc.ini`` 
             instead of using the files in the ``$HOME`` directory, the driver must provide 
             the following environment variables to override these:
         
             - **ODBCINI** - Use a custom ``odbc.ini`` file: ``isql`` will attempt to check for 
               write-access to ``$ODBCINI``, and hence, if you are testing with ``isql``, ensure the 
               file is in a writable location. Applications like MicroStrategy might not have 
               this limitation.
             - **SIMBAINI** - Use a custom ``hortonworks.hiveodbc.ini`` file.

.. _ms_odbc-install:

Install unixODBC
################

Before installing ``unixODBC`` with the instructions below, verify that the requested data 
sets (see :ref:`Get a Paranoid Review <ms_setup-paranoid>`) are accessible by unixODBC.

#. Download the `unixODBC source code <http://www.unixodbc.org/download.html>`_.
#. Untar the tarball and change to the directory created.
#. To build a 32-bit app, run the following: 

   ``$ CFLAGS="-m32 $CFLAGS" ./configure && make clean && make && sudo make install`` 

   For 64-bit unixODBC applications, remove the ``CFLAGS`` statement above as 64-bit 
   applications are built by default.

   .. note:: If ``gcc`` isn't installed, install it with ``sudo yum install gcc``.

#. Great, ``unixODBC`` is now installed, and all there is left to do is to use ``isql`` to 
   connect to HiveServer2.

.. _ms_odbc-hiveserver2:

Connect to HiveServer2
######################

#. Obtain a Kerberos ticket-granting ticket::

      $ kinit <username>@Y.CORP.YAHOO.COM
#. Use ``isql`` to execute Hive commands from the text file ``hive.sql``::

      $ isql -v "Sample Hive DSN 32" < hive.sql

.. note:: When connecting with MicroStrategy, do not use the ``mstrodbcadx`` command to 
          test the connection with HiveServer2. There seems to be a bug in ``mstrodbcadx`` 
          that replaces the driver path in the DSN definition (in ``odbc.ini``) with an 
          example path.



.. _gs_ms-setup-access:

Request Access to the Grid Cluster and HiveServer2
--------------------------------------------------

For existing headless accounts, you do not need to request access and can instead 
continue on to :ref:`Install/Configure MicroStrategy Server <gs_ms-install>`.

For new headless accounts, use the forms below to request access for both
the user account (headless account):

- http://supportshop.cloud.corp.yahoo.com/ydrupal/?q=grid-services-request (**User Account** tab)

If you are working outside of the ``ygrid`` network but in the same colo (most users),
you need the proper ACL settings to access port 50515 on HiveServer2 nodes on the 
cluster (e.g., on KryptoniteRed, HiveServer2 can be accessed through ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``).
In this case, you must file tickets to Grid SE to get access to HiveServer2 and 
Kerberos.

#. `File a Grid SE ticket <http://bug.corp.yahoo.com/enter_bug.cgi?product=kryptonite&component=Access>`_.
- See  `Bug -4387583 <http://bug.corp.yahoo.com/show_bug.cgi?id=4387583>`_ as an 
  example about how to request an update to a grid IP address.
- See `Siebel:1-1748453741 <http://eportal.corp.yahoo.com/ticket.php?srnumber=1-1748453741>`) 
  as an example of how to add a service (``YSS::BF1::GRIDCLIENT_LAUNCHER_PROD GRID::BF1::GRIDGW``).
- See `Bug -4148680&mark=15-c15 <http://bug.corp.yahoo.com/show_bug.cgi?id=4148680&mark=15-c15>`_ 
  as an example of how  to request access to Corporate KDC's (Destination macro: ``GRID::CRE1::CORP_KDC``).

.. note:: Because ACL changes are only pushed on certain days of the week, we
          require three to four days of lead time.)

.. _ms-install:

Install/Configure the MicroStrategy Server
------------------------------------------

- Request MicroStrategy Server from mstr-admin@verizonmedia.com. (You'll need to know what version to install).
- Modify the MicroStrategy Server ``odbc.ini`` to include the definition for the ODBC driver. 
  This entry should be as specified in the ``odbc.ini``.  Please use the respective 
  ``HOST`` names for the appropriate grid.

As this is just a sample, you will most likely need to modify the values given for the 
configurations below::

     [demo]
     Driver=/usr/lib/hive/lib/native/Linux-i386-32/libhortonworkshiveodbc32.so
     Description=DataDirect 7.1 Apache Hive Wire Protocol
     HOST=kryptonitered-hs2-noenc.ygrid.vip.bf1.yahoo.com
     PORT=50515
     Schema=my_super_duper_database
     ArraySize=16384
     DefaultLongDataBuffLen=1024
     EnableDescribeParam=0
     LoginTimeout=30
     LogonID=
     MaxVarcharSize=2147483647
     Password=
     RemoveColumnQualifiers=0
     StringDescribeType=12
     TransactionMode=0
     UseCurrentSchema=0
     HS2AuthMech=1
     HS2HostFQDN=kryptonitered-hs2.ygrid.vip.bf1.yahoo.com
     HS2KrbServiceName=hive
     HS2KrbRealm=YGRID.YAHOO.COM
     HiveServerType=2
- Run the following command in a cron job: ``kinit  -kt <path_to_keytab> <MicroStrategy Server_principal>``.
- Using the MicroStrategy Web interface and MicroStrategy Analytics Desktop, test that the 
  data sets are now accessible by the MicroStrategy Server.

.. _getting_started-tableau:

Visualizing Data and Ad Hoc Reporting With Tableau
==================================================

**Audience:** Developers, Administrators, and Architects


In this section, you'll be learning how to set up your system, install Tableau, and connect 
Tableau to Hive. What you won't be learning is how to use Tableau. See the 
`Tableau Quick Start Guides <http://www.tableausoftware.com/support/manuals/quickstart>`_
to learn how to use the software.

Before You Get Started
----------------------

Before you can use Tableau on either a Mac or Windows machine, you'll need access to HiveServer2 
on the Yahoo Grid. Complete the steps below to be a member of the privileged **hsuser** group,
which will allow you to use Hive.

#. In this tutorial, we'll be using the Kryptonite Red grid (KR), so the VIP URL that
   you'll be using is ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``. If you plan on using
   other grid VIPs, see :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>` to find
   the applicable URL and port.

#. Request authorization to HiveServer2 by clicking the **Hive Server 2** tab on the 
   `Grid Services Request Forms <http://supportshop.cloud.corp.yahoo.com/ydrupal/?q=grid-services-request>`_ 
   and following the instructions on the form shown below.
 
   .. image:: images/grid_services_req_form.jpg
      :height: 398 px
      :width: 800 px
      :scale: 95%
      :alt:  Grid Services Request Form
      :align: left
   

   .. note:: Submitting this request forms a contract between the individual and Yahoo. 

   The requesting user is required to read the document "Yahoo's Policy for Use of Tableau 
   Tool with Hadoop Services", which is available through the form. Special note should be 
   taken to the section on "Disciplinary Actions for Tableau Tool Violations" regarding 
   accessing PII data. Agreeing to the terms of usage on the form will create a Bugzilla 
   ticket that will track the status of your request.
   At this time HiveServer2 authorization will only be granted to select business units.

.. _tableau-env:

Grid Environments and Queues
############################

.. _tableau_env-mac:

Mac
***

When running Tableau on Mac, you can use Tableau to run queries to read data from
Hive tables on any Hadoop cluster, but you can only execute queries that write data
to clusters that have a ``default`` queue. For example, on Kryptonite Red, which we'll
be using in the tutorial, there is a ``default`` queue, so you can execute write statements
to a Hive table, but on Cobalt Blue, there is no ``default`` queue, so you're limited to
executing read queries to Hive tables. This is because we have not found a 
way to specify a queue name in the Mac version of Tableau. 

.. _tableau_env-windows:

Windows
*******

When using Tableau on Windows, you can specify the queue that you want to
use on any Hadoop cluster. This allows you to use Tableau to run queries to both read and
write data. We show you how to specify a queue in :ref:`Setting Up for Windows <tableau_setup-windows>`.

.. _tableau-setting_up:

I. Setting Up
-------------

.. _tableau_settingup-mac:

Mac
###


.. _tableau-mac_reqs:

Minimum Requirements
*********************

- OS X version 10.12, 10.13, or 10.14
- 2 GB memory
- 100 MB available disk space
- iODBC 3.52.9+

.. _tableau_windows-instructions:

Install and Configure the Simba Hive ODBC Driver
******************************************************

#. Download and install the `Simba Hive ODBC Driver for Mac OS X <https://drive.google.com/open?id=19gFskgkzaJrf21N4Wl7zB2gddShe1RU8>`_,
as per `Simba's documentation <https://www.simba.com/products/Hive/doc/ODBC_InstallGuide/mac/content/odbc/macosx/install.htm>`_, i.e.

    - Double-click ``SimbaHiveODBC.dmg`` to mount the disk image.
    - Double-click ``SimbaHiveODBC.pkg`` to run the installer.
    - In the installer, click ``Continue``.
    - On the ``Software License Agreement`` screen, click ``Continue``, and when the prompt appears, click Agree, then ``Install``.

The driver files will be installed under **/Library/simba/hive**.

#. Download the `Simba Hive ODBC Driver Mac License <https://drive.google.com/open?id=1hg9nHrB4FEmMQXtL_lY3yasYSsiAZ3zm>`_
and save it as **/Library/simba/hiveodbc//lib/SimbaApacheHiveODBCDriver.lic**. (You will need *root* privileges.)

The driver should now be ready for use.

#. Download and install iODBC. (Available from `iodbc.org <http://www.iodbc.org/dataspace/doc/iodbc/wiki/iodbcWiki/Downloads>`_, or
`on Google Drive <https://drive.google.com/open?id=1QexsK88FRzJgL86OPydPgo7z1CpK3Wmp>`_. This should allow the definition of ODBC
data sources that use the Simba ODBC driver.


.. _sample_krb5_config:

#. Create the file ``/etc/krb5.conf`` with the contents from the appropriate cluster gateway. For YGRID, it might look as follows::

    [libdefaults]
     default_realm = YGRID.YAHOO.COM
     dns_fallback = true
     dns_lookup_kdc = false
     dns_lookup_realm = true
     ticket_lifetime = 24h
     forwardable = true
     udp_preference_limit = 1
     renew_lifetime = 7d
     default_tgs_enctypes = aes256-cts
     default_tkt_enctypes = aes256-cts
     permitted_enctypes = aes256-cts aes128-cts arcfour-hmac-md5 des3-cbc-sha1

    [realms]
     YGRID.YAHOO.COM = {
      admin_server = krb-adm.ygrid.yahoo.com.:749
      kdc = krb-rr1.red.ygrid.yahoo.com.:88
      kdc = krb-rr2.red.ygrid.yahoo.com.:88
      kdc = krb-rr3.red.ygrid.yahoo.com.:88
      kdc = krb-rr4.red.ygrid.yahoo.com.:88
      auth_to_local = RULE:[1:$1@$0](.*@.*CORP.YAHOO.COM)s/@.*//
      auth_to_local = RULE:[1:$1@$0](.*@YGRID.YAHOO.COM)s/@.*//
      pkinit_kdc_hostname = ygrid-kdc.hadoop-prod.zts.yahoo.cloud
      pkinit_eku_checking = kpServerAuth
      pkinit_anchors = FILE:/opt/yahoo/share/ssl/certs/athenz_certificate_bundle.pem
     }


     Y.CORP.YAHOO.COM = {
      kdc = bf1-gdc01.corp.bf1.yahoo.com.:88
      kdc = bf1-gdc02.corp.bf1.yahoo.com.:88
      kdc = gq1-gdc01.corp.gq1.yahoo.com.:88
      kdc = gq1-gdc02.corp.gq1.yahoo.com.:88
      auth_to_local = RULE:[1:$1@$0](.*@.*CORP.YAHOO.COM)s/@.*//
     }

    [domain_realm]
      .ygrid.yahoo.com = YGRID.YAHOO.COM
      ygrid.yahoo.com = YGRID.YAHOO.COM
      ygrid.corp.sp1.yahoo.com = YGRID.YAHOO.COM
      .ygrid.corp.sp1.yahoo.com = YGRID.YAHOO.COM


    [capaths]
            YGRID.YAHOO.COM = {
              Y.CORP.YAHOO.COM = .
            }

    [appdefaults]
     pam = {
       debug=true
       forwardable=true
       krb4_convert=false
       cred_session=sshd
     }
#. Alternatively, on VCG, the ``/etc/krb5.conf`` looks as follows::

    [libdefaults]
     default_realm = VCG.OUROATH.COM
     dns_fallback = true
     dns_lookup_kdc = false
     dns_lookup_realm = true
     ticket_lifetime = 24h
     udp_preference_limit = 1
     renew_lifetime = 7d
     default_tgs_enctypes = aes256-cts

    [realms]
      VCG.OUROATH.COM = {
       kdc = krb-rr1.gq.vcg.ouroath.com.:88
       kdc = krb-rr2.gq.vcg.ouroath.com.:88
       admin_server = krb-adm.vcg.yahoo.com.:749
       auth_to_local = RULE:[1:$1@$0](.*@.*CORP.YAHOO.COM)s/@.*//
       auth_to_local = RULE:[1:$1@$0](.*@VCG.OUROATH.COM)s/@.*//
      }
      Y.CORP.YAHOO.COM = {
       kdc = gq1-gdc01.corp.gq1.yahoo.com.:88
       kdc = gq1-gdc02.corp.gq1.yahoo.com.:88
       kdc = bf1-gdc01.corp.bf1.yahoo.com.:88
       kdc = bf1-gdc02.corp.bf1.yahoo.com.:88
       auth_to_local = RULE:[1:$1@$0](.*@.*CORP.YAHOO.COM)s/@.*//
      }

    [domain_realm]
      .vcg.yahoo.com = VCG.OUROATH.COM
      vcg.yahoo.com = VCG.OUROATH.COM

    [capaths]
     VCG.OUROATH.COM =  {
        Y.CORP.YAHOO.COM = .
     }

#. Request a ticket: ``$ kinit {your_user_name}@Y.CORP.YAHOO.COM``
#. Confirm that your ticket was created: ``$ klist``


Creating Data Source definitions with iODBC:
*********************************************

#. Run `iODBC Administrator64` to define data sources, with Simba driver.
#. Under the ``User DSN`` tab, click on ``Add``.
#. If authenticating to the Hadoop cluster via Kerberos credentials, (e.g. to KryptoniteRed cluster in YGRID), use the following settings:

    - ``Data Source Name (DSN)``: A descriptive name for the data source (E.g. ``KryptoniteRed HS2 Kerberos``)
    - ``Comment``: A descriptive comment for the data source (E.g. ``KryptoniteRed Hive with Kerberos Authentication``
    - ``Host``: Hostname (E.g. ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``)
    - ``Port``: ``50514``
    - ``HiveServerType``: ``2`` (i.e. HiveServer2)
    - ``ThriftTransport``: ``1`` (i.e. For Thrift/SASL)
    - ``AuthMech``: ``1`` (i.e. "Kerberos")
    - ``KrbServiceName``: ``hive`` (Kerberos Service name for YGRID cluster. On VCG, it is ``HTTP``)
    - ``KrbHostFQDN``: ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com`` (Kerberos FQDN)
    - ``KrbRealm``: ``YGRID.YAHOO.COM`` (Kerberos Realm for YGRID clusters. For VCG, it is ``VCG.OUROATH.COM``).
    - ``Schema``: Name of database being accessed (E.g. ``default``).

   .. image:: images/macos_iodbc64_kerberos.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup on MacOS
      :align: center

#. Alternatively, if authenticating to the Hadoop cluster via X509 certificates, (e.g. to Polaris cluster in VCG), use the following settings:

    - ``Data Source Name (DSN)``: A descriptive name for the data source (E.g. ``Polaris HS2 X509``)
    - ``Comment``: A descriptive comment for the data source (E.g. ``Polaris Hive with Athenz Certificates``
    - ``Host``: Hostname (E.g. ``polarisgq-hs.gq.vcg.yahoo.com``)
    - ``Port``: ``4443``
    - ``HiveServerType``: ``2`` (i.e. HiveServer2)
    - ``ThriftTransport``: ``2`` (i.e. HTTPS)
    - ``SSL``: ``1`` (i.e. SSL-enabled)
    - ``TwoWaySSL``: ``1`` (i.e. Enable mutual TLS)
    - ``AuthMech``: ``0`` (i.e. "No authentication" (though, not really.))
    - ``HTTPPath``: ``cliservice`` (i.e. The webservice end-point.)
    - ``CAIssuedCertNamesMismatch``: ``1`` (i.e. Allow the names in CA-issued SSL certificates *not* to match the HS2 hostname.)
    - ``ClientCert``: Path to user's Athenz Role-certificate
    - ``ClientPrivateKey``: Path to user's private key
    - ``Schema``: Name of database being accessed (E.g. ``default``).

   .. image:: images/macos_iodbc64_x509.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup on MacOS
      :align: center

#. Click on ``OK`` to save. This saves the settings to ``~/odbc.ini``. For instance, for the two data sources defined above,
(``KryptoniteRed`` and ``Polaris``), the ``odbc.ini`` would have the following entries:

    .. image:: images/macos_odbc_ini.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  odbc.ini on MacOS
      :align: center

#. Click on ``Test`` to test that the data source was configured correctly. If a dialog-box pops up for ``Username`` and ``Password``, leave them blank.
A successful test should display a window saying ``The connection DSN was tested successfully, and can be used at this time.``.

.. _tableau_setup-windows:

Windows
#######

.. _tableau-reqs:

Requirements
************

- `Windows 7 <http://windows.microsoft.com/en-us/windows7/products/system-requirements>`_ 
  (Yahoo corporate Win7 64-bit image)


.. _tableau_setup-install:

Install MIT Kerberos Software
*****************************

#. `Download the installer for 64-bit system <https://web.mit.edu/kerberos/dist/>`_.
#. Run the installer by clicking the file and choosing the **Typical** install as shown below.

   .. image:: images/kerberos_setup.jpg
      :height: 394 px
      :width: 506 px
      :scale: 95%
      :alt:  Kerberos Install and Setup
      :align: left
   
#. When prompted by dialog **User Account Control** seen below, click **Yes**.
   (Ignore any warnings thrown by anti-virus software.) 

   .. image:: images/user_control_permission.jpg
      :height: 260 px
      :width: 466 px
      :scale: 95%
      :alt:  Kerberos Permissions
      :align: left

#. To set up Kerberos configuration file:

   - Obtain  a sample :ref:`krb5.conf <sample_krb5_config>` configuration file for your Kerberos setup. 
     (When working on your own  obtain ``/etc/krb5.conf`` from the appropriate cluster's 
     gateway.)
   - Change to ``C:\ProgramData\MIT\Kerberos5``. This is normally a hidden directory. 
     (Consult your Windows documentation if you wish to view and use this hidden directory.)
   - From **Explorer**, you'll see an empty file named ``krb5.ini``. This file is read-only. 
   - Right-click the file and open its **Properties**.
   - From the **Properties** window, select the **Security** tab. 
   - From the **Security** tab, select **Users**  and click **Edit** to change permissions.
   - From the **Security** dialog, select **Users** again and check the checkbox for 
     **Full Control** to give yourself write access.
   - Copy the contents of ``krb5.conf`` to overwrite those of the ``krb5.ini`` file
     and restore the permissions of ``krb5.ini`` so that it is again just read-only.

#. To set up the Kerberos credential cache:

   #. Create a writable directory ``C:\temp``. (You can use any directory name.)
   #. Click the Windows **Start** menu.
   #. Right-click **Computer** and click **Properties**.
   #. From the **Properties** dialog, click **Advanced system settings** as shown here.

      .. image:: images/kerberos_adv_setting.jpg
         :height: 597 px
         :width: 797 px
         :scale: 85%
         :alt:  Kerberos Advanced Settings
         :align: left
   
   #. From the **System Properties** dialog shown below, click **Environment Variables…**.

      .. image:: images/system_settings.jpg
         :height: 473 px
         :width: 423 px
         :scale: 90%
         :alt:  Kerberos Advanced Settings
         :align: left
   #. From the **Environment Variables** dialog, click **New…** for **System variables**.
   #. From the **New System Variable** dialog shown below, enter the variable name **KRB5CCNAME**
      and the variable value **FILE:\temp\krb5cache** as shown below:

      .. image:: images/new_user_variable.jpg
         :height: 151 px
         :width: 354 px
         :scale: 100%
         :alt:  Kerberos Advanced Settings
         :align: left
   #. Click **OK** to save the variable.
   #. Confirm that the variable is listed in the **System variables** list.
   #. Click **OK** to close **Environment Variables**.
   #. Click **OK** to close **System Properties**.


#. Restart your computer to ensure **MIT Kerberos for Windows** uses the new settings.
#. Use the **MIT Kerberos Ticket Manager** to obtain a ticket for the principal that will 
   be connecting to Hive 0.10. Enter your principal and Windows/Exchange password as shown 
   in the figure below.

   - Your principal is ``{your_corp_id}@Y.CORP.YAHOO.COM``, if you're on the ``Y`` domain.

   .. image:: images/kerberos_get_ticket.jpg
      :height: 225 px
      :width: 568 px
      :scale: 95%
      :alt:  Kerberos Get Ticket
      :align: left

#. On your Windows host, click **Start > All Programs > Control Panel > Network and Internet > Network and Sharing Center**.
#. Click **Change adapter settings** in the left panel seen below.

   .. image:: images/adapter_sharing.jpg
      :height: 682 px
      :width: 800 px
      :scale: 95%
      :alt:  Kerberos: Change Adapter Settings
      :align: left

When using network other than the Yahoo corporate network, you will need to update the
principals and IP addresses for DNS. Please consult your system administrator, or contact grid-ops.

.. _tableau_setup-odbc-kerberos:

Install and Configure the Simba Hive ODBC Driver to access Hive via Kerberos
*********************************************************************************

#. `Download the installer <https://drive.google.com/file/d/1jn0V5lbGGILTM2uUZuG_MGKczfqA7HKr/view?usp=sharing>`_
   for the Simba Hive ODBC driver. (Ensure that the file is saved with the extension ``.msi``.)
#. Run the installer, clicking **Yes** whenever prompted by **User Account Control** and 
   ignoring any warnings thrown by anti-virus software.
#. `Download the Simba Hive ODBC driver license <https://drive.google.com/file/d/14qIwOKfMO996Hd94bBfZY7oyEPpPOc8R/view?usp=sharing>`_
#. Copy the license file to ``C:/Program Files/Simba Hive ODBC Driver/lib``, and rename to ``SimbaApacheHiveODBCDriver.lic``.

#. Go to **Start > All Programs > Simba Hive ODBC Driver 2.6 (64-bit) > Driver Configuration**.
#. When prompted by **User Account Control**, click **Yes** to open the **ODBC Data Source Administrator** dialog.
#. In **Simba Hive ODBC Driver Configuration**, enter the following, being sure not to add extra
   spaces before or after the configuration value as that will cause errors:

   - **Hive Server Type:** Choose **Hive Server 2** from the drop-down list
   - **Service Discovery Mode:** Choose **No Service Discovery** from the drop-down list
   - **ZooKeeper Namespace:** Leave blank
   - **Authentication Mechanism:** Choose **Kerberos** from the drop-down list.
   - **Realm:** ``YGRID.YAHOO.COM``
   - **Host FQDN:** ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com`` (Again, refer to 
     :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>` when setting up for another grid VIP.)
   - **Service Name:** ``hive``
   - **Canonicalize Principal FQDN:** Leave unchecked
   - **Delegate Kerberos Credentials:** Leave unchecked
   - **User Name:** Leave blank
   - **Password:** Leave blank
   - **Delegation UID:** Leave blank
   - **Thrift Transport:** Choose **SASL** from drop-down list

   The filled out fields in the dialog **Simba Hive ODBC Driver Configuration** should
   look similar to the following figure:

   .. image:: images/simba_hive_odbc_driver_config.jpg
      :height: 706 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver Configuration
      :align: left

#. Click **Advanced Options...** to open the **Advanced Options** dialog.
#. From the dialog box, set **Rows fetched per block** to ``500`` as shown below. 

   .. image:: images/odbc_dsn_setup_adv_options.jpg
      :height: 342 px
      :width: 473 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup: Advanced Options
      :align: left

#. From the same dialog box, click **Add...** to add the server property for configuring a job queue.
#. In the **Edit Property** dialog, enter the key **mapred.job.queue.name**, the name of the job
   queue to use and click **OK**. You will need to have **SUBMIT_APPLICATION** ACL permission to the job queue.

   .. note:: Again, if you need to find the job queues that you can access, log on to the cluster (``in this case kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``)
             and run the command ``mapred queue -showacls``. You should see the queue names and the operations
             that are allowed. You can use the job queue that list the operation **SUBMIT_APPLICATIONS**.

#. Go to **ODBC Data Sources (64-bit)**. We're
   going to set many of the same configurations with the administrator tool.

#. When prompted by **User Account Control**, click **Yes** to open the **ODBC Data Source Administrator** dialog.
#. From the **ODBC Data Source Administrator** dialog shown below, select the second tab **System DSN**. 

   .. image:: images/odbc_data_src_admin.jpg
      :height: 389 px
      :width: 471 px
      :scale: 95%
      :alt:  ODBC Data Source Administrator: System DSN
      :align: left

#. From the **System DSN** dialog, you'll see **Simba Hive**. Select it and click **Configure...**
   as shown below.

   .. image:: images/hive_odbc_sys_dsn.jpg
      :height: 389 px
      :width: 471 px
      :scale: 95%
      :alt:  ODBC Data Source Administrator: System DSN
      :align: left
#. In **Simba Hive ODBC Driver DSN Setup**, enter the following. Again, be sure not to add extra
   spaces before or after the configuration value as that will cause errors:

   - **Data Source Name: {A descriptive name for the connection/data-source}
   - **Description:** {Description of the connection/data-source}
   - **Hive Server Type:** Choose **Hive Server 2** from the drop-down list
   - **Service Discovery Mode:*** ``No Service Discovery``
   - **Host:** ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com`` (When setting up for other
     grid hosts, please refer to :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>`.)
   - **Port:** 50514
   - **Database:** ``my_db``, or the name of the database being accessed. (To view the available databases, log on to the grid host,
     start the Hive shell, and run ``show databases;``.)
   - ZooKeeper Namespace: {blank}
   - **Authentication Mechanism:** Choose **Kerberos** from the drop-down list.
   - **Realm:** ``YGRID.YAHOO.COM``
   - **Host FQDN:** ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com`` (Again, refer to 
     :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>` when setting up for another grid VIP.)
   - **Service Name:** ``hive``
   - **Canonicalize Principal FQDN:** Leave unchecked
   - **Delegate Kerberos Credentials:** Leave unchecked
   - **User Name:** Leave blank
   - **Password:** Leave blank
   - **Delegation UID:** Leave blank
   - **Thrift Transport:** Choose **SASL** from drop-down list

   The filled out fields in the dialog **Simba Hive ODBC Driver DSN Setup** should
   look similar to the following figure:

   .. image:: images/simba_hive_odbc_dsn_setup.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup
      :align: left

#. Click **Advanced Options...** to open the **Advanced Options** dialog.
#. From the dialog box, set **Rows fetched per block** as we did earlier for the **Simba Hive ODBC Driver Configuration**.
#. From the same dialog box, click **Add...** to add the server property for configuring a job queue.
#. In the **Edit Property** dialog, as before, enter the key **mapred.job.queue.name**, the same job
   queue name that you entered before, and click **OK**. (Again, you will need to have **SUBMIT_APPLICATION** ACL permission to the job queue.)
#. Click **OK** to close the box.
#. From the **Simba Hive ODBC Driver DSN Setup** dialog, click **Test** to see if things work.
   If all goes well, you should see **TESTS COMPLETED SUCCESSFULLY!**.
   If your Kerberos credentials have expired, you'll get **GSSAPI Error** or get the **MIT 
   Kerberos** window to renew them, provided the **MIT Kerberos Ticket Manager** is already running 
   in the background. Enter your principal as instructed above to let the test proceed.

#. Click **OK** to close the setup and then close the **ODBC Administrator**.
#. Congratulations, you can now use the Simba Hive ODBC Driver with Tableau or any ODBC enabled application.


.. _tableau_setup-odbc-x509:

Install and Configure the Simba Hive ODBC Driver to access Hive via Athenz (X509) certificates
*************************************************************************************************

#. `Download the installer <https://drive.google.com/file/d/1jn0V5lbGGILTM2uUZuG_MGKczfqA7HKr/view?usp=sharing>`_
   for the Simba Hive ODBC driver. (Ensure that the file is saved with the extension ``.msi``.)
#. Run the installer, clicking **Yes** whenever prompted by **User Account Control** and
   ignoring any warnings thrown by anti-virus software.
#. `Download the Simba Hive ODBC driver license <https://drive.google.com/file/d/14qIwOKfMO996Hd94bBfZY7oyEPpPOc8R/view?usp=sharing>`_
#. Copy the license file to ``C:/Program Files/Simba Hive ODBC Driver/lib``, and rename to ``SimbaApacheHiveODBCDriver.lic``.

#. Go to **ODBC Data Sources (64-bit)**. Select to "Run as administrator". We're
   going to set many of the same configurations with the administrator tool.

#. If prompted by **User Account Control**, click **Yes** to open the **ODBC Data Source Administrator** dialog.
   Enter the administrator password, if prompted.
#. From the **ODBC Data Source Administrator** dialog shown below, select the second tab **System DSN**.

   .. image:: images/odbc_data_src_admin.jpg
      :height: 389 px
      :width: 471 px
      :scale: 95%
      :alt:  ODBC Data Source Administrator: System DSN
      :align: left

#. From the **System DSN** dialog, select "Add..." on the right, to add a new data source.

   .. image:: images/odbc_new_data_src.jpg
      :height: 389 px
      :width: 471 px
      :scale: 95%
      :alt:  ODBC Data Source Administrator: System DSN
      :align: left
#. In **Simba Hive ODBC Driver DSN Setup**, enter the following to set up connectivity for Polaris, via X509 certificates.
   Again, be sure not to add extra spaces before or after the configuration value as that will cause errors:

   - **Data Source Name:** {A descriptive name for the connection/data-source}
   - **Description:** {Description of the connection/data-source}
   - **Hive Server Type:** Choose **Hive Server 2** from the drop-down list
   - **Service Discovery Mode:*** ``No Service Discovery``
   - **Host(s):** ``polarisgq-hs2-v2.vcg.vip.gq2.yahoo.com`` (When setting up for other
     grid hosts, please refer to :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>`.)
   - **Port:** ``4443``
   - **Database:** ``my_db``, or the name of the database being accessed. (To view the available databases, log on to the grid host,
     start the Hive shell, and run ``show databases;``.)
   - ZooKeeper Namespace: {blank}
   - **Authentication Mechanism:** Choose **No Authentication** from the drop-down list.
   - **Realm:**          {blank}
   - **Host FQDN:**      {blank}
   - **Service Name:**   {blank}
   - **User Name:**      {blank}
   - **Password:**       {blank}
   - **Delegation UID:** {blank}
   - **Thrift Transport:** Choose **HTTP** from the drop-down list

   The filled out fields in the dialog **Simba Hive ODBC Driver DSN Setup** should
   look similar to the following figure:

   .. image:: images/simba_hive_odbc_dsn_x509_setup.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup
      :align: left

#. Click on **HTTP Options**, to configure HTTP Settings:

   - Set **HTTP Path:** to ``cliservice``

   .. image:: images/simba_hive_odbc_dsn_x509_http.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup
      :align: left

#. Click on **SSL Options** to set up Mutual TLS and certificate settings:

   - **Enable SSL:** Ensure this is enabled
   - **Allow Common Name Host Name mismatch:** Ensure this is enabled
   - **Allow Self-signed Server Certificate:** Disable
   - **Use System Trust Store:** Enable
   - **Check Certificate Revocation:** Enable
   - **Trusted Certificates:** {blank} (Using System Trust Store, for publicly signed certificates)
   - **Minimum TLS Version:** ``1.2``
   - **Two-way SSL:** Enabled
   - **Client Certificate File:** Path to X509 Certificate (PEM), fetched from Athenz
    `as described here <https://docs.google.com/document/d/1lFL4u2bZfwX8K6VjDKYaQZokrcZCkKU3oUF78jQMDaM/edit#heading=h.m0hb5o3dgrys>`_.
   - **Client Private Key File:** Path to User's private key (PEM)
   - **Client Private Key Password:** Password for the Private Key, if there is one. Typically defaults to ``changeit``, or leave it {blank}.

   .. image:: images/simba_hive_odbc_dsn_x509_ssl.jpg
      :height: 470 px
      :width: 353 px
      :scale: 95%
      :alt:  Simba Hive ODBC Driver DSN Setup
      :align: left

#. Click **Advanced Options...** to open the **Advanced Options** dialog.
#. From the dialog box, set **Rows fetched per block** to ``500``.
#. From the same dialog box, click **Add...** to add the server property for configuring a job queue.
#. In the **Edit Property** dialog, as before, enter the key **mapred.job.queue.name**, the same job
   queue name that you entered before, and click **OK**. (Again, you will need to have **SUBMIT_APPLICATION** ACL permission to the job queue.)
#. Click **OK** to close the box.
#. From the **Simba Hive ODBC Driver DSN Setup** dialog, click **Test** to see if things work.
   If all goes well, you should see **TESTS COMPLETED SUCCESSFULLY!**.
   Otherwise, the error message should describe the SSL-connection error.

#. Click **OK** to close the setup and then close the **ODBC Administrator**.
#. Congratulations, you can now use the Simba Hive ODBC Driver with Tableau or any ODBC enabled application.

.. _tableau-install:

II. Installing Tableau 8.0
--------------------------

.. _tableau_install-trial:

Trial Version
#############

Before getting a licensed copy of Tableau, first download a full-functioning free 
trial of Tableau's Software:

- `Tableau Desktop (Windows) <https://downloads.tableausoftware.com/tssoftware/TableauDesktop-32bit.exe>`_
- `Tableau Desktop (Mac) <http://www.tableausoftware.com/products/desktop/download?os=mac%20os%20x>`_

You can use the trial  version for 14 days without restrictions. If you're ready to get a 
licensed copy, see the next section.

.. _tableau_install-licensed:

Licensed Version
################

Follow the `instructions for obtaining a full license <http://it.corp.yahoo.com/_pages/RequestingSoftware.html-RequestingSiteLicensedSoftware?>`_.
Essentially, you `file a ticket <http://eportal.corp.yahoo.com/?obj_view=create&obj_type=sr>`_. 
The money comes out of each organization's budget, so would require a VP approval. Be sure to 
get the Professional Edition. Again, you'll need **Tableau Desktop**, not **Tableau Server**.

.. _tableau-hiveserver2:

III. Connecting Tableau to HiveServer2
--------------------------------------

After you've installed Tableau, you can connect Tableau to HiveServer2 
using the Simba Hive ODBC Driver by following the steps below:

.. note:: The screenshots were taken on a Windows machine, but the Tableau interface
          for both Mac and Windows are the same except where marked in the instructions below. 
          
#. Start **Tableau Desktop**.
#. In the top-left corner, click **Connect to data**.
#. In the **On a server** list, select **Simba Hadoop Hive**.

   .. note:: Ensure that you've already set up the 'Driver Configuration' 
#. From the **Simba Hadoop Hive Connection** dialog, enter the following information

   * **Step 1: Enter a server name:** ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com`` (For other grid hosts, refer 
     to :ref:`Grid VIP URLs/Ports <gs_appendix-grid_vips>` for the URL and port.)
   * **Port:**   ``50514``
   * **Type:** HiveServer2 
   * **Authentication:** Kerberos
   * **Realm** ``YGRID.YAHOO.COM``
   * **Host FQDN:**  ``kryptonitered-hs2.ygrid.vip.bf1.yahoo.com``
   * **Service Name:** hive
#. Click **Connect**.
#. (**Windows**) If you are denied access, make sure that your MIT Kerberos ticket has not expired. If it has expired,
   for Windows, go to **Start > All Programs > Kerberos for Windows (64-bit) > MIT Kerberos Ticket Manager**  
   as shown below and click **Renew Ticket**. 

   .. image:: images/kerberos_renew_ticket.jpg
      :height: 397 px
      :width: 741 px
      :scale: 95%
      :alt:  MIT Kerberos: Renew Ticket 
      :align: left
   
   (**Mac**) For Macs, if you are denied access, run ``kinit {user_name}@Y.CORP.YAHOO.COM`` from a terminal
   to renew your Kerberos ticket.

#. For **Step 4: Select a schema on the server**, the field should be automatically populated
   with 'default' upon a successful connection. Replace that value with **tableau**.
#. For the table, enter **starling**.
#. **Steps for Windows:** 
   
   #. Select the appropriate option in step 4.
   #. (Optional) Provide a name to this connection. It's automatically created for you **starling (tableau)**
   #. Click **OK**.
   #. From the **Data Connection** dialog shown below, click **Connect live**.

      .. image:: images/data_connection.jpg
         :height: 312 px
         :width: 413 px
         :scale: 95%
         :alt:  Tableau: Data Connection
         :align: left
#. **Steps for Macs:**

   #. From the **Table** panel, drag **starling (tableau.starling)** to the **Drag tables here** panel. 
   #. Click **Go to Worksheet**.
 
#. Congratulations, we're now ready to use Tableau to make queries to the **starling** table in the 
   next section.

.. _tableau-data:

IV. Using Tableau With Data 
---------------------------

In this section, we're just going to run a couple of queries to verify that Tableau
has connected to Hive table ``tableau`` on the grid. To learn how to use Tableau, we
again refer you to the `Tableau Quick Start Guides <http://www.tableausoftware.com/support/manuals/quickstart>`_.

.. note:: Once again, the screenshots of Tableau Desktop were taking on a Windows machine,
          but the differences between the Mac version is negligible. The steps
          for using Tableau in this tutorial are the same.

#. After **Tableau** has connected to the **tableau** table, you should see the 
   **Tableau - Book1** window shown below:

   .. image:: images/tableau_book1.jpg
      :height: 473 px
      :width: 800 px
      :scale: 95%
      :alt:  Tableau: Book1
      :align: left

#. From the **Tableau - Book1** window, select **status** (**Status** on Macs) from the **Data** panel and
   drag it to the **Columns** field.
#. Again from the **Data** panel, drag **grid** (**Grid** on Macs) to the **Rows** field. You should
   see the status codes as the top row and the grids listed in the first column.
#. From the **Data** panel, go to **Measures**, select **Number of Records** and drag
   it to **Text** in the **Marks** panel. 
#. You should see the following simple table showing the status for the different grids:

   .. image:: images/grid_status_table.jpg
      :height: 326 px
      :width: 370 px
      :scale: 100%
      :alt:  Tableau: table of grid statuses
      :align: left
#. From the **Measures** panel, drag **Measure Values** to the **Columns** field to see
   the following bar graph. 

   .. image:: images/tableau_bar_graph.jpg
      :height: 270 px
      :width: 800 px
      :scale: 95%
      :alt:  Tableau: bar graph
      :align: left
   
#. Great, you have confirmed that **Tableau** has accessed your **tableau** table and gotten 
   the basic idea of how to use it. 


.. _bi-custom_client:

Creating Custom Clients with JDBC 
=================================

Introduction
------------

Users can use the Hive JDBC APIs so that client applications
can connect to HiveServer2. The JDBC driver is available as a ``yinst`` package and also through 
``yMaven`` for development.  Only Kerberos authentication is supported. 
The JDBC URIs include QOP and the Kerberos principal.

To use JDBC to connect to HiveServer2, you would use the URL below, where ``<host>``
would be the Grid cluster, ``<database>`` the name of the Hive database you are using,
and ``<principal>`` being the  HiveServer2 principal.

    jdbc:hive2://<host>:50514/<database>;saslQop=auth-conf;principal=<principal>

.. note:: If you are using Tableau or MicroStrategy, you do not need to create a custom client with 
          JDBC. If you are unsure if you need to create a custom client with JDBC, ask Hive users 
          on the iList yahoo-hive-dev@verizonmedia.com.

JDBC Requirements
-----------------

- ACLs on JDBC client should be set up.
- Access to Kerberos servers.
- Access to HiveServer2 machines and ports.
- The JDBC driver works with >= RHEL6.4 and Java 7.
- Paranoid approval during onboarding since data on the grid might be opened up.
  and we will get it going.

Limitations
-----------

- custom UDFs are not supported
- only read operations supported

.. _beeline_jdbc_sasl_kerberos:

Using Beeline With JDBC (SASL/Kerberos)
---------------------------------------

To use the JDBC client ``Beeline`` to get data through HiveServer2,
follow the steps below.

#. Log onto a Grid gateway such as Kryptonite Red (``kryptonite-gw.red.ygrid.yahoo.com``).
#. Get Kerberos Ticket, via ``kinit`` or `pkinit <http://yo/pkinit>`_. E.g. : ``$ kinit <user>@Y.CORP.YAHOO.COM``
#. Run Beeline as follows, to connect to HS2 over SASL/Kerberos: ::

        $ hive --service beeline -n "" -p "" -u "jdbc:hive2://kryptonitered-hs2.ygrid.vip.bf1.yahoo.com:50514/default;saslQop=auth-conf;principal=hive/kryptonitered-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM" -e " show databases "
        ...
        Connecting to jdbc:hive2://kryptonitered-hs2.ygrid.vip.bf1.yahoo.com:50514/default;saslQop=auth-conf;principal=hive/kryptonitered-hs2.ygrid.vip.bf1.yahoo.com@YGRID.YAHOO.COM
        Connected to: Apache Hive (version 1.2.7.3.1909180546)
        Driver: Hive JDBC (version 1.2.7.3.1909180546)
        Transaction isolation: TRANSACTION_REPEATABLE_READ
        +------------------------+
        |     database_name      |
        +------------------------+
        | acluster               |
        | ajaytestdb             |
        | ajeeshr                |
        | ...                    |

.. _beeline_jdbc_https_kerberos:

Using Beeline With JDBC (Thrift/HTTPS with Kerberos)
----------------------------------------------------

To use the JDBC client ``Beeline`` to get data through HiveServer2,
follow the steps below.

#. Log onto a Grid gateway such as Kryptonite Red (``kryptonite-gw.red.ygrid.yahoo.com``).
#. Get Kerberos Ticket, via ``kinit`` or `pkinit <http://yo/pkinit>`_. E.g. : ``$ kinit <user>@Y.CORP.YAHOO.COM``
#. Run Beeline as follows, to connect to HS2 over HTTPS with Kerberos authentication: ::

        $ hive --service beeline -n "" -p "" -u "jdbc:hive2://kryptonitered-hs2.red.ygrid.yahoo.com:4443/default;transportMode=http;httpPath=cliservice;ssl=true;auth=kerberos;sslTrustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.jks;principal=HTTP/kryptonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM" -e " show databases ; "
        ...
        Connecting to jdbc:hive2://hs452n22.red.ygrid.yahoo.com:4443/default;transportMode=http;httpPath=cliservice;ssl=true;auth=kerberos;sslTrustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.jks;principal=HTTP/kryptonitered-hs2.red.ygrid.yahoo.com@YGRID.YAHOO.COM
        Connected to: Apache Hive (version 1.2.7.4.1910031932)
        Driver: Hive JDBC (version 1.2.7.3.1909180546)
        Transaction isolation: TRANSACTION_REPEATABLE_READ
        +------------------------+
        |     database_name      |
        +------------------------+
        | acluster               |
        | ajaytestdb             |
        | ajeeshr                |
        | ...                    |

.. _beeline_jdbc_https_x509:

Using Beeline With JDBC (Thrift/HTTPS with Athenz X509 Certificates)
--------------------------------------------------------------------

To use the JDBC client ``Beeline`` to get data through HiveServer2,
follow the steps below.

#. Log onto a Grid gateway such as Kryptonite Red (``kryptonite-gw.red.ygrid.yahoo.com``).
#. Fetch Athenz user-certificate: ``$ athenz-user-cert``. Touch YubiKey if prompted to.
#. Fetch role-certificate using ``zts-rolecert``, as per GridOps' `documentation <https://docs.google.com/document/d/1fUziPmsB-QALJtqQ6QZ9xf18n6mLOqRHasR9Ru7hXMg/edit>`_.
#. Convert role-certificate and private key into a Java KeyStore (e.g. ``griduser.role.uid.mithunr.jks``) , as per :ref:`instructions in the Appendix <gs_appendix-generate-role-certs>` of this document.
#. Run Beeline as follows, to connect to HS2 over HTTPS with X509 authentication: ::

        $ hive --service beeline -n "" -p "" -u "jdbc:hive2://kryptonitered-hs2.red.ygrid.yahoo.com:4443/default;transportMode=http;httpPath=cliservice;ssl=true;sslTrustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.jks;twoWay=true;sslKeyStore=/homes/mithunr/.athenz/griduser.role.uid.mithunr.jks;keyStorePassword=changeit" -e " show databases ; "t
        ...
        Connecting to jdbc:hive2://kryptonitered-hs2.red.ygrid.yahoo.com:4443/default;transportMode=http;httpPath=cliservice;ssl=true;sslTrustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.jks;twoWay=true;sslKeyStore=/homes/mithunr/.athenz/griduser.role.uid.mithunr.jks;keyStorePassword=changeit
        Connected to: Apache Hive (version 1.2.7.4.1910031932)
        Driver: Hive JDBC (version 1.2.7.3.1909180546)
        Transaction isolation: TRANSACTION_REPEATABLE_READ
        +------------------------+
        |     database_name      |
        +------------------------+
        | acluster               |
        | ajaytestdb             |
        | ajeeshr                |
        | ...                    |

Tutorial: Creating a Client Application That Uses JDBC
-------------------------------------------------------

The following steps will show you how to use the JDBC driver for a simple example. 

Prerequisites
#############

- Have access to a Grid cluster. If you don't have access to a cluster yet, we recommend
  `on-boarding to Kryptonite Red <http://adm005.ygrid.corp.bf1.yahoo.com:9999/grid_forms/main.php>`_ 
  (File request from **User Account** tab.)

Setting Up
##########

#. Clone the example code:: 

       git clone git@git.ouroath.com:thiruvel/hive_jdbc_sample.git
#. Change to the ``hive_jdbc_sample`` directory.
#. Update the version of Hive in the ``pom.xml`` file. See the ``hive-client`` 
   column in the `Grid Versions table <http://yo/gridversions>`_
   to find the Hive version for the cluster you're using. 
#. Build the project:: 

       mvn clean package
#. Copy the sample code with the built project to Kryptonite (or the cluster you're 
   using). For example: ``scp -r hive_jdbc_sample {your_user_name}@kryptonite-gw.red.ygrid.yahoo.com:~/``
#. Log onto to the cluster. For example: ``ssh kryptonite-gw.red.ygrid.yahoo.com``
#. Change to the ``hive_jdbc_sample/scripts`` directory.

Run Query
#########

#. From the scripts directory, if you are **NOT** using Kryptonite Red, 
   either un-comment the variables ``HS2HOST`` and ``DB`` for Cobalt Blue 
   or for other clusters.

   For other clusters, you'll have to replace {cluster_name} and {colo} with the 
   appropriate information. To find the URL to the HiveServer2 in a cluster, 
   see `Grid Versions table <http://yo/gridversions>`_. 

   .. note:: We're using port 5015 and appending ``-noenc`` to ``hs2`` because we're not 
             using encryption for this example.   

#. Use ``kinit`` for authentication: ``kinit {your_user_name}@Y.CORP.YAHOO.COM``
#. Run the ``results.sh`` script that uses the JDBC driver to execute the HQL 
   (``show tables``) on the specified database.
#. You should see the following tables in the returned results. If you are getting 
   connection errors, check that the ``JDBCURI`` variable is assigned the correct URL. 
   If you're having issues with the database or table, confirm that the
   database exists on the cluster and that it has tables.

Closer Look at the Code
#######################

results.sh
**********

We are doing three main tasks in this file:

#. Defining the URI to the JDBC to the Starling table on Axonite Blue::

       HS2HOST=axoniteblue-hs2.ygrid.vip.gq1.yahoo.com
       JDBCURI="jdbc:hive2://$HS2HOST:4443/starling;transportMode=http;httpPath=cliservice;ssl=true;auth=kerberos;sslTrustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.jks;principal=HTTP/$HS2HOST@YGRID.YAHOO.COM;mapred.job.queue.name=unfunded"

#. Pointing to the JAR that we built. That JAR creates the connection and executes our HQL statement::

      JAR="../target/hive_jdbc_example-1.0-SNAPSHOT.jar"

#. Running the Hadoop command that uses the JAR to execute the HQL with the JDBC URI::

       /home/gs/hadoop/current/bin/hadoop jar $JAR com.yahoo.hive.HelloHiveServer2 "$QUERY" "$JDBCURI"

HelloHiveServer2.java
*********************

The simple ``HelloHiveServer2`` class attempts a connection to the JDBC path,
executes the HQL statement, and prints the result set. 

Before we can make a connection with the JDBC, we use the private method ``loadClass`` 
to load the class ``HiveDriver``:

.. code-block:: java

   private static void loadClass() {
      try {
         String driverName = "org.apache.hive.jdbc.HiveDriver";
         Class.forName(driverName);
      } catch (ClassNotFoundException e) {
          System.err.println("ERROR: Loading class " + e.getMessage());
          System.exit(1);
      }
   }

With the ``HiveDriver`` class loaded, we then can make a connection 
to the HiveServer2 with the JDBC and execute the HQL statement
that simply prints out tables in a given database.

.. code-block:: java

   try {

       /*
       * Note:
       * Do not leave a connection object idle for a long time. Since the
       * connections go through VIP, idle connections are closed by the VIP and one
       * might get a connection reset error on the client.
       */
       connection = DriverManager.getConnection(jdbcURI);
       stmt = connection.createStatement();

       // One can also set Hive/Hadoop parameters like this. They can also be set via JDBC URI.
       stmt.execute("set mapred.job.queue.name=unfunded");

       if (stmt.execute(sql)) {
           resultSet = stmt.getResultSet();

           while (resultSet.next()) {
               // Print the first String just to confirm we are reading data.
               System.out.println(resultSet.getString(1));
           }
           resultSet.close();
       }

    } finally {

        /*
        * Please close all resources as soon as you can. Otherwise, the server will
        * be holding an idle connection and objects associated with it for a long time.
        */
        if (stmt != null) {
           stmt.close();
        }
        if (connection != null) {
           connection.close();
        }
   }


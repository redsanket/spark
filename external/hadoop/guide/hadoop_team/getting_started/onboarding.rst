.. _hadoop_team_getting_started_onboarding:

##################
Onboarding
##################

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------


You have attended the `GCD orientation session <https://thestreet.ouroath.com/community/globalservicedesk/>`_ . By now, you
should be able to access the internal network on your laptop and you
should have the following:

1. email address: i.e. john.doe@verizonmedia.com
2. a user ID: i.e., djohn
3. a ``ubkey`` that you attach to the USB port
4. Duo Mobile app installed on your phone

.. _hadoop_team_getting_started_onboarding_resources:

*********************
Resources and Tools
*********************

#. Use ``BREW`` (homebrew) to install tools needed for building hadoop (on Mac).

#. Attend Orientation sessions (see `yo/cmitraining <https://yo/cmitraining>`_); especially the one about debugging and hadoop.

   * `yo/cmitraining <https://yo/cmitraining>`_
   * `yo/champaignslides <https://yo/champaignslides>`_

#. Become familiar with Jira: `Internal Hadoop Jira home-page <https://jira.vzbuilders.com/projects/YHADOOP/summary>`_
#. Open Source:

   * `How to Contribute to Apache Hadoop <https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute/>`_
   * `Jira <https://issues.apache.org/jira/secure/Dashboard.jspa/>`_

#. :guilabel:`IT Environment:`

   * Sean Smith is the Champaign IT guy. He should walk you throw the steps to have your account set-up.
   * OKTA - ``oath.okta.com``
   * `myworkday <https://wd5.myworkday.com/oath/d/home.htmld>`_ : submit time-off, profile information, job/pay,..etc.
   * Google Hang-Outs - `yo/join <https://yo/join>`_, `yo/hangouts <https://yo/hangouts>`_, `yo/startmeeting <https://yo/startmeeting>`_
   * `LastPass <https://ecp.vzbuilders.com/users/me/lastpass>`_ provides the ability to manage and share confidential information such as passwords, application credentials etc.
   * GSD global support desk (`yo/chat <https://yo/chat>`_)

   * :token:`Paranoids:`

     * yo/paranoids, yo/one-phish-two-phish, yo/phishy
     * phish@verizonmedia.com
     * paranoids@verizonmedia.com
     * Forward suspicious emails

   * :token:`For Champaign (CMI):`

      * GSD related stuff: `champaign-gsd slack channel <https://vzbuilders.slack.com/archives/C7X7U7J9X>`_
      * `CMI landing page <http://yo/cmi>`_
      * `General CMI slack channel <https://vzbuilders.slack.com/archives/G61MG9Q5Q>`_
      * `CMI Announcements slack channel <https://vzbuilders.slack.com/archives/C7XT83VQV>`_: This SLACK Channel is only for announcements. Please avoid discussions.
      * :token:`CMI Office:`

         * YFi is the primary network
         * YGuest - visitors - there’s also an extended-stay
         * Add printers by IP address (IP address is on the printer) ``CHP-114-Xeroxmfp``
         * Real Estate Workplace - REW - `yo/rew (Leslie Reynolds) <https://yo/rew>`_)

#. :guilabel:`Tools/Misc:`

   * `yo links <http://yo/links>`_
   * `yo/dogfood <https://yo/dogfood>`_ - test Verizonmedia products internally
   * Books warehouse: `yo/safaribooksonline <https://yo/safaribooksonline>`_
   * The Street - Company Directory: Management chain - The Street - Employee Directory - see full org
   * Corporate Phone - `yo/smartphone <https://yo/smartphone>`_
   * Perks at `yo/perks <https://yo/perks>`_

#. :guilabel:`GRID Tools`

   * `pkinit User Guide <https://git.ouroath.com/pages/developer/Bdml-guide/migrated-pages/PKINIT_User_Guide/>`_
   * `yinst User Guide <https://git.ouroath.com/pages/Tools/guide-yinst/>`_ describes how to use the Yahoo! installer tool, ``yinst``
   * `yo/supportshop <https://yo/supportshop>`_ yo/supportshop - GRID dashboard
   * `yo/gridci <https://yo/gridci>`_ yo/gridci - Build status
   * Check grid access at: `yo/doppler <https://yo/doppler>`_
   * Important contacts:

      * Koji Noguchi
      * Shawna Martell
      * David Kuder
      * Dheeraj Kapur

   * Published binaries:

      * Packages archive to search existing packages and jars - `yo/artifactory <https://yo/artifactory>`_
      * Publishing artifacts `userguide <https://git.ouroath.com/pages/developer/Open-Source-Guide/docs/publishing/publishing-artifacts/>`_

   * :token:`Grid Access:`

      * Get familiar with the grid naming convention: `Gateway Naming Convention <https://git.ouroath.com/pages/developer/Bdml-guide/Gateway_Naming_Convention/>`_
      * `yo/grid-dash <https://yo/grid-dash/>`_ List all hadoop clusters
      * There is a list of yo/links to access cluster web UI. Go to `yo/links <https://yo.vzbuilders.com/ylinks/index.php>`_ and search by owner ``ebadger``.
        For example, to access Jet Blue Resource manager, it will be `yo/jbrm <https://yo/jbrm>`_.
      * `ygrid versions <https://git.ouroath.com/pages/GridSE/gsdocs/ygrid_versions.html/>`_ also has clusters list.
      * :token:`Software versions on Clusters:`

         * `ygrid versions <https://git.ouroath.com/pages/GridSE/gsdocs/ygrid_versions.html/>`_
         * `vcg versions <https://git.ouroath.com/pages/GridSE/gsdocs/vcg_versions.html/>`_
         * `rdg versions <https://git.ouroath.com/pages/GridSE/gsdocs/rdg_versions.html/>`_
         * Libra View: Web UI for viewing statistics/troubleshooting of VIPs, servers in VIPs, etc.

            * `Legacy yo/vipviewer <https://yo/vipviewer/>`_
            * `Libra page <https://libra.ops.corp.yahoo.com:4443/>`_
            * `Confluence Documentation <https://confluence.vzbuilders.com/pages/viewpage.action?spaceKey=GNISDOCS&title=Libra+View/>`_

      * :token:`Grid Command Line`

          This `Bdml-guide CLI grid documentation <https://git.ouroath.com/pages/developer/Bdml-guide/grid_cline/>`_ provides a good source for some useful commands to access the grid command line, and run basic HDFS commands

      * :token:`OpsDB`

         * `API Documentation <https://git.ouroath.com/pages/ops-opsdb/apidoc/>`_
         * list of V4 APIs in `production <https://api.opsdb.ops.yahoo.com:4443/V4/>`_
         * list of V4 APIs in `QA (development server) <https://qa.api.opsdb.ops.yahoo.com:4443/V4/>`_

      * :token:`Roles:` provide named groupings of Yahoo!

         * Get familiar with `Roles <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Devel/Roles.html/>`_
         * List Roles per clusters/nodes through `web-page <https://roles.corp.yahoo.com/ui/role/>`_

              * Example `listing Role hbase <https://roles.corp.yahoo.com/ui/role?action=list&page_to=1&per_page=25&sort_by=&role=hbase&tags=&members=&Xycrumb=rz-vtkx7ep4zJEKGu-TeMrZMkC3oKOL5dDs_eDL0MaY&bycrumb=zWK-TfH5h8U29ZJWLSFwZVFic5zcUtXFWHNaJTSff_M/>`_
              * Example `rsgroup <https://roles.corp.yahoo.com/ui/role?action=view&id=890657&Xycrumb=rz-vtkx7ep4zJEKGu-TeMrZMkC3oKOL5dDs_eDL0MaY&bycrumb=gCrvnPpxb4bwBevd1L2fZuUEiswkloy35PSpa-xFg-E/>`_

         * rocl -- a RolesDB command-line client. It provides a command-line interface to RolesDB.
           It can be used to list and manipulate keywords, members, namespaces, roles, tags and specifications by various criteria.
           See `github repo <https://git.ouroath.com/Tools/rocl/>`_ for more information.

      * CI/CD: `Grid CI Screwdriver V4 Cookbook <http://yo/gridci-sdv4-cookbook/>`_

.. _hadoop_team_getting_started_onboarding_checklist:

*********************
New-Members Checklist
*********************

Account Access
==============

Verify ubkey is working
-----------------------

Next step is to verify that your ``ubkey`` is working:

  -  Open terminal
  -  type the following command ``yinit``
  -  you should see prompt message asking you to enter the pin. Use The
     default pin in the output message.
  -  When Asked for ``PKCS-11``: enter the same default key once more.
  -  If asked ``YubiKey for:``, touch and hold the ubkey until the key is
     generated.
  -  Enter your Unix password (the Bouncer password) when you are asked
     for the password.


The previous process should look like the following:


  .. code-block:: bash

    yinit
    Please enter your PIN code for YubiKey when prompted.
    2019/01/30 10:40:33 (Default PIN code for YubiKey is XXXXXX)
    Enter PIN: <ENTER_DEFAULT_PIN=123456>
    2019/01/30 10:40:48 [INFO] Generating new touchless key in hardware......
    2019/01/30 10:40:49 [INFO] Generating new emergency key in hardware......
    Enter passphrase for PKCS#11: <ENTER_DEFAULT_PIN
    Card added: /Library/OpenSC/lib/opensc-pkcs11.so
    YubiKey for: <TOUCH_UBKEY>
    Password: <OKTA_PASSSWORD>


A typical day to day ``ybkey`` operation would look like this:

  .. code-block:: bash

    $ yinit
    Enter passphrase for PKCS#11: <DEFAULT>
    Card added: /Library/OpenSC/lib/opensc-pkcs11.so
    2019/01/31 09:59:01 Refreshing your credentials...
    YubiKey for `djohn`: <TOUCH_YBKEY>
    Password: <UNIX_PASSWORD_AKA_BOUNCER>
    Touch YubiKey: <TOUCH_YBKEY>
    2019/01/31 09:59:20 SSHCA credentials loaded into your ssh-agent.

**Troubleshooting**

If you have any problems initializing your ssh key:

#. First, Contact Sean
#. Reset bouncer password using the following link `http://yo/pw <http://yo/pw>`_
#. Use yo/ubkey to register your ubkey


Verify Access
-------------

#. **slack channels:**

   * `hadoopcoreteam <https://vzbuilders.slack.com/archives/G6MQ07F9P>`_
   * `nroberts_directs <https://vzbuilders.slack.com/archives/GGC5GAPU4>`_

#. **Google Docs, Sheets, and Calendars:**

   * `2020 Hadoop Core / Spark / Jupyter caplabor <https://docs.google.com/spreadsheets/d/1E0QZABFUxHfLvgW3PHgXL9wkvtR0XX4ZCDHn5gonQ1M>`_
   * `Flubber Next Gen <https://docs.google.com/spreadsheets/d/18OaLmkoS7YG_A6Jg554UvcEar15J2brXIxSrRGxYziA>`_
   * Subscribe to Google Calnedar ``Oath Big Data (Grid/ML/Vespa/VI) Out Of Office`` to mark your OOO days.


#. **Github repositories:**

   Make sure that you have access to the following repositories

      - `Internal Hadoop <https://git.ouroath.com/hadoop/Hadoop>`_
      - `Hadoop Configuration <https://git.ouroath.com/hadoop/hadoop_configs>`_
      - `Hadoop Documentation <https://git.ouroath.com/hadoop/docs>`_
      - `Cloud Engineering Group Test Automation Framework (Hadoop/Storm/Spark/GDM) <https://git.ouroath.com/HadoopQE/hadooptest>`_

#. **Create Athenz domain:**

   * go to `yo/athenz <https://yo/athenz>`_
   * if you have nothing listed, then click ``Create``

     .. figure:: /images/team_onboarding/athenz_domain.png
        :alt:  Athenz domain: create personal domain

        Fig Athenz domain: create personal domain

   * select ``personal`` type and add define your domain as ``home.djohn``

#. **Doppler:**

   It takes some time to the permissions to propagate through the system.
   More details can be found on `Grid Onboarding Guide <https://git.ouroath.com/pages/developer/Bdml-guide/Onboarding_to_the_Grid>`_.

   * Go to `yo/doppler <https://yo/doppler>`_
   * click ``Request Verizon VCG Grid Access``
   * click ``Request Verizon MEDIA Grid Access``

#. **Deployment permissions:**

   * Make sure that you already have access to verizonmedia grid. (previous step).
   * Ask Raj to add your userID to get access to `yo/hadoop-deploy <https://yo/hadoop-deploy>`_
   * Ask Raj to add you to the group ``ygrid_netgroup_griddev``


Hadoop Community
=================

* Fill the `individual contributor LA form <https://www.apache.org/licenses/icla.pdf>`_
* create an account on hadoop jira and Subscribe to `the mailing lists <https://hadoop.apache.org/mailing_lists.html>`_


Build and Run Hadoop
====================

Environment Setup
-----------------

Follow the steps in :ref:`hadoop_team_getting_started_development` to achieve the following goals.

#. Setup machine for development (VM and OS X)
#. Get Yahoo Hadoop code
#. Build Hadoop
#. Run Hadoop
#. Launch Hadoop job
#. Execute command line examples from `Bdml_guide CLI <https://git.ouroath.com/pages/developer/Bdml-guide/grid_cline>`_

Picking first Jira
------------------

The team should help new member to pick a Jira.
This will be first task to get familiar with the following:

#. Jira Workflow
#. Development and making changes
#. Testing changes
#. Creating PR
#. Peer Review process
#. Merging code changes

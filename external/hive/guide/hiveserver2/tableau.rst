Tableau Desktop
###############

.. _Tableau_Desktop_Connectivity:

This document contains operating system wise details to connect to HiveSever2 hosted on **YGRID** from Tableau Desktop.

MAC
***

1. Install and Configure the Simba Hive ODBC Driver
******************************************************

**NOTE: This is a one time step**.

#. Download and install the `Simba Hive ODBC Driver for Mac OS X <https://edge.artifactory.ouroath.com:4443/artifactory/ygrid/SimbaODBCDriver/ApacheHive/2.6/Darwin/>`_,
as per `Simba's documentation <https://www.simba.com/products/Hive/doc/ODBC_InstallGuide/mac/content/odbc/macosx/install.htm>`_, i.e.

    - Double-click ``SimbaHiveODBC.dmg`` to mount the disk image.
    - Double-click ``SimbaHiveODBC.pkg`` to run the installer.
    - In the installer, click ``Continue``.
    - On the ``Software License Agreement`` screen, click ``Continue``, and when the prompt appears, click Agree, then ``Install``.

The driver files will be installed under **/Library/simba/hive**.

#. Download the `Simba Hive ODBC Driver Mac License <https://drive.google.com/open?id=1hg9nHrB4FEmMQXtL_lY3yasYSsiAZ3zm>`_
and save it as **/Library/simba/hiveodbc//lib/SimbaApacheHiveODBCDriver.lic**. (You will need *root* privileges.)

The driver should now be ready for use.


2. Authentication
*****************

Authentication to HiveServer2 can be done using mutual TLS with
`Athenz <https://git.ouroath.com/pages/athens/athenz-guide>`_ X.509 user or role certificates.

Users running Tableau Desktop on the Mac Laptop, will have to fetch the ``griduser.uid.<username>``
role certificates daily before accessing HiveServer2. Download the latest release of `athenz-user-cert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/athenz-user-cert/>`_
and `zts-rolecert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-rolecert/>`_ scripts for the ``Darwin`` operating system.

.. code-block:: text

  # One time setup. Download athenz CLI utilities and truststore file. Execute following commands on terminal
  curl -o /usr/local/bin/zts-rolecert "https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-rolecert/1.26/Darwin/zts-rolecert"
  curl -o /usr/local/bin/athenz-user-cert "https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/athenz-user-cert/1.4.9/Darwin/athenz-user-cert"
  chmod +x /usr/local/bin/zts-rolecert /usr/local/bin/athenz-user-cert
  rsync -avz jet-gw.blue.ygrid.yahoo.com:/home/y/share/ssl/certs/yahoo_certificate_bundle.pem ${HOME}/.athenz/

  # Run once a day before using the BI tool to renew the expired user certificate and role certificate
  yinit
  athenz-user-cert
  zts-rolecert -svc-key-file ${HOME}/.athenz/key -svc-cert-file ${HOME}/.athenz/cert -zts https://zts.athens.yahoo.com:4443/zts/v1 -role-domain griduser -role-name uid.${USER} -dns-domain zts.yahoo.cloud -role-cert-file  ${HOME}/.athenz/griduser.uid.${USER}.cert.pem
  # If you get a permission error for above command, remove the ${HOME}/.athenz/griduser.uid.${USER}.cert.pem file (command:  rm ${HOME}/.athenz/griduser.uid.${USER}.cert.pem) and re run the above command.
  openssl x509 -in ${HOME}/.athenz/griduser.uid.${USER}.cert.pem -text | less

Your role cert is in ${HOME}/.athenz/griduser.uid.${USER}.cert.pem

${HOME} indicates home directory for Mac, also referred as **~/**.

${USER} indicates current user's username.


3. Update odbc.ini
******************

You need to update cluster specific settings in the .odbc.ini file in your home directory to connect to a specific cluster.

.. code-block:: text

  # One time step. The python script is a helper to update the ~/.odbc.ini file for specific cluster.
  Download the python script :download:`update_ygrid_odbc_ini.py <files/update_ygrid_odbc_ini.py>`

  # install configparser for python
  pip install configparser

  # Run the script with the cluster abbreviation to which you want to connect as argument
  # The following command will update the odbc.ini with JetBlue (JB) connection settings
  python update_ygrid_odbc_ini.py JB

**NOTE: The python script  is tested with python 2.7**.

3. Run Tableau Desktop
**********************

- Run Tableau Desktop
- In **Connect** menu, select **Other Databases(ODBC)** option under **To a server**
- Select the cluster HS2 mTLS connection under DSN. **JetBlue HS2 mTLS** in this case.
  .. image:: images/tableau_desktop_connection.png
     :height: 516px
     :width: 883px
     :scale: 80%
     :alt:
     :align: left
- Click **Sign in**

You will be able to access HiveServer2 database/schemas/tables from Tableau Desktop now.
Authentication
##############

The following modes of authentication are supported to the Presto Coordinator.

  - Kerberos
  - mTLS with Athenz X.509 User or Role Certificate
  - Okta (Browser and UI)

Kerberos
********

`Kerberos <https://en.wikipedia.org/wiki/Kerberos_(protocol)>`_ authentication is
the key authentication mechanism in Hadoop and is supported by all grid services.

The first step involves getting a Kerberos TGT for the Kerberos principal.

   - If you are a regular user, you can authenticate using password to the Y.CORP.YAHOO.COM which is a domain trusted by YGRID.YAHOO.COM and VCG.OUROATH.COM KDC domains.
   - If you are running as a headless user, use keytabs generated for the headless user to authenticate to the YGRID.YAHOO.COM or VCG.OUROATH.COM KDC domains.
   - Both regular and headless users can authenticate to KDC using Athenz X.509 certificates via `pkinit <https://yo/pkinit>`_.

The Y.CORP.YAHOO.COM support is deprecated and will be EOL in Q2 2020. Users newly
joining Verizon Media, do not have accounts created there anymore. For security reasons,
use of keytabs for authenticating headless users will also be stopped by end of Q1 2020. So all users
will have to migrate to using `pkinit <https://yo/pkinit>`_ for Kerberos authentication.

kinit
=====

Note: Below methods using passwords or keytabs is only supported till end of Q1 2020.

To kinit and get TGT as yourself, run ``kinit`` and enter your Unix password::

       $ kinit $USER@Y.CORP.YAHOO.COM

To kinit as the headless user after ``sudo`` on a gateway or launcher box in YGRID clusters::

       $ kinit -kt ~/`whoami`.prod.headless.keytab `whoami`@YGRID.YAHOO.COM

To kinit as the headless user after ``sudo`` on a gateway or launcher box in VCG clusters::

       $ kinit -kt ~/`whoami`.prod.headless.keytab `whoami`@VCG.OUROATH.COM

You can check the validity and other details of the TGT fetched by running::

       $ klist

kinit using pkinit
==================

For regular users, run the `pkinit-user <https://docs.google.com/document/d/1Xtxahbf0Z9m9fGUHQVj4IItEElex024aR-TaETPmSx0/edit#heading=h.vjyhzksc16rt>`_
wrapper script that fetches Athenz user certificate and also does ``kinit`` and gets the user TGT for YGRID.YAHOO.COM or VCG.OUROATH.COM domain::

       $ pkinit-user; klist

For headless users, pkinit requires one time setup of Athenz roles in
`griduser <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ or
`vcg.user <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ domains before ``pkinit-user`` can be run.
For setup instructions , please refer to
`pkinit <https://docs.google.com/document/d/1Xtxahbf0Z9m9fGUHQVj4IItEElex024aR-TaETPmSx0/edit#heading=h.4sc36kaimeaw>`_ documentation.

Once the role is setup, doing ``sudo`` as the headless user on the gateway will automatically fetch ``~/.athenz/griduser.role.uid.`whoami`.{cert,key}.pem`` or ``~/.athenz/vcg.user.role.uid.`whoami`.{cert,key}.pem`` files based on the domain.
The ``pkinit-user`` wrapper script should then be run to do ``kinit``.

CLI
===

Please refer to :doc:`Executing queries using CLI from gateway or launcher <connectivity/cli>` for more details and examples.

The CLI options pertaining to Kerberos are:

+-----------------------------------------------------------+------------------------------------------------------------------------+
| CLI option                                                | Description                                                            |
+===========================================================+========================================================================+
| --krb5-config-path <krb5 config path>                     | Kerberos config file path (default: /etc/krb5.conf)                    |
+-----------------------------------------------------------+------------------------------------------------------------------------+
| --krb5-credential-cache-path <krb5 credential cache path> | Kerberos credential cache path (default: /tmp/krb5cc_$UID)             |
+-----------------------------------------------------------+------------------------------------------------------------------------+
| --krb5-disable-remote-service-hostname-canonicalization   | Disable service hostname canonicalization using the DNS reverse lookup |
+-----------------------------------------------------------+------------------------------------------------------------------------+
| --krb5-keytab-path <krb5 keytab path>                     | Kerberos keytab file path (default: /etc/krb5.keytab)                  |
+-----------------------------------------------------------+------------------------------------------------------------------------+
| --krb5-principal <krb5 principal>                         | Kerberos principal to be used                                          |
+-----------------------------------------------------------+------------------------------------------------------------------------+
| --krb5-remote-service-name <krb5 remote service name>     | Remote peer's kerberos service name (default: HTTP)                    |
+-----------------------------------------------------------+------------------------------------------------------------------------+

The default authentication for CLI is Kerberos. For execution on gateways and launchers
after ``kinit``, users do not have to specify any of the above options as default values will hold good.
If you do ``kinit`` on the command line and then run the Presto CLI, it
automatically picks the TGT from ``/tmp/krb5cc_$UID`` and authenticates as that user principal.

JDBC
====

Please refer to :doc:`Presto JDBC <connectivity/jdbc>` documentation for more details and examples.

The JDBC properties pertaining to Kerberos are:

+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| Name                         | Description                                                                                                                                                                                                                   | |
+==============================+===============================================================================================================================================================================================================================+=+
| KerberosRemoteServiceName    | Presto coordinator Kerberos service name. This parameter is required for Kerberos authentication                                                                                                                              | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| KerberosUseCanonicalHostname | Use the canonical hostname of the Presto coordinator for the Kerberos service principal by first resolving the hostname to an IP address and then doing a reverse DNS lookup for that IP address. This is enabled by default. | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| KerberosConfigPath           | Kerberos configuration file                                                                                                                                                                                                   | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| KerberosPrincipal            | The principal to use when authenticating to the Presto coordinator                                                                                                                                                            | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| KerberosKeytabPath           | Kerberos keytab file                                                                                                                                                                                                          | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| KerberosCredentialCachePath  | Kerberos credential cache                                                                                                                                                                                                     | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+


Kerberos keytabs will be disabled by end of Q1 2020. So please migrate from using
``KerberosKeytabPath`` in JDBC to :ref:`X.509 certificates <jdbc_x509_auth>`.

.. _x509_auth:

Athenz X.509 Certificate
************************

Authentication to Presto can be done using
`mutual TLS <https://git.ouroath.com/pages/athens/athenz-guide/mtls/>`_ with
`Athenz <https://git.ouroath.com/pages/athens/athenz-guide>`_ X.509 user or role certificates.
Authentication from the following roles are supported.

  - ``user.<regular_user_name>``
  - `griduser.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ (YGRID only)
  - `griduser.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ (YGRID only)
  - `vcg.user.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ (VCG only)
  - `vcg.user.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ (VCG only)

User certificate
================
Refer to `Athenz User X.509 Certificates <https://git.ouroath.com/pages/athens/athenz-guide/user_x509_credentials>`_
documentation for fetching ``user.<regular_user_name>`` user certificate. User certificates are valid for only one hour.

Role certificate
================
Role certificates from `griduser <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ and
`vcguser <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ domain are accepted. The naming convention of the role is ``uid.<username>``.
For regular users, roles are already created and ``user.username`` is added to the role.

1. Please follow steps in
   `Creating Athenz Roles for Grid Authentication <https://docs.google.com/document/d/1fUziPmsB-QALJtqQ6QZ9xf18n6mLOqRHasR9Ru7hXMg/edit>`_
   to create the Athenz role for headless user.
   After that you can add user principals or Athenz services to the newly created role.
2. Refer to `Athenz X.509 Role Certificates <https://git.ouroath.com/pages/athens/athenz-guide/zts_rolecert>`_
   documentation for fetching role certificates using the Athenz service certificate and key.
   Role certificates are currently valid for 30 days and will have to be refreshed once they expire.
   The validity will be reduced to 1 day for these domains in Feb 2020.

Linux
-----
On Linux hosts, Athenz team provides the ``hca`` utility to automatically fetch and rotate the role certificates.
Please refer to `Calypso <https://git.ouroath.com/pages/athens/calypso-guide/role_certs/>`_ documentation for instructions to set that up.
You can refer to `pkinit <https://docs.google.com/document/d/1Xtxahbf0Z9m9fGUHQVj4IItEElex024aR-TaETPmSx0/edit#heading=h.wlol3rcp9417>`_
documentation for an example config of ``/etc/sia/sia_config`` to setup fetching role certificates for ``griduser.uid`` role.

Windows
-------
Athenz team supports fetching user and role certificates in Windows hosts as well.
You can find the download links below

- `athenz-user-cert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/athenz-user-cert/1.5.1/Windows/>`_
- `zts-svccert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-svccert/1.30/Windows/>`_
- `zts-rolecert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-rolecert/1.30/Windows/>`_

The ``hca`` utility is not supported on Windows. For Tableau servers running on Windows,
the fetching of service and role certs will have to be automated by setting up
a scheduled task using `Windows Task Scheduler <https://docs.microsoft.com/en-us/windows/win32/taskschd/task-scheduler-start-page>`_
or `Powershell <https://docs.microsoft.com/en-us/windows/win32/taskschd/schtasks>`_
to run ``zts-svccert`` and ``zts-rolecert`` commands periodically.

Mac
---
Users running BI tools (Tableau, DbVisualizer, etc) on the Mac Laptop, will have to fetch the ``griduser.uid.<username>``
role certificates daily before accessing Presto. Please download the latest release of `athenz-user-cert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/athenz-user-cert/>`_
and `zts-rolecert <https://artifactory.ouroath.com/artifactory/simple/core-tech/releases/zts-rolecert/>`_ scripts for the ``Darwin`` operating system.
We have provided two scripts below to make the process easier and they are applicable for connecting to HiveServer2 as well.

.. _mac_onetime:

One time setup
^^^^^^^^^^^^^^

1. Please :download:`download <connectivity/files/macOS_ygrid_mtls_onetime_setup.sh>` the Athenz utilities install script.
   Or you can copy the contents below and save it to a file.

   .. literalinclude:: connectivity/files/macOS_ygrid_mtls_onetime_setup.sh

2. After downloading, ``cd`` to the directory where you saved the file and then invoke it.

   .. code-block:: text

      # Replace Downloads with the directory you saved the file to
      cd $HOME/Downloads
      bash -v macOS_ygrid_mtls_onetime_setup.sh

The script does the following:

1. Downloads ``athenz-user-cert`` and ``zts-role-cert`` to ``/usr/local/bin``.
   For users with restricted access on macOS, ``/usr/local/bin`` is not writable and in those cases, they are downloaded
   to ``${HOME}/athenz/bin``.

   If you are on macOS Catalina, you might run into below error

   .. code-block:: text

      "athenz-user-cert" cannot be opened because the developer cannot be verified.

      macOS cannot verify that this app is free from malware.

   To get past the error and allow ``athenz-user-cert`` and ``zts-role-cert`` that we downloaded to ``${HOME}/athenz/bin`` to be run,
   follow instructions in `Mac Help <https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac>`_
   and add them as an exception. We are raising the issue with Athenz team and Corp IT, so that ``athenz-user-cert`` and ``zts-role-cert``
   are installed on the laptops similar to ``yinit`` or available via ``Self Service`` and users don't have to go
   through above steps in future.

2. Copies ``yahoo_certificate_bundle.pem`` truststore file from JB gateway which is a part of
`yahoo_certificate_bundle dist package <https://dist.corp.yahoo.com/by-package/yahoo_certificate_bundle/>`_.

.. _mac_daily:

Daily setup
^^^^^^^^^^^

1. Please :download:`download <connectivity/files/macOS_ygrid_mtls_cert_refresh.sh>` the Athenz mTLS certificate refresh script.
   Or you can copy the contents below and save it to a file.

   .. literalinclude:: connectivity/files/macOS_ygrid_mtls_cert_refresh.sh

2.  After downloading, ``cd`` to the directory where you saved the file and then invoke it.

    .. code-block:: text

      # Replace Downloads with the directory you saved the file to
      cd $HOME/Downloads
      bash -v macOS_ygrid_mtls_cert_refresh.sh

   **Important**: This script has to be run once daily before connecting to Presto or HiveServer2.
   Please make sure that you see no errors when the script is run.

The script does the following:

1. Generates the Athenz user cert using SSH CA (yubikey).
2. Generates the Athenz role cert and key in PEM format for ``griduser.uid.<username>`` role
   which will be used to authenticate to Grid services like Presto and Hive Server 2.
3. Creates a keystore in JKS format using the role cert and key which is needed for JDBC connections to HiveServer2.
   Internal Presto JDBC driver supports both PEM and JKS formats and so the PEM file can be used directly.
   The JKS keystore is only required if the open source Presto JDBC driver is used which we do not recommend.

CLI
===

Please refer to :doc:`Executing queries using CLI from gateway or launcher <connectivity/cli>` for more details and examples.

The CLI options pertaining to X.509 certificate authentication are:

+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| CLI option                                  | Description                                                                                                                                                       |
+=============================================+===================================================================================================================================================================+
| --certificate-path <certificate path>       | The location of the certificate file in PEM format that contains the certificate to use for authentication                                                        |
+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| --keystore-path <keystore path>             | The location of the key file in PEM format (or) the Java KeyStore file in JKS format that contains both the certificate and private key to use for authentication |
+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| --keystore-password <keystore password>     | The password for the KeyStore                                                                                                                                     |
+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| --truststore-path <truststore path>         | The location of the Java TrustStore file that will be used to validate HTTPS server certificates                                                                  |
+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| --truststore-password <truststore password> | Kerberos principal to be used                                                                                                                                     |
+---------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+

For the truststore, `yahoo_certificate_bundle <https://dist.corp.yahoo.com/by-package/yahoo_certificate_bundle/>`_ dist package has to be installed.

You can invoke the Presto CLI using certificate authentication as below::

       $ presto --certificate-path /var/lib/sia/certs/griduser.role.uid.`whomai`.cert.pem --keystore-path /var/lib/sia/certs/griduser.role.uid.`whomai`.key.pem --truststore-path /home/y/share/ssl/certs/yahoo_certificate_bundle.pem


.. _jdbc_x509_auth:

JDBC
====

Please refer to :doc:`Presto JDBC <connectivity/jdbc>` documentation for more details and examples.

The JDBC options pertaining to X.509 certificate authentication are:

+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Name                  | Description                                                                                                                                                       |
+=======================+===================================================================================================================================================================+
| SSLCertificatePath    | The location of the certificate file in PEM format that contains the certificate to use for authentication                                                        |
+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SSLKeyStorePath       | The location of the key file in PEM format (or) the Java KeyStore file in JKS format that contains both the certificate and private key to use for authentication |
+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SSLKeyStorePassword   | The password for the KeyStore                                                                                                                                     |
+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SSLTrustStorePath     | The location of the file containing trusted certificate authorities in PEM format or JKS format that will be used to validate HTTPS server certificates           |
+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| SSLTrustStorePassword | The password for the TrustStore                                                                                                                                   |
+-----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------+

Example of JDBC properties that will have to be added to connect using certificate authentication:

.. code-block:: text

  # Please replace all occurrences of <username> with your username or headless username
  # Linux
  SSL=true
  SSLCertificatePath=/var/lib/sia/certs/griduser.role.uid.<username>.cert.pem
  SSLKeyStorePath=/var/lib/sia/keys/griduser.role.uid.<username>.key.pem
  SSLTrustStorePath=/home/y/share/ssl/certs/yahoo_certificate_bundle.pem

  # Mac
  SSL=true
  SSLCertificatePath=/Users/<username>/.athenz/griduser.uid.<username>.cert.pem
  SSLKeyStorePath=/Users/<username>/.athenz/griduser.uid.<username>.key.pem
  SSLTrustStorePath=/Users/<username>/.athenz/yahoo_certificate_bundle.pem

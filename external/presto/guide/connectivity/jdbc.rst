JDBC
####

Presto queries can be run programmatically from Java using the JDBC driver.


Maven Dependency
****************
The driver is available from both ``ymaven`` and ``artifactory``. Please use
``artifactory`` as ``ymaven`` will be EOL in 2020.


Snippet of pom.xml for artifactory:

.. code-block:: text

  <repositories>
    <repository>
      <id>maven-release</id>
        <url>https://artifactory.ouroath.com:4443/artifactory/maven-release</url>
        <snapshots>
          <enabled>false</enabled>
        </snapshots>
    </repository>
  </repositories>
  ....
  <dependency>
    <groupId>com.facebook.presto</groupId>
    <artifactId>presto-jdbc</artifactId>
    <version>0.204.3.2.1911111904</version>
  </dependency>

The direct URL for the jar in the above example is
`https://artifactory.ouroath.com/artifactory/maven-release/com/facebook/presto/presto-jdbc/0.204.3.2.1911111904/ <https://artifactory.ouroath.com/artifactory/maven-release/com/facebook/presto/presto-jdbc/0.204.3.2.1911111904/>`_.

Snippet of pom.xml for ymaven:

.. code-block:: text

  <repositories>
    <repository>
      <id>yahoo</id>
        <url>https://ymaven.corp.yahoo.com:9999/proximity/repository/public</url>
        <snapshots>
          <enabled>false</enabled>
        </snapshots>
    </repository>
  </repositories>
  ....
  <dependency>
    <groupId>yahoo.yinst.presto_client</groupId>
    <artifactId>presto-jdbc</artifactId>
    <version>0.204.3.2.1911111904</version>
  </dependency>

You can also install the dist package and add ``/home/y/libexec/presto_client/lib/presto-jdbc.jar``
to the class path of your Java application instead of bundling the jar with maven assembly.

You can find the stable internal presto version to use from
`dist <https://dist.corp.yahoo.com/by-package/presto_client/>`_ or use the version
shown on the Presto UI for that cluster.

Connection URL
**************

The following JDBC URL formats are supported:

.. code-block:: text

  jdbc:presto://host:port
  jdbc:presto://host:port/catalog
  jdbc:presto://host:port/catalog/schema

Parameter Reference
*******************

+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| Name                         | Description                                                                                                                                                                                                                   | |
+==============================+===============================================================================================================================================================================================================================+=+
| user                         | Username to use for authentication and authorization                                                                                                                                                                          | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSL                          | Use HTTPS for connections                                                                                                                                                                                                     | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLCertificatePath           | The location of the certificate file in PEM format that contains the certificate to use for authentication                                                                                                                    | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLKeyStorePath              | The location of the key file in PEM format (or) the Java KeyStore file in JKS format that contains both the certificate and private key to use for authentication                                                             | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLKeyStorePassword          | The password for the KeyStore                                                                                                                                                                                                 | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLTrustStorePath            | The location of the file containing trusted Certificate Authorities in PEM format or JKS format that will be used to validate HTTPS server certificates                                                                       | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLTrustStorePassword        | The password for the TrustStore                                                                                                                                                                                               | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
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

Kerberos
********

Below is a code sample for connecting using Kerberos authentication.

.. code-block:: text

  // URL parameters for ygrid
  String url = "jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443/dilithiumblue/benzene?user=p_search&SSL=true&KerberosRemoteServiceName=HTTP&KerberosUseCanonicalHostname=false&KerberosConfigPath=/etc/krb5.conf&KerberosPrincipal=p_search&KerberosKeytabPath=/homes/p_search/p_search.prod.headless.keytab";

  // URL parameters for VCG
  // String url = "jdbc:presto://hothgq-presto.gq.vcg.yahoo.com:4443/kessel/vcghivedb?user=p_search&SSL=true&KerberosRemoteServiceName=HTTP&KerberosUseCanonicalHostname=false&KerberosConfigPath=/etc/krb5.conf&KerberosPrincipal=p_search&KerberosKeytabPath=/homes/p_search/p_search.prod.headless.keytab";
  Connection connection = DriverManager.getConnection(url);

  // Using properties
  String url = "jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443/dilithiumblue/benzene";
  Properties properties = new Properties();
  properties.setProperty("user", "p_search");
  properties.setProperty("SSL", "true");
  properties.setProperty("KerberosRemoteServiceName", "HTTP");
  properties.setProperty("KerberosUseCanonicalHostname", "false");
  properties.setProperty("KerberosConfigPath", "/etc/krb5.conf");
  properties.setProperty("KerberosPrincipal", "p_search");
  properties.setProperty("KerberosKeytabPath", "/homes/p_search/p_search.prod.headless.keytab");
  Connection connection = DriverManager.getConnection(url, properties);

You can set ``KerberosCredentialCachePath`` instead of ``KerberosKeytabPath`` if
you want to run as a regular user and test. Kerberos keytabs will be disabled by end of Q1 2020.
So please migrate to using Athenz X.509 certificates for authentication.

X.509
*****
Authentication to Presto can be done using mutual TLS with
`Athenz <https://git.ouroath.com/pages/athens/athenz-guide>`_ X.509 role certificates.
Refer to :ref:`X.509 certificate authentication <x509_auth>` for detailed information on the initial setup required.
Authentication from the following roles are supported.

  - ``user.<regular_user_name>``
  - `griduser.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ (YGRID only)
  - `griduser.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>`_ (YGRID only)
  - `vcg.user.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ (VCG only)
  - `vcg.user.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>`_ (VCG only)

``SSLCertificatePath``, ``SSLKeyStorePath`` and ``SSLTrustStorePath`` can be used to specify the
location of ``.pem`` or ``.jks`` files containing the X.509 role cert, private
key and trusted CAs to validate the server for mutual TLS.
In case of ``.jks`` files, ``SSLKeyStorePassword`` and ``SSLTrustStorePassword`` will also have to be specified.

Please do yinst install of `yahoo_certificate_bundle <https://dist.corp.yahoo.com/by-package/yahoo_certificate_bundle/>`_
package from dist. This package contains the trust store files.

For example:

.. code-block:: text

  // Example for setting up JDBC connection using cert and key generated by sia in JDBC properties
  String url = "jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443/dilithiumblue/benzene";
  Properties properties = new Properties();
  properties.setProperty("user", "p_search");
  properties.setProperty("SSL", "true");
  properties.setProperty("SSLCertificatePath", "/var/lib/sia/certs/griduser.role.uid.p_search.cert.pem");
  properties.setProperty("SSLKeyStorePath", "/var/lib/sia/certs/griduser.role.uid.p_search.key.pem");
  properties.setProperty("SSLTrustStorePath", "/home/y/share/ssl/certs/yahoo_certificate_bundle.pem"); // From yahoo_certificate_bundle dist package
  Connection connection = DriverManager.getConnection(url, properties);

  // Example for configuring JDBC properties if you want to refer to a JKS file instead of PEM
  Properties properties = new Properties();
  properties.setProperty("user", "p_search");
  properties.setProperty("SSL", "true");
  properties.setProperty("SSLKeyStorePath", "/homes/p_search/griduser_role_cert_and_key.jks");
  // changeit is the default password. If you generated the keystore with a different password, specify that value.
  properties.setProperty("SSLKeyStorePassword", "changeit");
  properties.setProperty("SSLTrustStorePath", "/home/y/share/ssl/certs/yahoo_certificate_bundle.jks"); // From yahoo_certificate_bundle dist package
  // changeit is the actual password for yahoo_certificate_bundle.jks.
  properties.setProperty("SSLTrustStorePassword", "changeit");

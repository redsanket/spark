JDBC
####

Presto can be accessed from Java using the JDBC driver.

The driver is available from ``ymaven``:

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
    <groupId>yahoo.yinst.bouncer_auth_java</groupId>
    <artifactId>presto_client</artifactId>
    <version>0.204.2.1.1905172013</version>
  </dependency>

You can find the current internal presto version to use from
`dist <https://dist.corp.yahoo.com/by-package/presto_client/>`_. You can also
install the dist package and add ``/home/y/libexec/presto_client/lib/presto-jdbc.jar``
to the class path of your Java application instead of bundling the jar with maven.

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
| SSLKeyStorePath              | The location of the Java KeyStore file that contains the certificate and private key to use for authentication                                                                                                                | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLKeyStorePassword          | The password for the KeyStore                                                                                                                                                                                                 | |
+------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-+
| SSLTrustStorePath            | The location of the Java TrustStore file that will be used to validate HTTPS server certificates                                                                                                                              | |
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

  // URL parameters
  String url = "jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443/dilithiumblue/benzene?user=p_search&SSL=true&KerberosRemoteServiceName=HTTP&KerberosUseCanonicalHostname=false&KerberosConfigPath=/etc/krb5.conf&KerberosPrincipal=p_search&KerberosKeytabPath=/homes/p_search/p_search.prod.headless.keytab";
  For VCG
  
  //String url = "jdbc:presto://hothgq-presto.gq.vcg.yahoo.com:4443/kessel/vcghivedb?user=p_search&SSL=true&KerberosRemoteServiceName=HTTP&KerberosUseCanonicalHostname=false&KerberosConfigPath=/etc/krb5.conf&KerberosPrincipal=p_search&KerberosKeytabPath=/homes/p_search/p_search.prod.headless.keytab";
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
you want to run as a regular user and test.

X.509
*****
Authentication to Presto can be done using mutual TLS with
`Athenz <https://git.ouroath.com/pages/athens/athenz-guide>`_ X.509 role certificates.
Authentication from the following roles are supported.

  - ``user.<regular_user_name>``
  - `_`griduser.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>``_
  - `_`griduser.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/griduser/role>``_
  - `_`vcg.user.uid.<regular_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>``_
  - `_`vcg.user.uid.<headless_user_name> <https://ui.athenz.ouroath.com/athenz/domain/vcg.user/role>``_

Refer `Athenz User X.509 Certificates <https://git.ouroath.com/pages/athens/athenz-guide/user_x509_credentials>`_
for fetching ``user.<regular_user_name>`` user role certificate.
User role certificates are valid for only one hour.

Refer `Athenz X.509 Role Certificates <https://git.ouroath.com/pages/athens/athenz-guide/zts_rolecert>`_
for fetching other role certificates. Role certificates are currently valid for
30 days, but can be reduced to 7 days.

``SSLKeyStorePath`` and ``SSLKeyStorePassword`` can be used to specify the
location of ``.pem`` or ``.jks`` file containing the X.509 role cert and private
key for mutual TLS.

For example:

.. code-block:: text

  String url = "jdbc:presto://xandarblue-presto.blue.ygrid.yahoo.com:4443/dilithiumblue/benzene";
  Properties properties = new Properties();
  properties.setProperty("user", "p_search");
  properties.setProperty("SSL", "true");
  properties.setProperty("SSLKeyStorePath", "/homes/p_search/griduser_role_cert.pem");
  properties.setProperty("SSLKeyStorePassword", "changeit");
  Connection connection = DriverManager.getConnection(url, properties);



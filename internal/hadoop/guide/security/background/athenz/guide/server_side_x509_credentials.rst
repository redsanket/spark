*******************************************
Server Side Service Identity Authentication
*******************************************


In order to authenticate x.509 client certificates on Athenz enabled
services, your server needs to be configured to authenticate TLS client
certificates along with updated trust store with Athenz CA certificates.
This section contains some examples how to setup your server to enable
TLS client certificate authentication.

If you are looking for an instruction for a specific container such as
undertow or jetty, please see
`mtls container support <https://git.vzbuilders.com/pages/athens/athenz-guide/mtls/#container-support>`_

Get Athenz CA Certificates
--------------------------

First, download Athenz CA certificates following
`Athenz CA Certificate Details <https://git.vzbuilders.com/pages/athens/athenz-guide/athenz_ca_certs/>`_ section. The preferred
option is to choose the ``yahoo_certificate_bundle`` rpm or dist package
since that already includes a couple of truststores in PEM, PKCS12 and
JKS formats - one including only the Athenz CA certificates and another
that includes Athenz CA certificates along with other public CA
certificates that we obtain certificates from.

  .. code:: bash

     athenz_certificate_bundle.jks
     athenz_certificate_bundle.pem
     athenz_certificate_bundle.pkcs12
     yahoo_certificate_bundle.jks
     yahoo_certificate_bundle.pem
     yahoo_certificate_bundle.pkcs12

You must configure your container to recognize one of these trust
stores.

If you are using other containers, you can set the SSL properties at the
JVM level via system properties.

.. code:: bash

   -Djavax.net.ssl.trustStore=/home/y/share/ssl/certs/yahoo_certificate_bundle.pkcs12

If the ``yahoo_certificate_bundle`` does not satisfy your requirements,
you can use the ``keytool`` utility to add Athenz CA certificates to
your truststore.

For example, if you are using the default JVM trust store, the following
command will add given the Athenz AWS us-west-2 region CA certificate to
the truststore:

.. code:: bash

   sudo keytool -importcert -alias athenz_aws_uswest2 \
                -keystore $JAVA_HOME/jre/lib/security/cacerts \
                -storepass changeit -file oath_aws_us_west_2_ca.pem

When prompted, type yes to trust the certificate and verify that the
operation is completed successfully:

.. code:: java

   Trust this certificate? [no]: yes
   Certificate was added to keystore

Above steps must be completed for all of Athenz CA certificates and each
CA certificate must have a unique alias. If you are NOT using the
default JVM trust store, change the -keystore path as needed.

Enable TLS client authentication in your container
--------------------------------------------------

If you are using your own application server, you would need to set
setNeedClientAuth to true. For example:

.. code:: java

   SSLServerSocketFactory ssf = sc.getServerSocketFactory();
   SSLServerSocket sslserversocket =
       (SSLServerSocket) ssf.createServerSocket(4443);
   sslserversocket.setNeedClientAuth(true);

If you need a specific container support, please checkout out
`mtls container-support <https://git.vzbuilders.com/pages/athens/athenz-guide/mtls/#container-support>`_

All other containers, please follow specific documentations provided by
your container vendor on how to require client side TLS authentication.

Extract Certificate and Verify
------------------------------

If your trust store only has Athenz CA certificates, no need to extract
and verify the issuer. If not, please follow below code example for
verification.

The client certificate is accessible from
``javax.servlet.request.X509Certificate`` HttpServletRequest attribute.
Here is how you can get access to the TLS certificate:

.. code:: java

   import java.security.cert.X509Certificate;
   import javax.servlet.http.HttpServletRequest;
   public static final String JAVAX_CERT_ATTR =
      "javax.servlet.request.X509Certificate";

   X509Certificate[] certs =
      (X509Certificate[]) servletRequest.getAttribute(JAVAX_CERT_ATTR);
   X509Certificate x509cert = null;
   if (null != certs && certs.length != 0) {
       for (X509Certificate cert: certs) {
           if (null != cert) {
               //find the first occurrence of non-null certificate
               x509cert = cert;
               break;
           }
       }
   }

Then, validate the certificate issuers against a pre-configured set of
Athenz CA issuers - the list of Athenz CA issuers is documented in the
`Athenz CA Certificate Details <https://git.vzbuilders.com/pages/athens/athenz-guide/athenz_ca_certs/>`_
section:

You may use the following Servlet Filter to validate the issuers. See
`oath_mtls_filter <https://git.vzbuilders.com/JavaPlatform/oath_mtls_filter>`_

Below is an `example code <https://git.vzbuilders.com/JavaPlatform/oath_mtls_filter/blob/master/src/main/java/com/oath/filter/mtls TLSFilter.java>`_ from the
`oath_mtls_filter <https://git.vzbuilders.com/JavaPlatform/oath_mtls_filter>`_ that demonstrates how
this done.



.. code:: java

   private static final String DEFAULT_ISSUERS_FILE_NAME =
       "/home/y/share/ssl/certs/athenz_certificate_bundle.jks";
   private static Set<String> X509_ISSUERS = new HashSet<>();

   X509Certificate[] certs =
      (X509Certificate[]) request.getAttribute(JAVAX_CERT_ATTR);
   X509Certificate x509cert = null;
   if (null != certs && certs.length != 0) {
       for (X509Certificate cert: certs) {
           if (null != cert) {
               //find the first occurrence of none null certificate
               x509cert = cert;
               if (LOG.isDebugEnabled()) {
                   LOG.debug("Found x509 cert");
               }
               break;
           }
       }
   }

   if (null == x509cert) {
       // fail as x509cert is missing
       LOG.error("x509 certificate is missing");
       response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
       return;
   }

   // validate the certificate against CAs
   X500Principal issuerx500Principal = x509cert.getIssuerX500Principal();
   String issuer = issuerx500Principal.getName();
   if (LOG.isDebugEnabled()) {
       LOG.debug("Found x509 cert issuer: {}", issuer);
   }
   //example: CN=Athenz AWS CA,OU=us-west-2,O=Oath Inc.,L=Sunnyvale,ST=CA,C=US
   if (issuer == null || issuer.isEmpty()
           || !X509_ISSUERS.contains(issuer)) {
       //fail
       LOG.error("Issuer is missing or not apart of authorized Athenz CA");
       response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
       return;
   }

   private final void setX509CAIssuers(final String issuersFileName) {
       if (issuersFileName == null || issuersFileName.isEmpty()) {
           return;
       }
       try {
           Path path = Paths.get(issuersFileName);
           if (!path.isAbsolute()) {
               path = Paths.get(getClass().getClassLoader().getResource(issuersFileName).toURI());
           }

           KeyStore ks = null;
           try (InputStream in = new FileInputStream(path.toString())) {
               ks = KeyStore.getInstance(KeyStore.getDefaultType());
               ks.load(in, null);
           }
           for (Enumeration<?> e = ks.aliases(); e.hasMoreElements(); ) {
               String alias = (String)e.nextElement();
               X509Certificate cert = (X509Certificate)ks.getCertificate(alias);
               X500Principal issuerx500Principal = cert.getIssuerX500Principal();
               String issuer = issuerx500Principal.getName();
               X509_ISSUERS.add(issuer);
               if (LOG.isDebugEnabled()) {
                   LOG.debug("issuer: {} " , issuer);
               }
           }
       } catch (Throwable e) {
           LOG.error("Unable to set issuers from file " + issuersFileName, e);
       }
   }

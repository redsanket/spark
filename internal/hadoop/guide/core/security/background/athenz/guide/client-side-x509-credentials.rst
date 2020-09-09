*******************************************
Client Side Service Identity Authentication
*******************************************

In order to contact Athenz Services (ZMS/ZTS) or other Athenz Enabled
services, your client needs to establish a HTTPS connection using its
Athenz issued x.509 certificate. This section contains some examples how
to utilize Athenz x.509 certificates for service authentication

In the following set of examples we’re going to assume that the service
has already obtained its x.509 certificate from Athenz.

ZMS Client
==========

We’re going to use our ZMS Java client to communicate with ZMS running
in AWS to carry out a centralized access check to see if principal
``user.john`` has ``read`` access to ``sports:nhl-scores`` resource.

First we need to update our Java project ``pom.xml`` file to indicate
our dependency on the ZMS Java Client and Certificate Refresh Helper
libraries

  .. code-block:: xml

     <dependencies>
       <dependency>
         <groupId>com.yahoo.athenz</groupId>
         <artifactId>athenz-zms-java-client</artifactId>
         <version>1.8.37</version>
       </dependency>
       <dependency>
         <groupId>com.yahoo.athenz</groupId>
         <artifactId>athenz-cert-refresher</artifactId>
         <version>1.8.37</version>
       </dependency>
     </dependencies>

Next, let’s assume the Athenz identity for the service is ``sports.api``
and SIA running on this host has already generated the private key for
the service and retrieved the X.509 certificate from ZTS Server:

  .. code-block:: bash

     /var/lib/sia/keys/sports.api.key.pem
     /var/lib/sia/certs/sports.api.cert.pem

The ZMS server is running with a public X.509 certificate so we’re going
to use the standard jdk truststore for our connection which has a
default password of ``changeit``.

  .. code:: java

     import javax.net.ssl.SSLContext;
     import com.oath.auth.KeyRefresher;
     import com.oath.auth.Utils;

     final String zmsUrl = "https://zms.athenz.ouroath.com:4443/zms/v1";
     final String keyPath = "/var/lib/sia/keys/sports.api.key.pem";
     final String certPath = "/var/lib/sia/certs/sports.api.cert.pem";
     final String trustStorePath = javaHome + "/jre/lib/security/cacerts";
     final String trustStorePassword = "changeit";

     try {
         // Create our SSL Context object based on our private key and
         // certificate and jdk truststore

         KeyRefresher keyRefresher = Utils.generateKeyRefresher(trustStorePath, trustStorePassword,
             certPath, keyPath);
         // Default refresh period is every hour.
         keyRefresher.startup();
         // Can be adjusted to use other values in milliseconds. However,
         // only one keyRefresher.startup call must be present.
         // keyRefresher.startup(900000);
         SSLContext sslContext = Utils.buildSSLContext(keyRefresher.getKeyManagerProxy(),
             keyRefresher.getTrustManagerProxy());

         // create our zms client and execute request

         try (ZMSClient zmsClient = new ZMSClient(zmsUrl, sslContext)) {
             try {
                 Access access = zmsClient.getAccess("read", "sports:nhl-scores", null, "user.john");
                 System.out.println("Access: " + access.getGranted());
             } catch (ZMSClientException ex) {
                 LOGGER.error("Unable to carry out access check: {}", ex.getMessage());
                 return;
             }
         }
     } catch (Exception ex) {
         LOGGER.error("Unable to process request", ex);
         return;
     }


ZTS Client
==========

We’re going to use our ZTS Java client to communicate with ZTS Server
running in AWS to retrieve the public key for the ``sys.auth.zms``
service with key id ``zms.athens.gq1.0``.

First we need to update our Java project ``pom.xml`` file to indicate
our dependency on the ZTS Java Client and Certificate Refresh Helper
libraries

  .. code-block:: xml

     <dependencies>
       <dependency>
         <groupId>com.yahoo.athenz</groupId>
         <artifactId>athenz-zts-java-client</artifactId>
         <version>1.8.37</version>
       </dependency>
       <dependency>
         <groupId>com.yahoo.athenz</groupId>
         <artifactId>athenz-cert-refresher</artifactId>
         <version>1.8.37</version>
       </dependency>
     </dependencies>

Next, let’s assume the Athenz identity for the service is ``sports.api``
and SIA running on this host has already generated the private key for
the service and retrieved the X.509 certificate from ZTS Server:


  .. code:: bash

     /var/lib/sia/keys/sports.api.key.pem
     /var/lib/sia/certs/sports.api.cert.pem

The ZTS server is running with a public X.509 certificate so we’re going
to use the standard jdk truststore for our connection which has a
default password of ``changeit``.

  .. code:: java

     import javax.net.ssl.SSLContext;
     import com.oath.auth.KeyRefresher;
     import com.oath.auth.Utils;

     final String ztsUrl = "https://zts.athenz.ouroath.com:4443/zts/v1";
     final String keyPath = "/var/lib/sia/keys/sports.api.key.pem";
     final String certPath = "/var/lib/sia/certs/sports.api.cert.pem";
     final String trustStorePath = javaHome + "/jre/lib/security/cacerts";
     final String trustStorePassword = "changeit";

     try {
         // Create our SSL Context object based on our private key and
         // certificate and jdk truststore

         KeyRefresher keyRefresher = Utils.generateKeyRefresher(trustStorePath, trustStorePassword,
             certPath, keyPath);
         // Default refresh period is every hour.
         keyRefresher.startup();
         // Can be adjusted to use other values in milliseconds.
         //keyRefresher.startup(900000);
         SSLContext sslContext = Utils.buildSSLContext(keyRefresher.getKeyManagerProxy(),
             keyRefresher.getTrustManagerProxy());

         // create our zts client and execute request

         try (ZTSClient ztsClient = new ZTSClient(ztsUrl, sslContext)) {
             try {
                 PublicKeyEntry publicKey = ztsClient.getPublicKeyEntry("sys.auth", "zms", "zms.athens.gq1.0");
                 System.out.println("PublicKey: " + publicKey.getKey());
             } catch (ZTSClientException ex) {
                 LOGGER.error("Unable to retrieve public key: {}", ex.getMessage());
                 return;
             }
         }
     } catch (Exception ex) {
         LOGGER.error("Unable to process request", ex);
         return;
     }


.. important::
  During the shutdown of the application, ``ZTSClient.cancelPrefetch()``
  must be called to stop the timer thread that automatically fetches and
  refreshes any cached tokens in the ZTS Client.
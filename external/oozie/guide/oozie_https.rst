.. _httpsoozie:

Oozie With HTTPS
================

.. 04/12/17: Rewrote.

   :depth: 1 
   :local:

.. _installing_cert:  

Installing cert package on Launcher
-----------------------------------
If you are accessing Oozie from a launcher instead of gateway, you need to install 
ygrid_cacert package on your launcher. Package ygrid_cacert-2.0 contains Grid CA certificate 
sthat is used to validate the SSL certificate presented by Oozie server.

.. code-block:: bash

   $ yinst install ygrid_cacert-2.0


ygrid_cacert package contains two files:

- ``/home/y/conf/ygrid_cacert/certstore.jks`` : Java trust store file containing Cert for use in java clients.
- ``/home/y/conf/ygrid_cacert/ca-cert.pem`` : CA Cert in pem format for use in clients like curl.


.. _ui_access:  

UI Access through Browser
-------------------------
Specify ``https`` instead of ``http`` and port ``4443`` instead of ``4080`` in the oozie url. 
For eg: https://uraniumtan-oozie.tan.ygrid.yahoo.com:4443/oozie/ instead of 
http://uraniumtan-oozie.tan.ygrid.yahoo.com:4080/oozie/. 

.. _commdline_access:  

CommandLine Access
------------------

``export OOZIE_URL``  to the https url or specify that in ``-oozie`` option.
For eg:

.. code-block:: bash

   $ export OOZIE_URL=https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie
   $ oozie job -info xxxx


Or -

.. code-block:: bash

   $ oozie -oozie https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie job -info xxxxx


Oozie client by default uses ``/home/y/conf/ygrid_cacert/certstore.jks`` as truststore, 
and that can be changed by exporting ``OOZIE_SSL_CLIENT_CERT`` to the new truststore path.

.. _java_client_access:  

Java Client
-----------

While running java program, specify ``/home/y/conf/ygrid_cacert/certstore.jks`` as truststore 
using ``-Djavax.net.ssl.trustStore`` system property while launching the jvm. 
Apart from changing the URL from http to https, no other code change is required.

.. code-block:: bash

   $ java -Djavax.net.ssl.trustStore=/home/y/conf/ygrid_cacert/certstore.jks...


If you are making bouncer from same JVM then you need to make code changes. 
Refer `this code <https://git.corp.yahoo.com/hadoop/yahoo-oozie/blob/ytrunk/yauth/src/main/java/com/yahoo/oozie/security/authentication/client/BouncerAuthenticator.java#L169>`_.

.. _curl_users:  

Curl Users
----------

Specify the location of the CA cert in the --cacert parameter to validate the SSL certificate presented by the Oozie server.

.. code-block:: bash

   $ curl -v --negotiate -u : --cacert /home/y/conf/ygrid_cacert/ca-cert.pem https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/v1/job/

Troubleshooting
---------------

If you encounter the following error: 

.. code-block:: java

   IO_ERROR : javax.net.ssl.SSLHandshakeException: sun.security.validator.ValidatorException: PKIX path building failed: sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target


You are facing same issue as http://bug.corp.yahoo.com/show_bug.cgi?id=6843888.
The solution is to combine using a java tool other ssl certificates along with the 
Grid CA cert required for accessing other secure servers.
Manually add the grid certs to java keystore using following commands:

.. code-block:: bash

   keytool -export -alias gridcanew -file gridcanew.cer -keystore /home/y/conf/ygrid_cacert/certstore.jks
   sudo keytool -import -v -trustcacerts -alias gridcanew -file gridcanew.cer -keystore /home/y/libexec64/java/jre/lib/security/cacerts


Authentication/Authorization
============================

.. 04/15/15: Rewrite
.. 05/11/15: Second edit.

Oozie provides two different ways of authentication: Kerberos and YCA.
You need to do some setting up for Kerberos and YCA.

Kerberos Authentication
-----------------------

Yahoo Oozie is bundled with a custom Oozie command-line tool that adds Kerberos 
authentication. The ``-auth`` option can take the argument ``kerberos`` to authenticate 
by Kerberos. When submitting a job or any other tasks, the user can only specify 
Kerberos as authentication type if the Oozie server is configured to accept this 
authentication.

To support new authentication (Kerberos) in the Oozie client, the Java Cryptography Extension (JCE) JARs 
have to  be replaced in ``JAVA_HOME`` to support stronger encryption. The passphrase required 
for installation can be found in `yjava_jce package <http://dist.corp.yahoo.com/by-package/yjava_jce>`_: ``$ yinst install yjava_jce``


#. Before invoking Oozie, obtain and cache the Kerberos ticket-granting ticket::

       $ kinit $USER@Y.CORP.YAHOO.COM

   You can also use the following::

       $ kinit -k -t ~/Headless_USER.keytab Headless_USER/localhost@LOCALHOST

#. To invoke Oozie using Kerberos authentication::

       $ oozie job -oozie http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie -run -config job.properties -auth KERBEROS


#. To test Kerberos using cURL:

   - Create a cookie file::

         $ curl -v -c cookie.txt --negotiate -u $USER -k http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/v1/admin/build-version

   - Reuse the existing cookie::

         $ curl -b cookie.txt --negotiate -u $USER -k http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/v1/admin/build-version

#. (Optional) You can also use ``kinit`` to create the Kerberos ticket::

      $ kinit -kt ~/`whoami`.dev.headless.keytab `whoami`@DEV.YGRID.YAHOO.COM

#. Use the default Kerberos ticket::

       $ curl --negotiate -u $USER -k http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/v1/admin/build-version


.. note:: The examples above use the Oozie server on Kryptonite Red. To use Oozie servers on other clusters,
          see :ref:`Oozie Serves on Clusters <references-oozie_servers>`.

Client API Example of Kerberos Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#. Include the JAR ``/home/y/var/yoozieclient/lib/*jar`` in your ``CLASSPATH``.
#. Create the Oozie client ``KerbAPIExample.java`` with the following code:

   .. code-block:: java

      package com.yahoo.oozie.test;
      import org.apache.oozie.client.OozieClientException;
      import org.apache.oozie.client.XOozieClient;
      import org.apache.oozie.client.AuthOozieClient;
      import org.apache.hadoop.security.authentication.client.Authenticator;
      import com.yahoo.oozie.security.authentication.client.KerberosAuthenticator;
      import java.net.URL;
      import java.util.HashMap;
      import java.util.Map;
      
      public class KerbAPIExample {
      
          public static void main(String args[]) {
              String oozieurl="http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie";
              String jobId = args[0];
              KerbOozieClient koc = new KerbOozieClient(oozieurl);
              try {
                  System.out.println(koc.getJobInfo(jobId,0,10));
              } catch (OozieClientException e) {
                  e.printStackTrace();
              }
          }
          static class KerbOozieClient extends AuthOozieClient {
      
              String kerbAuth = "KERBEROS";
              public KerbOozieClient(String oozieUrl) {
                  super(oozieUrl, "KERBEROS");
              }
      
              @Override
              protected Map<String, Class<? extends Authenticator>> getAuthenticators() {
                  Map<String, Class<? extends Authenticator>> authClasses = new HashMap<String, Class<? extends Authenticator>>();
                  authClasses.put(kerbAuth, KerberosAuthenticator.class);
                  return authClasses;
              }
      
          }
      }

#. Compile the code: ``$ javac -cp $CLASSPATH KerbAPIExample.java``
#. Run your example: ``$ java -cp $CLASSPATH KerbAPIExample 00001-1234-W``


YCA Authentication
------------------

Yahoo Oozie is also bundled with a custom command-line tool that adds YCA 
authentication. The ``-auth`` option can take the argument ``yca`` to 
authenticate by YCA. When using Oozie to submit job or any other tasks, you 
can only specify YCA as the authentication type if Oozie server is configured to accept 
this authentication. Also, the allowed YCA namespaces have to be configured in the Oozie server.

Creating an Oozie Role
~~~~~~~~~~~~~~~~~~~~~~

To create a role in Oozie for a YCA allowed namespace:

#. If Oozie server accepts namespace ``"griduser"``, the user should create a 
   role under it. Refer to `Support YCAProtected Grid Servic <http://twiki.corp.yahoo.com/view/Grid/SupportGYCA>`_ 
   for details.
         
   #. File a `Jira issue with OpsDB <https://jira.corp.yahoo.com/servicedesk/customer/portal/89/create/554>`_
      to create a role. Your role name should use the syntax ``<namespace>.<username>``.
   #. Register the list of hosts as members in this role.
   #. Install the ``yca`` and ``yca_client_certs`` packages. 
      The ``yca_client_certs`` package will only install successfully when
      that host is already present in the ``rolesdb``.
   #. Run the command ``/home/y/bin/yca-cert-util --show``. It will list 
      the ``yca`` certificates of the machine.


Invoking Oozie With YCA Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To invoke Oozie by YCA authentication as the ``<username>`` at one of the registered hosts::

    $ oozie job -oozie http://localhost:8080/oozie -run -config job.properties -auth YCA


Verifying YCA Certificates 
~~~~~~~~~~~~~~~~~~~~~~~~~~

To list the YCA certificates of the machine and their expiration date::

    $ /home/y/bin/yca-cert-util --show --detail

If the certificate has expired, to refresh::

    $ /home/y/bin/yca-cert-util --fetch --refresh

To verify the certificate::

    $ curl -H "Yahoo-App-Auth: {the yca certificate from command yca-cer-util --show; starting from v1=1;a=yahoo.griduser.......}" -k http://{oozie server hostname}:4080/oozie/v1/admin/build-version


YCA Authentication With YCA Proxy Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use the YCA proxy server for YCA authentication::

    $ oozie -Dhttp.proxyHost=yca-proxy.corp.yahoo.com -Dhttp.proxyPort=3128 jobs -oozie http://{oozieurl} -auth YCA


Adding YCA to a Workflow
~~~~~~~~~~~~~~~~~~~~~~~~

Creating a Namespace and a Role
*******************************

The role ``oozie.httpproxy`` is created for this purpose. You can create your 
namespace in the roles ``db`` and add a role under the namespace. In our case, the namespace 
is ``oozie``, and the role name is ``httpproxy``. Under the role, you can add the user who 
wants to submit the job with gYCA credential. For example, the user ``strat_ci``
can submit the workflow with gYCA credential, so we add ``strat_ci.wsca.user.yahoo.com``
to the role ``oozie.httpproxy``. See the example http://roles.corp.yahoo.com:9999/ui/role?action=view&id=217516.


Submit a Workflow With the YCAv2(gYCA) Certificate
**************************************************

Users have to specify the gYCA credential explicitly in the beginning of a Workflow and 
ask Oozie to retrieve certificate whenever an action needs to call a YCA-protected 
Web service. In each credential element, the attribute ``name`` is the key and the attribute ``type``
indicates which credential to use.

The credential ``type`` is defined in the Oozie server. For example, on 
``axoniteblue-oozie.blue.ygrid.yahoo.com``,  the YCA credential type is defined as ``yca``, 
with the following::

    yoozie_conf_axoniteblue.axoniteblue_conf_oozie_credentials_credentialclasses: yca=com.yahoo.oozie.action.hadoop.YCAV2Credentials,howl=com.yahoo.oozie.action.hadoop.HowlCredentials,hcat=com.yahoo.oozie.action.hadoop.HowlCredentials

Users can give multiple ``credential`` elements under ``credentials`` and specify a 
comma-separated list of credentials to use under each action ``cred`` attribute.
There is only one parameter required for the credential ``type``.

- ``yca-role``: the role name contains the user names for YCA v2 certificates.

There are three optional parameters for the credential type ``yca``:

- ``yca-webserver-url``: the YCA server URL. The default URL is http://ca.yca.platform.yahoo.com:4080.
- ``yca-cert-expiry``: The expiry time of the YCA certificate in seconds. The default is one day (86400). This is available from Oozie 3.3.1.
- ``yca-http-proxy-role``: The role name in the Roles DB that contains the hostnames of 
  the machines in the HTTP proxy VIP. The default value is ``grid.httpproxy`` which contains 
  all HTTP proxy hosts. This parameter depends on the HTTP proxy VIP you will be using to send 
  the obtained YCA v2 certificate to the Web service outside the grid. You can limit 
  the corresponding role name that contains the hosts of the HTTP proxy VIP. The 
  role names containing members of production HTTP proxy VIPs are ``grid.blue.prod.httpproxy``, 
  ``grid.red.prod.httpproxy``, and ``grid.tan.prod.httpproxy``. 

  For example, the following contains the hosts of the production ``httpproxy``: ``http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.prod.httpproxy``
  This role is the parent role containing the staging, research, and production ``httpproxy`` hosts: ``http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.httpproxy``
  See the `Http Proxy Node List <http://twiki.corp.yahoo.com/view/Grid/HttpProxyNodeList>`_ for 
  the role name and VIP name of the deployed HTTP proxies for staging, research, and sandbox grids.


Example Workflow
****************

The following ``workflow.xml`` snippet shows how to configure your Workflow to use YCA authentication and set the role:

.. code-block:: xml

   <workflow-app>
      <credentials>
         <credential name='myyca' type='yca'>
            <property>
               <name>yca-role</name>
                  <value>griduser.actualuser</value>
            </property>
         </credential> 
      </credentials>
      <action cred='myyca'>
         <map-reduce>
            ...
         </map-reduce>
      </action>
   <workflow-app>

Java Code Example
*****************

When an Oozie action executor sees a ``cred`` attribute in the current action, depending 
on the credential name, it finds the appropriate credential class to retrieve 
the token or certificate and inserts the action configuration. 

In the above example, Oozie gets the certificate of gYCA and passes it to the action configuration. 
Mapper can then use this certificate by getting it from the action configuration, adding it to 
the HTTP request header when connecting to the YCA-protected Web service through ``HTTPProxy``. 

A certificate or token retrieved in the credential class would set an action configuration
as the name of credential defined in ``workflow.xml``. The following example shows 
how to communicate with the YCAV2-protected Web service from the grid.

.. code-block:: java


   //**proxy setup**

   //blue proxy
   //InetSocketAddress inet = new InetSocketAddress("flubberblue-httpproxy.blue.ygrid.yahoo.com", 4080);
   //gold proxy
   InetSocketAddress inet = new InetSocketAddress("httpproxystg-rr.gold.ygrid.yahoo.com", 4080);
   Proxy proxy = new Proxy(Type.HTTP, inet);
   URL server = new URL(fileURL);

   //**web service call**
   String ycaCertificate = conf.get("myyca");
   HttpURLConnection con = (HttpURLConnection) server.openConnection(proxy);
   con.setRequestMethod("GET");
   con.addRequestProperty("Yahoo-App-Auth", ycaCertificate);



Oozie Authorization
===================

Oozie provides three different ways of authentication, Bouncer, Kerberos, and YCA.
Pre-setup should be done for Kerberos and YCA.

Bouncer Authentication (default)
--------------------------------

The Yahoo! Oozie is bundled with a custom Oozie command-line tool that adds 
Backyard authentication. Backyard authentication is default authentication type.
The ``-auth`` option can be specified with ``BOUNCER`` to activate Backyard 
authentication.

Bouncer authentication related behavior of the Oozie command-line tool can be 
modified via the following shell environment variables:

- ``OOZIE_SAVE_COOKIE=[true|false*]``: If set to ``true``, the Oozie command line will cache the 
  ``BY`` cookie in the ``~/.oozie-cookie`` file. If not set or set to false the Oozie command 
  line will ask for the password every time and the cache filed will be deleted if 
  it exists. The default is false.

- ``OOZIE_BOUNCER=[gh*|by]``: It indicates which Yahoo Bouncer to use, Backyard or 
  Guesthouse. The default is Guesthouse.

- ``OOZIE_CLI_AUTH_DISABLED=[true|false*]``: If set to true the Oozie command line 
  will not do Backyard authentication. If set to true it has to be disabled on the 
  server as well.

The above environment variables can be overridden for a single Oozie invocation 
using the following Java System properties when invoking oozie:

- oozie.save.cookie
- oozie.bouncer
- oozie.cli.auth.disabled

Invoking oozie by Bouncer authentication.
      $ oozie job -oozie http://localhost:8080/oozie -run -config job.properties -auth BOUNCER
or
       $ oozie job -oozie http://localhost:8080/oozie -run -config job.properties


Kerberos Authentication
-----------------------

Yahoo Oozie is bundled with a custom oozie command line tool that adds Kerberos 
authentication. The ``-auth`` option can be specified with KERBEROS to authenticate 
by Kerberos. When using oozie to submit job or any other tasks, user can specify 
Kerberos as authentication type if Oozie server is configured to accept this 
authentication.

To support new authentication (Kerberoes) in ``OOZIE CLIENT``, JCE jars have to 
be replaced in ``JAVA_HOME`` to support stronger encryption. The passphrase required 
for installation can be found in ``http://dist.corp.yahoo.com/by-package/yjava_jce``: ``$ yinst install yjava_jce``


#. Before invoking oozie, obtain and cache Kerberos ticket-granting ticket::

       $ kinit username@DS.CORP.YAHOO.COM

   You can also use the following::

       $ kinit -k -t ~/Headless_USER.keytab Headless_USER/localhost@LOCALHOST

#. Invoking Oozie using Kerberos authentication::

       $ oozie job -oozie http://localhost:8080/oozie -run -config job.properties -auth KERBEROS


#. To test Kerberos using curl:

   - Create a cookie file::

         $ curl -v -c cookie.txt --negotiate -u : -k http://localhost:8080/oozie/v1/admin/build-version
   - Reuse the existing cookie::

         $ curl -b cookie.txt --negotiate -u : -k http://localhost:8080/oozie/v1/admin/build-version

  You can also use ``kinit`` to create the Kerberos ticket::

      $ kinit -kt ~/`whoami`.dev.headless.keytab `whoami`@DEV.YGRID.YAHOO.COM

#. Use the default Kerberos ticket::

       $ curl --negotiate -u : -k http://localhost:8080/oozie/v1/admin/build-version


Client API example of Kerberos Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

YCA Authentication
------------------

Yahoo Oozie is bundled with a custom command-line tool that adds YCA 
authentication. The ``-auth`` option can be specified with YCA to 
authenticate by YCA. When using oozie to submit job or any other tasks, user 
can specify YCA as authentication type if Oozie server is configured to accept 
this authentication. The allowed YCA namespaces have to be configured in oozie server.

To create role in Oozie allowed namespace:

#. If Oozie server accepts namespace ``"griduser"``, the user should create a 
   rol under it. Please refer to http://twiki.corp.yahoo.com/view/Grid/SupportGYCA for details.
         
   #. File a Bugzilla ticket to create a role using the following
      as a template: http://bug.corp.yahoo.com/show_bug.cgi?id=3899711
        
      Your role name should use the syntax ``<namespace>.<username>``.
   #. Register the list of hosts as members in this role.
   #. Install the ``yca`` and ``yca_client_certs`` packages. 
      The ``yca_client_certs`` package will only install successfully when
      that host is already present in the ``rolesdb``.
   #. Run the command ``/home/y/bin/yca-cert-util --show``. It will list 
      the ``yca`` certificates of the machine.


To invoke Oozie by YCA authentication as the ``<username>`` at one of the registered hosts::

    $ oozie job -oozie http://localhost:8080/oozie -run -config job.properties -auth YCA


YCA Certificate Verification
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To list the yca certificates of the machine, and their expiration date::

    $ /home/y/bin/yca-cert-util --show --detail

If the certificate has expired, to refresh::

    $ /home/y/bin/yca-cert-util --fetch --refresh

To verify the certificate::

    $ curl -H "Yahoo-App-Auth: {the yca certificate from command yca-cer-util --show; starting from v1=1;a=yahoo.griduser.......}" -k http://{oozie server hostname}:4080/oozie/v1/admin/build-version


YCA Authentication With YCA Proxy Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    $ oozie -Dhttp.proxyHost=yca-proxy.corp.yahoo.com -Dhttp.proxyPort=3128 jobs -oozie http://{oozieurl} -auth YCA

Adding YCA to a Workflow
~~~~~~~~~~~~~~~~~~~~~~~~

Creating a Namespace and a Role
*******************************

This role ``oozie.httpproxy`` is creating for this purpose. You can create your 
namespace in the roles ``db`` and add a role under the namespace. In our case, namespace 
is ``oozie`` and role name is ``httpproxy``. Under the role, you can add the user who 
wants to submit the job with gYCA credential. For example, we use the user ``strat_ci``
to submit the workflow with gYCA credential, so we add ``strat_ci.wsca.user.yahoo.com``
to the role ``oozie.httpproxy``. See the example http://roles.corp.yahoo.com:9999/ui/role?action=view&id=217516.


Submit a Workflow With the YCAv2(gYCA) Certificate
**************************************************

User has to specify the gYCA credential explicitly in the workflow beginning and 
asks Oozie to retrieve certificate whenever an actions needs to call YCA protected 
Web service. In each credential element, the attribute ``name`` is the key and the attribute ``type``
indicates which credential to use.

The credential ``type`` is defined in oozie server. For example, on ``axoniteblue-oozie.blue.ygrid.yahoo.com``, 
the YCA credential type is defined as ``yca``, as in the following::

    yoozie_conf_axoniteblue.axoniteblue_conf_oozie_credentials_credentialclasses: yca=com.yahoo.oozie.action.hadoop.YCAV2Credentials,howl=com.yahoo.oozie.action.hadoop.HowlCredentials,hcat=com.yahoo.oozie.action.hadoop.HowlCredentials

User can give multiple ``credential`` elements under ``credentials`` and specify a 
list of credentials with comma separated to use under each action ``cred`` attribute.
There is only one parameter required for the credential ``type``.

- ``yca-role``: the role name contains the user names for YCA v2 certificates.

There are three optional parameters for the credential type ``yca``.

- ``yca-webserver-url``: the YCA server URL. Default is http://ca.yca.platform.yahoo.com:4080
- ``yca-cert-expiry``: The expiry time of the YCA certificate in seconds. Default is one day (86400). Available from Oozie 3.3.1
- ``yca-http-proxy-role``: The roles DB role name which contains the hostnames of 
  the machines in the HTTP proxy vip. Default value is ``grid.httpproxy`` which contains 
  all http proxy hosts. Depending on the http proxy vip you will be using to send 
  the obtained YCA v2 certificate to the Web service outside the grid, you can limit 
  the corresponding role name that contains the hosts of the http proxy vip. The 
  role names containing members of production http proxy vips are ``grid.blue.prod.httpproxy``, 
  ``grid.red.prod.httpproxy`` and ``grid.tan.prod.httpproxy``. 
  For example: http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.prod.httpproxycontains the 
  hosts of production httpproxy. http://roles.corp.yahoo.com:9999/ui/role?action=view&name=grid.blue.httpproxy 
  is a uber role which contains staging, research and production httpproxy hosts.http://twiki.corp.yahoo.com/view/Grid/HttpProxyNodeList 
  gives the role name and VIP name of the deployed HTTP proxies for staging, research, and sandbox grids.

Example Workflow
****************

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

When Oozie action executor sees a ``cred`` attribute in current action, depending 
on credential name given, it finds the appropriate credential class to retrieve 
the token or certificate and insert to action conf for further use. In above example, 
Oozie gets the certificate of gYCA and passed to action conf. Mapper can then use 
this certificate by getting it from action conf, and add to http request header 
when connect to YCA protected web service through HTTPProxy. A certificate or token 
which retrieved in credential class would set in action conf as the name of credential 
defined in workflow.xml. The following examples shows sample code to use in mapper 
or reducer class for talking to YCAV2 protected web service from grid.

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





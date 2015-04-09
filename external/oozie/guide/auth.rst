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


To invoke Oozie by YCA authentication as the ``<username>`` 
at one of the registered hosts::

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


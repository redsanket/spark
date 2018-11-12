============================
Authorization/Authentication
============================


Authorization
=============

By default, you will have permissions to manipulate and see any topology that you 
launch. If you want to allow others to also manipulate this topology you need to 
set the topology configuration ``topology.users`` or ``topology.groups`` to be a list of all the users or groups you want to 
be able to manipulate this topology. This must be specified in the configuration used for
launching your topology.


Authentication
==============

Supported Authorization Methods
-------------------------------

The following authorization methods are supported:

- **HTTP** - Using HTTP Authentication or with a Custom Java Servlet Filter.
- **Thrift** - Kerberos (possibly through a forwarded TGT).
- **ZooKeeper** - Kerberos for system processes (because there is a ``keytab`` available) a 
  shared secret for worker processes with MD5SUM in ZooKeeper.
- **File System** - OS user + file system (FS) permissions. Some processes on the same system communicate through files.
- **Worker to Worker** - Can use encryption with shared secret, but still does not 
  allow Simple Authentication and Security Layer (SASL) authorization.
- **External Services (like HBase)** - For the time being, you have to implement your authorization method for external services.


Kerberos Authentication
-----------------------

Storm at Oath uses Kerberos to authenticate the end user with Nimbus. For hosted 
clusters, it should already be set up on the gateways so that all you need to do 
to access Storm is to run ``kinit`` before running the ``storm`` command.

If you are not running from a properly configured gateway you may need to force 
the client to authenticate with Nimbus using Kerberos by adding the following 
configurations on the command line.

::

    storm -c storm.thrift.transport=backtype.storm.security.auth.kerberos.KerberosSaslTransportPlugin -c java.security.auth.login.config=/path/to/jaas.conf

It is a known issue that if you try to use an insecure client with a secure 
Nimbus, the client will hang with no explanation of the problem.

The ``jaas.conf`` file needs to have a section called ``StormClient`` that tells Storm 
how to authenticate with Nimbus. If you want it to go off of the Ticket Granting Ticket (TGT) in the ticket 
cache use a ``jaas.conf`` similar to the following::

    StormClient {
       com.sun.security.auth.module.Krb5LoginModule required
       doNotPrompt=false
       useTicketCache=true
       serviceName="$nimbus_user";
    };


If you have a headless user with a ``keytab``, you can use a ``jaas.conf`` like the following::

    StormClient {
       com.sun.security.auth.module.Krb5LoginModule required
       useKeyTab=true
       keyTab="$keytab"
       storeKey=true
       useTicketCache=false
       serviceName="$nimbus_user"
       principal="$principal";
    };


Authentication for External Services
------------------------------------

Multi-tenant storm supports pushing credentials as well as tickets and tokens when launching 
a topology and on demand when they are about to expire.


Automatic Credentials Push
##########################

Credentials can be gathered automatically by the 
client and put into the current Java Subject on the workers. By default, on 
multi-tenant clusters that support this a plugin for pushing the TGT and setting 
it up so that it is compatible with Hadoop should be automatic. There is also a 
plugin that will renew the Ticket-Granting Ticket (TGT), so you will only have to push a new TGT once or 
twice a week. If you have other credentials that you would like to automatically 
push, `ask the Storm team <email:storm-devel@oath.com>`_ how to set this up.

These plugins do not schedule anything to push credentials periodically, so before 
they are about to expire you will need to have something cron, etc., that will log 
the user in through a keytab and run ``storm upload-credentials <topology-name>`` or 
call ``StormSubmitter.pushCredentials`` programatically. Both of these options can 
be combined with manually populated credentials.

For TGT, the metric TGT-TimeToExpiryMsecs exists to indicate when the credentials will expire.


Manual Credentials Push
#######################

To have bolts or spouts notified when credentials change, you will need to have 
them implement ``ICredentialsListener``. The ``setCredentials`` method will 
be called before the methods ``prepare`` or ``open`` is called. It will also be called any time new 
credentials are pushed to a topology. The credentials are just a mapping of string 
to string. This gives a lot of flexibility in what is and is not pushed.

To push new credentials, use the command-line tool::

    storm upload-credentials <topology-name> [-f <cred-file.properties>] [<cred-key> <cred-value>]*

You can also use the ``StormSubmitter.pushCredentials`` API. 

See :ref:`Accessing External Services Through Multitenant Storm <auth-access_ext_services>` 
for details about specific services.

Credentials Push (Authenticating With External Services)
--------------------------------------------------------

A set of APIs and plugins that allow credentials to securely be delivered and renewed.

- **ICredentialsListener** - using HTTP Authentication or with a Custom Java Servlet Filter.
- **IAutoCredentials** - Kerberos (Possibly through a forwarded TGT).
- **ICredentialsRenewer** - Kerberos for system processes (Because there is a 
  keytab available) a shared secret for worker processes with MD5SUM in ZooKeeper.
- **storm upload_credentials** - OS user and filesystem permissions. Some processes on the 
  same system communicate through files.
- **AutoTGT** - can use encryption with shared secret, but SASL Authorization is still not enabled.


Accessing External Services Through Multitenant Storm 
======================================================

We've discussed authorization and authentication for Storm at Oath. In this section, 
we'll look at using credentials for multi-tenant Storm to access external services. 

If you are running your own cluster without multi-tenancy, you can simply use 
credentials in a more traditional way with host-based YCA v1, ``ykeykey``, etc. For 
multi-tenant Storm, we do not plan on installing any credentials for individuals on the cluster. 
You will have to transmit those credentials with the topology.

There are numerous services used at Oath that require authentication to be able to access them. 
We are working on proper solutions and examples for many of these. If you need 
more of them, please mention it when `on-boarding <../onboarding>`_ 
or `file a yo/ystorm-request <http://yo/ystorm-request>`_
so that we can work on it with the other teams involved.

Credentials API
---------------

The credentials API has been added that allows owners of a topology to send credentials to 
a topology when it is launched and to send updated credentials periodically
before the old ones expire. 

For most cases, plugins that automatically push credentials on your behalf should 
allow your topology to run unchanged. These plugins support 
pushing your TGT out and allowing services like Hadoop and HBASE to access it 
unchanged.

The manual API will send a ``Map<String,String>`` to spouts and bolts that are 
listening for it. There is little convention about how keys and values are stored 
in the Map. As more types of credentials are used, we hope to expand the set of 
plugins that automatically push them with a small amount of configuration.

Credentials Push
################

To submit a topology with the credential API you would run something like the following:

.. code-block:: java

    import backtype.storm.StormSubmitter;
    import backtype.storm.topology.TopologyBuilder;
    import backtype.storm.generated.SubmitOptions;
    import backtype.storm.generated.TopologyInitialStatus;
    import backtype.storm.generated.Credentials;
    
    //...
    
    Map<String,String> creds = new HashMap<String, String>();
    //Fill creds as needed
    
    TopologyBuilder builder = new TopologyBuilder();
    //Setup Topology
    
    SubmitOptions opts = new SubmitOptions(TopologyInitialStatus.ACTIVE);
    opts.set_creds(new Credentials(_creds));
    StormSubmitter.submitTopology(topologyName, conf, builder.createTopology(), opts);

To use the plugins to send credentials::

.. code-block:: java

   import backtype.storm.StormSubmitter;
   import backtype.storm.topology.TopologyBuilder;
   
   //...
   
   Map<String,String> creds = new HashMap<String, String>();
   //Fill creds as needed
   
   TopologyBuilder builder = new TopologyBuilder();
   //Setup Topology
   
   StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());

To send updated credentials:

.. code-block:: java

   import backtype.storm.StormSubmitter;
   //...
   
   Map<String,String> creds = new HashMap<String, String>();
   //Fill creds as needed
   
   StormSubmitter.pushCredentials(topologyName, conf, creds);

Receiving Credentials
#####################

To get the pushed credentials, a spout or a bolt can implement the 
`ICredentialsListener <https://git.ouroath.com/storm/storm/blob/master-security/storm-core/src/jvm/backtype/storm/ICredentialsListener.java>`_ 
interface. It provides the following single method:

.. code-block:: java

   public void setCredentials(Map<String,String> credentials);

This method will be called before the ``prepare`` method of the bolt or the ``open`` method 
of the spout. It will also be called after new credentials are pushed, but may take up to a 
few minutes from the time the client finishes.


YCA Authentication
------------------

YCA v1 is not available for hosted multi-tenant storm. YCAv2 **must** be used. You 
can get a YCAv2 certificate using either Kerberos or by using a YCAv1 cert for a 
role in the ``griduser`` namespace with the role name matching the user name. Although 
this is generally reserved for launcher boxes, anyone with access to the 
box can get the corresponding certificate.

The V2 certificate being fetched must be for a role that includes a special host name for the user::

    <username>.wsca.user.yahoo.com

Code has been added to Storm to automatically fetch 
and push YCA certificates on your behalf. To use this, you need to know about the
three configurations in the table below.

.. csv-table:: YCA Configurations
   :header: "Name", "Configuration"
   :widths: 15, 40
   
   "``yahoo.autoyca.appids``", "This is the config that you will interact with the most. It is a comma separated list of YCAv2 application IDs that should be fetched and passed to the topology."
   "``yahoo.autoyca.v1appid``",	"If set this is the YCAv1 cert that should be used when fetching YCAv2 certs. If not set kerberos will be used instead."
   "``yahoo.autoyca.proxyappid``", "This is the role for the http proxies that should be used with this YCAv2 cert. If not set YCA will guess based off of the colo you are in. It almost always gets this correct."

On the worker side, you can fetch the most up-to-date certificate using static methods in 
the ``com.yahoo.storm.security.yca.AutoYCA`` class. This class is in a separate Oath-
specific ``storm`` jar in the same ``yinst`` package/maven artifact. You need to 
include a dependency on ``storm_yahoo`` to compile your code.

.. code-block:: xml

   <dependency>
     <groupId>yahoo.yinst.ystorm</groupId>
     <artifactId>storm_yahoo</artifactId>
     <version>0.9.0_wip21.225</version>
     <exclusions>
       <exclusion>
         <groupId>storm</groupId>
         <artifactId>storm-core</artifactId>
       </exclusion>
     </exclusions>
   </dependency>

(Exclusions are due to incompatibilities between Maven and Yinst.)

You should only use the method ``getYcaV2Cert(String appId)`` to get a specific YCA v2 certificate. 
It returns ``null`` if the certificate is not found. There are other methods to help with testing
or to support other use cases.

The following are some examples:

- Submitting a topology and requesting YCAv2 certs from the command line. 
  Kerberos will be used to fetch the certificate::

      storm jar ./my-topology.jar com.yahoo.RunTopology -c "yahoo.autoyca.appids=yahoo.role.name"

- On the worker side getting that same certificate:

  .. code-block:: java
 
     import com.yahoo.storm.security.yca.AutoYCA;
     ...
     String myCert = AutoYCA.getYcaV2Cert("yahoo.role.name");
     httpRequest.addHeader("Yahoo-App-Auth",myCert);

- Submitting a topology while setting the config programatically to fetch two certificates::

      conf.put(AutoYCA.YCA_APPIDS_CONF, "yahoo.role.name1,yahoo.role.name2");
      ...
      StormSubmitter.submitTopology("name", conf, builder.createTopology());

- Pushing new YCA credentials to a topology without writing any code::

      storm upload-credentials my-topology-name -c yahoo.autoyca.appids=yahoo.role.name1,yahoo.role.name2"



HBase
-----

The plugin for automatically pushing TGT credentials should work with HBase. You should be 
able to access Hbase just as if you were logged in through Kerberos, but because 
HBase is not installed on the workers, you will need to push the code and 
configuration to your topology ``jar``.

For HBase authentication, the ticket cache only needs to be placed on the
gateway/launcher box.  When you run ``kinit`` to get a TGT from the 
Key Distribution Center (KDC) you need to be sure you either pass in the ``-f`` flag 
or have you ``krb5.conf`` file set up to get a TGT that can be forwarded.
Then when you submit your Storm topology, a piece of
code called AutoTGT will take your TGT and send it to the processes
in your topology. It also knows about Hadoop/HBase, so if it finds Hadoop on
your class path and the Hadoop configuration indicates that security is enabled, it will
do what is needed to make Hadoop/HBase use the TGT.
    
The big difference here is that in your topology you will not need to run any
code that will log you into a keytab because we have already done that for
you. 
    
Because TGTs expire, you will need to push a new TGT at least once a day to
your topology. You can do this by re-running ``kinit`` just like before, and then
running the following::
    
    storm upload-credentials <name-of-topology>
    
 This will push the new TGT to your topology and AutoTGT will put it where it
 needs to go for HBase/Hadoop to access it.

Include a file like the following ``hadoop-site.xml`` in your topology jar:

.. code-block:: xml

   <configuration>
       <property><name>hadoop.security.authentication</name><value>kerberos</value></property>
       <property><name>hadoop.security.auth_to_local</name><value>RULE:[2:$1@$0](.*@DS.CORP.YAHOO.COM)s/@.*//
           RULE:[1:$1@$0](.*@DS.CORP.YAHOO.COM)s/@.*//
           RULE:[2:$1@$0](.*@Y.CORP.YAHOO.COM)s/@.*//
           RULE:[1:$1@$0](.*@Y.CORP.YAHOO.COM)s/@.*//
           RULE:[2:$1@$0]([jt]t@.*YGRID.YAHOO.COM)s/.*/mapred/
           RULE:[2:$1@$0]([nd]n@.*YGRID.YAHOO.COM)s/.*/hdfs/
           RULE:[2:$1@$0](mapred@.*YGRID.YAHOO.COM)s/.*/mapred/
           RULE:[2:$1@$0](hdfs@.*YGRID.YAHOO.COM)s/.*/hdfs/
           RULE:[2:$1@$0](mapred@.*YGRID.YAHOO.COM)s/.*/mapred/
           RULE:[2:$1@$0](hdfs@.*YGRID.YAHOO.COM)s/.*/hdfs/
           DEFAULT</value></property>
   </configuration>

HDFS
----

HDFS is similar to HBase except the configuration is much simpler.

`yahoo examples <https://git.ouroath.com/storm/storm/tree/master-security/examples/yahoo-examples>`_ in the storm repo includes an example topology accessing HDFS.  This particular one uses storm-hdfs to access it, but you can access HDFS directly if you prefer.  The important things to remember to do are

first include the storm client conf as a dependency.

.. code-block:: xml

   <dependency>
     <groupId>yahoo.yinst.storm_hadoop_client_conf</groupId>
     <artifactId>storm_hadoop_client_conf</artifactId>
     <version>1.0.0</version>
   </dependency>

Second make sure you create your uber jar using the shade plugin.

.. code-block:: xml

   <plugin>
     <groupId>org.apache.maven.plugins</groupId>
     <artifactId>maven-shade-plugin</artifactId>
     <version>1.4</version>
     <configuration>
       <createDependencyReducedPom>true</createDependencyReducedPom>
     </configuration>
     <executions>
       <execution>
         <phase>package</phase>
         <goals>
           <goal>shade</goal>
         </goals>
         <configuration>
           <finalName>${artifactId}-${version}-jar-with-dependencies</finalName>
           <transformers>
             <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
             <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
               <mainClass></mainClass>
             </transformer>
           </transformers>
         </configuration>
       </execution>
     </executions>
   </plugin>

This allows the hadoop client to be packaged properly.  It uses service loaders, and the assembly plugin does not combine the service loader config files properly.  If you make this mistake you will get an error about not knowing how to handle "hdfs://"

Finally you need to use a fully qualified path to get the FileSystem, and ideally access it as well.

.. code-block:: xml
   Path path = new Path("hdfs://mithrilred-nn1.red.ygrid.yahoo.com:8020/");
   Configuration conf = new Configuration();
   FileSystem fs = path.getFileSystem(conf);


YkeyKey
-------

The preferred way to get YKeyKey data to your topology is to use Athenz to authenticate with YKeyKey and pull the credentials down yourself.  See documentation 
`How-To: Use ykeykey in GRID <https://yahoo.jiveon.com/docs/DOC-128583>`_

It should come down to setting up an Athenz domain and service that you can use an SIA server to get role tokens for. Once you have that setup you need to setup 
your ykeykey keygroup to allow your domain to access this. You can do this through the CKMS UI for the keygroup. Each keygroup has a field in the UI for the Athenz 
domain where you would put this role. Once you have that setup you need to grant the ckms access to your service so they can verify you are you. To do this run::

    zms-cli -d $DOMAIN add-provider-role-member paranoids.ppse.ckms.ykeykey_prod $KEY_GROUP access $DOMAIN.$SERVICE

If you want to do it for a different ckms like corp or alpha replace the _prod in the command above with _corp or _alpha. The DOMAIN is the Athenz domain you setup. 
The SERVICE is the Athenz service you setup and the $KEY_GROUP is the key group you setup just above.

Once you have all of this done you need to write a very small amount of code to access the ckms from storm, and a bit of configuration to have the Athenz credentials fetched on your behalf.

First you need to add com.yahoo.cryptogen:credbank:0.1.20 as a dependency to your topology. It includes the code needed to fetch your keys from the ckms. Next when your bolt or spout is 
initialized you will want to create a ZTSClient and YKeyKeyBank instance to give you access to these credentials.


.. code-block:: java

   import com.yahoo.auth.zts.ZTSClient;
   import com.yahoo.cryptogen.credbank.ykeykey.YKeyKeyBank;
   import com.yahoo.cryptogen.Credential;

   ...

   ZTSClient ztsClient = new ZTSClient(DOMAIN, SERVICE);
   YKeyKeyBank bank = new YKeyKeyBank("corp", Arrays.asList(KEY_GROUP), ztsClient);
   bank.start();
   //The background thread can take a while to read all of the creds so do the manualRefresh to be sure we are ready to go
   bank.manualRefresh();

   ...

   //Wen you need a credential you can call
   Credential cred = bank.get(KEY);

   ...

   //when closing your bolt/spout you probably want to call
   bank.stop();
   ztsClient.close();

When launching your topology, and periodically as you push new credentials you will want to configure AutoAthens to fetch the Athenz tokens for you and push them to your topology. 
The tenant domain and service are the domain and service you configured above. The role you want to configure Athenz to fetch is “paranoids.ppse.ckms".


Athenz
------

Athenz support for Storm is provided by AutoAthens plugin. It is similar to other automatic credentials plugins where it will pull package credentials for you on a 
gateway/launcher box and forward them to your running topology. In this case, AutoAthens will fetch RoleTokens using the ZTSClient Java API and then on the worker 
side insert them into the token cache for the ZTSClient. 

This means that unlike AutoYCA, code written to use the ZTSClient can run unmodified on Storm clusters.

Athenz supports several different ways of authenticating, aka telling Athenz who you are, but because AutoAthens was written initially for CMS and CMS only supports 
authenticating using the SIA server we have done the same thing. If you have a use case that needs other forms of authentication please feel free to reach out to 
the storm team. Setting up and running an SIA server is beyond the scope of this document. But to make this work you need the SIA server configured with the 
private key(s) for the domain/service(s) you need to authenticate as running on your launcher box.

Once you have your launcher box setup you need to tell AutoAthens the RoleTokens you want to fetch and the tenant domain/service you want to fetch them with. 
Conceptually the tenant domain/service is who you are, the role and role-suffix indicate who you want to talk to. This can be done by setting the yahoo.athens.roles 
config to be a list of maps in the form:

.. code-block:: java
   {“role”: <role>, “suffix”: <role-suffix>, “trust-domain”:<trust-domain>, “tenant-domain”: <tenant-domain>, “tenant-service”: <tenant-service>}

Role is required and is the role that you are fetching the token for, aka who you want to talk to.

Suffix and trust-domain are optional. It is beyond the scope of this document to describe how Athenz uses them.

tenant-domain and tenant-service are the domain and the service that the client is a part of and will be used to fetch the role token. 
These are required unless defaults are provided by the storm configs yahoo.athens.tenant.domain and yahoo.athens.tenant.service respectively. 
These represent who you are, or how you authenticated with Athenz.

If the only thing in the map is the “role” you can replace the map with the string name of the role.

For Example::

   storm upload_credentials MyTopology -c yahoo.athens.tenant.domain=”my.storm.prod.domain” -c yahoo.athens.tenant.service=”client” -c yahoo.athens.roles=’[“remote.special.service”,  “some.other.remote.service”, {“role”: “final.remote.service”, “tenant-service”: “test.client”, “tenant-domain”: “my.storm.test”}]’


would fetch and forward three role tokens. One each for “remote.special.service” and “some.other.remote.service” using “my.storm.prod.domain:client” and one for “final.remote.service” using “my.storm.test:test.client”.

When fetching a RoleToken, Athenz requires you to specify a time range that the token should be good for. If Athenz cannot find a valid token with that time range 
in its cache it will try to fetch a new one from the SIA server. This can be problematic because if we ship a RoleToken to your topology with an expiration 
time that is either too far in the future or not far enough the token will be rejected. This can be seen by looking in the logs for messages like::

   LookupRoleTokenInCache: role-cache-lookup key: p=something;d=something.else token-expiry: 85949 req-min-expiry: 86399 req-max-expiry: 86400 client-min-expiry: 900 result: expired

If this happens it either means that you are not pushing new tokens frequently enough using upload_credentials or the client in your topology is asking for a range 
that is not compatible with the range of tokens that AutoAthens uses. Currently AutoAthens will fetch a token that is good for between 1 day and 1 second less than 
1 day. It does this to be sure that we get a token with a very strict expiry (not too long and not too short), and it is expected that you will push a new token 
twice a day. This is because CMS requests a token that is good for between 2 hours and 1 day. If the token used is good for longer than 1 day we risk the token 
being rejected, and you need to push a new one before it only has 2 hours left or you risk it expiring. If you do have a client where the 1 day expiry AutoAthens 
uses is not compatible please reach out to the Storm team and we can make that configurable as well.


Athenz TSL Certs using AutoSSL
==============================

Storm has an AutoSSL plugin similar to AutoAthens that you can use to send both private and public key files to your topology. AutoAthens is specific to role tokens. 
Role tokens have their own API that is controlled by the athenz team and as such we can plug into it to make accessing the role tokens fairly transparent to the end 
user. Athenz TLS certs are not nearly as transparent because there is no java API for fetching them and they tend to be used just by reading them from a file. As 
such AutoSSL just provides the ability to ship small files securely to your topology. You can specify which files you want to ship by setting the config 
ssl.credential.files to be a list of strings that are paths to the files. The exact location of these files is specific to Athens and the SIA server. I don't know 
all of the details of this, but I believe that they are at /var/lib/sia/keys/ but https://git.ouroath.com/pages/athens/athenz-guide/service_x509_credentials/ should 
explain more of how to generate them. Any file that you ship will show up in the current working directory of the worker process with the same name as the local file.

Because Athenz is doing mutual authentication using SSL you need to make sure you ship the public and private keys for the role you want to use. The default java 
trust-store that we ship with storm is not guaranteed to allow you to authenticate with the server. It may but that is tied to the version of java that is shipped 
with storm, and we are rather conservative about upgrading java versions. So please make sure you install the yahoo_certificate_bundle package as described here 
https://git.ouroath.com/pages/athens/athenz-guide/athenz_ca_certs/ and ship one of the truststores in /opt/yahoo/share/ssl/certs.

One of the key differences between Athenz TLS certs and most other TLS certs is that the athenz ones expire after about 30 days. AutoSSL allows you to ship new 
versions of the files when you run storm upload-credentials, but most web servers/clients don't support switching certs wile the system is live. To work around this 
the Athenz team has provided an SSLContext that for most java web servers and clients should work, but you should also explicitly test this with whatever server/client 
you are using.

All of the following came from https://git.ouroath.com/pages/athens/athenz-guide/client_side_x509_credentials/

You might want to check with the Athenz team to be sure the versions and everything are up to date.

Maven dependency:

.. code-block:: java
       <dependency>
           <groupId>com.yahoo.athenz</groupId>
           <artifactId>athenz-cert-refresher</artifactId>
           <version>1.7.33</version>
       </dependency>

How to use it:

.. code-block:: java

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

A pointer to the actual code:

https://github.com/yahoo/athenz/blob/739554711a2b0e0bc5c8afe5e666ba637b46c896/libs/java/cert_refresher/src/main/java/com/oath/auth/KeyRefresher.java
https://github.com/yahoo/athenz/blob/739554711a2b0e0bc5c8afe5e666ba637b46c896/libs/java/cert_refresher/src/main/java/com/oath/auth/KeyManagerProxy.java
https://github.com/yahoo/athenz/blob/739554711a2b0e0bc5c8afe5e666ba637b46c896/libs/java/cert_refresher/src/main/java/com/oath/auth/TrustManagerProxy.java






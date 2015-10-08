============================
Authorization/Authentication
============================

.. Status: first draft. Needs tighter writing and a technical review.

Authorization
=============

By default, you will have permissions to manipulate and see any topology that you 
launch. If you want to allow others to also manipulate this topology you need to 
set the topology configuration ``topology.users`` to be a list of all the users you want to 
be able to manipulate this topology. This must be set in the configuration you pass 
to ``StormStarter`` when launching your topology.

.. note:: Distributed PRC (DRPC) Authorization is still being worked on, but should be done soon.

Plugin API
----------

A plugin API has also been added to block unwanted operations, along with some reasonable implementations.

For example ``SimpleACLAuthorizer`` for Nimbus.

- Can configure administrators to do anything.
- Users that the supervisors are running as.
- Topology can also configure who is allowed to kill or re-balance it.


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

Storm at Yahoo uses Kerberos to authenticate the end user with Nimbus. For hosted 
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

Zookeeper Authentication
------------------------

We have secured access to Zookeeper as well. For workers, this is done through a 
MD5 digest authentication. For most topologies, you should not need to worry about 
it at all as a new secret will be generated for each topology launched. But, if 
you are using Trident or any other transactional state topology, you will want to 
provide that secret yourself. Otherwise, later topologies will not have access to 
the previous topologies state to pick up where it left off. To set this payload, 
you want to be set the following::

    storm.zookeeper.topology.auth.scheme: digest
    storm.zookeeper.topology.auth.payload: <username>:<password>

Authentication for External Services
------------------------------------

Multi-tenant storm supports pushing credentials as well as tickets and tokens when launching 
a topology and on demand when they are about to expire.

This code has not been accepted back into open source Storm yet, so if you wish 
to use it, you need ``ystorm`` to be a Maven dependency. You do this
by adding the dependency below to ``pom.xml``:: 

    <dependency>
      <groupId>yahoo.yinst.ystorm</groupId>
      <artifactId>storm-core</artifactId>
      <version>0.9.0_wip21.205</version>
      <!-- keep storm out of the jar-with-dependencies -->
      <scope>provided</scope>
    </dependency>

Automatic Credentials Push
##########################

As of ``ystorm-0.9.0_wip21.205`` credentials can be gathered automatically by the 
client and put into the current Java Subject on the workers. By default, on 
multi-tenant clusters that support this a plugin for pushing the TGT and setting 
it up so that it is compatible with Hadoop should be automatic. There is also a 
plugin that will renew the Granting Ticket Ticket (TGT), so you will only have to push a new TGT once or 
twice a week. If you have other credentials that you would like to automatically 
push, `ask the Storm team <email:yahoo-storm-team@yahoo-inc.com>`_ how to set this up.

These plugins do not schedule anything to push credentials periodically, so before 
they are about to expire you will need to have something cron, etc., that will log 
the user in through a keytab and run ``storm upload-credentials <topology-name>`` or 
call ``StormSubmitter.pushCredentials`` programatically. Both of these options can 
be combined with manually populated credentials.


Manual Credentials Push
#######################

To have bolts or spouts notified when credentials change, you will need to have 
them implement ``ICredentialsListener``. The ``setCredentials`` method will 
be called before the methods ``prepare`` or ``open`` is called. It will also be called any time new 
credentials are pushed to a topology. The credentials are just a mapping of string 
to string. This gives a lot of flexibility in what is and is not pushed.

To push new credentials, use the command-line tool::

    storm upload-credentials <topology-name> [-f <cred-file.properties>] [<cred-key> <cred-value>]*

As of ``ystorm-0.9.0_wip21.205``, you can use the ``StormSubmitter.pushCredentials`` API. 
(If you have to do this on an older cluster please see the ystorm team for your options.)

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

We've discussed authorization and authentication for Storm at Yahoo. In this section, 
we'll look at using credentials for multi-tenant Storm to access external services. 

If you are running your own cluster without multi-tenancy, you can simply use 
credentials in a more traditional way with host-based YCA v1, ``ykeykey``, etc. For 
multi-tenant Storm, we do not plan on installing any credentials for individuals on the cluster. 
You will have to transmit those credentials with the topology.

There are numerous services used at Yahoo that require authentication to be able to access them. 
We are working on proper solutions and examples for many of these. If you need 
more of them, please mention it when `on-boarding <../onboarding>`_ 
or `file a yo/ystorm-request <http://yo/ystorm-request>`_
so that we can work on it with the other teams involved.

Credentials API
---------------

A new API has been added that allows owners of a topology to send credentials to 
a topology when it is launched and to send updated credentials periodically
before the old ones expire. This API has not been accepted back into open source 
yet, so to use it, you will need to compile your topology with
``ystorm`` 0.9.0_wip21.205 or higher.

.. code-block:: xml

   <dependency>
       <groupId>yahoo.yinst.ystorm</groupId>
       <artifactId>storm</artifactId>
       <version>0.9.0_wip21.205</version>
   </dependency>

For most cases, plugins that automatically push credentials on your behalf should 
allow your topology to run unchanged as of ``ystorm`` 0.9.0_wip21.205. These plugins support 
pushing your TGT out and allowing services like Hadoop and HBASE to access it 
unchanged.

The manual API will send a ``Map<String,String>`` to spouts and bolts that are 
listening for it. There is little convention about how keys and values are stored 
in the Map. As more types of credentials are used, we hope to expand the set of 
plugins that automatically push them with a small amount of configuration.

Credentials Push
################

To submit a topology with this new API you would run something like the following:

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
`ICredentialsListener <https://git.corp.yahoo.com/storm/storm/blob/master-security/storm-core/src/jvm/backtype/storm/ICredentialsListener.java>`_ 
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

As of ``ystorm-0.9.0_wip21.225`` code has been added to Storm to automatically fetch 
and push YCA certificates on your behalf. To use this, you need to know about the
three configurations in the table below.

.. csv-table:: YCA Configurations
   :header: "Name", "Configuration"
   :widths: 15, 40
   
   "``yahoo.autoyca.appids``", "This is the config that you will interact with the most. It is a comma separated list of YCAv2 application IDs that should be fetched and passed to the topology."
   "``yahoo.autoyca.v1appid``",	"If set this is the YCAv1 cert that should be used when fetching YCAv2 certs. If not set kerberos will be used instead."
   "``yahoo.autoyca.proxyappid``", "This is the role for the http proxies that should be used with this YCAv2 cert. If not set YCA will guess based off of the colo you are in. It almost always gets this correct."

On the worker side, you can fetch the most up-to-date certificate using static methods in 
the ``com.yahoo.storm.security.yca.AutoYCA`` class. This class is in a separate Yahoo-
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

The older way of doing this is not recommended as it is much more complex and error prone. 


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
code we wrote called AutoTGT will take your TGT and send it to the processes
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

`yahoo examples <https://git.corp.yahoo.com/storm/storm/tree/master-security/examples/yahoo-examples>`_ in the storm repo includes an example topology accessing HDFS.  This particular one uses storm-hdfs to access it, but you can access HDFS directly if you prefer.  The important things to remember to do are

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

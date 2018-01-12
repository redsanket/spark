Oozie Monitoring
================

.. 04/23/15: Rewrote.
.. 05/15/15: Edited.
.. 01/11/18: Edited for CMSv2


The following sections discuss how to monitor Oozie jobs, get notifications,
connecting to the `Cloud Messaging Service <http://developer.corp.yahoo.com/product/Cloud%20Messaging%20Service>`_, 
and writing a listener for getting notifications.

SLA Monitoring
--------------

Critical jobs often have to meet SLA requirements. The SLAs can 
be a time requirement, such as a maximum allowed time limit associated with when the 
job should start, by when should it end, and its duration of run. You can
define SLA limits for Oozie Workflows in the application ``definition.xml``.

Oozie can now actively monitor the state of these 
SLA-sensitive jobs and send out notifications for SLA meets and misses.

To learn how to use SLA monitoring, see `Oozie SLA monitoring <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_SLAMonitoring.html>`_.


CMS Notifications for Job and SLA
---------------------------------

CMS is the Cloud Messaging Service, which has an API
for sending messages between two or more clients. You can use CMS
to fetch job status messages from the Yahoo Cloud Messaging Service (CMS).
Apache Oozie has only support for JMS which is the Java Messaging Service and supports systems like ActiveMQ which implement JMS.
At Yahoo, Oozie worked with CMS v1 which was JMS compliant before EOL for it was announced on Feb 2018. Now only CMS v2 is
supported which has its own set of APIs and is not JMS compliant. So the section
`JMS Notifications <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_JMSNotifications.html>`_ in Apache
documentation is not relevant for Yahoo Oozie.


In the following sections, we will take a look at how to connect to CMS,
what code changes you need to make, and how to write a listener to fetch messages from CMS.


Connecting to CMS
~~~~~~~~~~~~~~~~~

CMS is a centralized multi-tenant cloud service that provides low latency, world-wide delivery,
deliver-at-least-once, publish and subscribe semantics at Yahoo scale.
CMS is designed for high performance messaging for application integration, with Yahoo specific
requirements such as monitoring and security  based on YAMAS and YCA respectively.

See also the `Cloud Messaging Service - Tutorial <https://docs.google.com/document/d/1og5FQXFJhucBFOLvlJE1S64A7_b5_413ToWaRaVu7a8/edit#heading=h.ijmxb7godl86>`_
for more information.

.. _connect_cms-acls:

ACLs
****

Based on the colo you are using, ensure you have ACLs open to 
CMS on ports 4080 and 6650 on the following:: ``brokerv2.messaging.gq1.yahoo.com``,
``brokerv2.messaging.ne1.yahoo.com``, or ``brokerv2.messaging.bf1.yahoo.com``
The macro ``CLOUDMESSAGING::PROD_BROKER`` opens the ACL.

If you are in SZ50 ``PROD_MAIN``, you do not need ACLs to talk to CMS in the same colo.
You do currently need to open ACLs to connect to CMS cross-colo.


YCA
****

The messages published by grid services such as Oozie to the ``ygrid`` namespace in CMS can 
only by consumed from hosts in the role ``yahoo.griduser.ALL``, which is a super role 
including all the ``yahoo.griduser.<headlessusername>`` roles. 
If your headless user role is not included in the ``yahoo.griduser.ALL`` role, 
file a ticket with Grid SE and have it included.

Most of the grid users will already have their launcher boxes configured in a role 
of the format ``yahoo.griduser.<headlessusername>`` . For example: ``yahoo.griduser.apollog``.

If you do not have your launcher box there, add it. This is the same when using the ``v2`` namespace.

.. note:: You cannot listen to CMS messages from gateways as long running processes are 
          not allowed to run on gateways. Also ACLs to CMS are not open from a grid gateway, 
          and they are not part of the YCA roles, which have permissions to consume messages. 
          So, to listen to CMS, you need to have your own host.

Namespace and Topic Name
************************

The topic name using the ``v2`` namespace is ``non-persistent://grid/#{COLO}/#{CLUSTER}#{COLOR}-oozie-v2/user.user_name``.
For example, the headless user ``apollog`` in Jet Blue cluster would use the following:
``non-persistent://grid/gq1/jetblue-oozie-v2/user.apollog``

.. note:: In the ``v1`` namespace (using JMS), the topic name was ``ygrid:oozie.${CLUSTER}-${COLOR}.user.user_name``. From
02/14/2018, CMS team is dropping support for CMSv1.

The topic name prefix can be programmatically obtained using Oozie API. See the example 
:ref:`Writing a Listener to Consume Messages <write_listener>`.
  
  .. code-block:: java 

     KerbOozieClient oc = new KerbOozieClient(url);
     JMSConnectionInfo jmsInfo = oc.getJMSConnectionInfo();
     String topicPrefix = jmsInfo.getTopicPrefix();


Code Changes to Work With CMS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~



#. Place the ``cloud-messaging-client-java.jar`` in the classpath by downloading it as a Maven artifact or
   installing `yinst package <https://dist.corp.yahoo.com/by-package/cloud_messaging_client_java/>`_.

#. Add the following dependency to your ``pom.xml``:

   .. code-block:: xml

        <dependency>
            <groupId>yahoo.yinst.cloud_messaging_client_java</groupId>
            <artifactId>cloud-messaging-client-java</artifactId>
            <version>1.1.7</version>
            <scope>compile</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.oozie</groupId>
            <artifactId>yoozie-client</artifactId>
            <version>4.4.7.4</version>
            <scope>compile</scope>
        </dependency>


#. To configure for YCA authentication, the hosts configured in the ``griduser`` namespace 
   role in RolesDB can consume the Oozie JMS messages.

   - Install ``yjava_yca`` (0.20.x or higher) and ``yca_client_certs`` on your client.
   - Add ``-Djava.library.path=/home/y/lib or /home/y/lib64`` (if 64-bit JDK) as argument 
     while launching your Java program and have ``/home/y/lib/yjava_yca.jar`` in the ``CLASSPATH``.
   - Use the YCA role name ``yahoo.griduser.ALL`` in ``ClientConfiguration`` for creating

  .. code-block:: java

     com.yahoo.cloud.messaging.client.api.ClientConfiguration config = new com.yahoo.cloud.messaging.client.api.ClientConfiguration();
     config.setAuthentication(com.yahoo.cloud.messaging.client.api.Authentication.ycaV1("yahoo.griduser.ALL"));
     CmsClient cmsClient = CmsClient.create(brokerUrlCmsV2, config);



.. _write_listener:

Writing a Listener to Consume Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Consuming Notifications <http://oozie.apache.org/docs/4.0.0/DG_JMSNotifications.html#Consuming_Notifications>`_ 
offers a guideline for writing Java code to listen for the JMS message ``broken`` (CMS in this case) and 
consume messages about your Oozie jobs.

Below is a working code snippet to connect through the Oozie client 
using Kerberos authentication and a JMS message listener.

.. code-block:: java

    import com.yahoo.cloud.messaging.client.api.CmsClient;
    import com.yahoo.cloud.messaging.client.api.CmsClientException;
    import com.yahoo.cloud.messaging.client.api.Consumer;
    import com.yahoo.cloud.messaging.client.api.ConsumerConfiguration;
    import com.yahoo.cloud.messaging.client.api.Message;
    import com.yahoo.cloud.messaging.client.api.MessageListener;
    import com.yahoo.oozie.client.event.cms.CMSMessagingUtils;
    import com.yahoo.oozie.security.authentication.client.KerberosAuthenticator;
    import org.apache.hadoop.security.authentication.client.Authenticator;
    import org.apache.oozie.AppType;
    import org.apache.oozie.client.AuthOozieClient;
    import org.apache.oozie.client.JMSConnectionInfo;
    import org.apache.oozie.client.OozieClientException;
    import org.apache.oozie.client.event.Event.MessageType;
    import org.apache.oozie.client.event.jms.JMSHeaderConstants;
    import org.apache.oozie.client.event.message.CoordinatorActionMessage;
    import org.apache.oozie.client.event.message.SLAMessage;
    import org.apache.oozie.client.event.message.WorkflowJobMessage;

    import java.util.HashMap;
    import java.util.Map;
    import java.util.Properties;
    import java.util.Scanner;

    public class OozieMessages implements MessageListener {

        private String oozieUrl;
        private String topicStr;

        public static void main(String args[]) throws OozieClientException, CmsClientException, InterruptedException {

            OozieMessages oozieMessages = new OozieMessages();
            oozieMessages.oozieUrl = args[0];
            oozieMessages.topicStr = args[1];
            oozieMessages.consumeMessages();
        }

        private void consumeMessages() throws OozieClientException, CmsClientException, InterruptedException {
            CmsClient cmsClient = null;
            Consumer consumer = null;
            try {
                KerbOozieClient oc = new KerbOozieClient(oozieUrl);
                JMSConnectionInfo cmsInfo = oc.getJMSConnectionInfo();
                Properties jndiProperties = cmsInfo.getJNDIProperties();

                String topicPrefix = cmsInfo.getTopicPrefix();
                String topicPattern = cmsInfo.getTopicPattern(AppType.WORKFLOW_JOB);
                String topic = null;
                // Following code checks if the topic pattern is
                // 'username', then the topic name is set to the actual user submitting
                // the job
                if (topicPattern.equals("${username}")) {
                    // The topics naming convention is - non-persistent://grid/#{COLO}/#{CLUSTER}#{COLOR}-oozie-v2/user.<username> where
                    // grid/#{COLO}/#{CLUSTER}#{COLOR}-oozie-v2 is the CMS namespace and the rest is the topic name.
                    // For eg: non-persistent://grid/gq1/jetblue-oozie-v2/user.apollog
                    topic = topicPrefix + topicStr;
                }

                com.yahoo.cloud.messaging.client.api.ClientConfiguration config = new com.yahoo.cloud.messaging.client.api.ClientConfiguration();
                config.setAuthentication(com.yahoo.cloud.messaging.client.api.Authentication.ycaV1("yahoo.griduser.ALL"));
                String brokerUrl = jndiProperties.getProperty("cms.broker.url");
                cmsClient = CmsClient.create(brokerUrl, config);

                ConsumerConfiguration conf = new ConsumerConfiguration();
                conf.setMessageListener(this);

                // Subscribe to the topic
                consumer = cmsClient.subscribe(topic, "oozie-subscriber-yourheadlessusername", conf);
                System.out.println("*** Submit job now.....");
                Thread.sleep(120 * 1000);
                Scanner sc = new Scanner(System.in);
                System.out.println("*** Type 'exit' to stop listener....");
                while (true) {
                    if (sc.nextLine().equalsIgnoreCase("exit")) {
                        System.exit(0);
                    }
                }
            }
            catch (Exception e) {
                e.printStackTrace(System.err);
                throw e;
            }
            finally {
                if (consumer != null) {
                    consumer.close();
                }
                if (cmsClient != null) {
                    cmsClient.close();
                }
            }

        }

        @Override
        public void received(Consumer consumer, Message message) {
            try {
                if (message.getProperty(JMSHeaderConstants.MESSAGE_TYPE).equals(MessageType.SLA.name())) {
                    SLAMessage slaMessage = CMSMessagingUtils.getEventMessage(message);
                    System.out.println("*** [Message]: " + slaMessage.getSLAStatus());
                }
                else if (message.getProperty(JMSHeaderConstants.APP_TYPE).equals(AppType.WORKFLOW_JOB.name())) {
                    WorkflowJobMessage wfJobMessage = CMSMessagingUtils.getEventMessage(message);
                    System.out.println("*** [Message]: " + wfJobMessage.getEventStatus());
                }
                else if (message.getProperty(JMSHeaderConstants.APP_TYPE).equals(AppType.COORDINATOR_ACTION.name())) {
                    CoordinatorActionMessage caActionMsg = CMSMessagingUtils.getEventMessage(message);
                    System.out.println("*** [Message]: " + caActionMsg.getEventStatus());
                }
            }
            catch (Exception e) {
                e.printStackTrace(System.err);
            }

        }

        static class KerbOozieClient extends AuthOozieClient {

            public KerbOozieClient(String oozieUrl) {
                super(oozieUrl, "KERBEROS");
            }

            @Override
            protected Map<String, Class<? extends Authenticator>> getAuthenticators() {
                Map<String, Class<? extends Authenticator>> authClasses = new HashMap<String, Class<? extends Authenticator>>();
                authClasses.put("KERBEROS", KerberosAuthenticator.class);
                return authClasses;
            }
        }
    }


Run the Listener

    .. code-block:: bash

        java -cp <<jar containing OozieMessages class>>:/home/y/lib/jars/cloud-messaging-client-java.jar:/home/y/lib/jars/yjava_yca
        .jar:/home/y/var/yoozieclient/lib/*  OozieMessages https://jetblue-oozie.blue.ygrid.yahoo.com:4443/oozie/ apollog


Troubleshooting
~~~~~~~~~~~~~~~

Unauthorized exception on subscribe
***********************************

    .. code-block:: java

        java.lang.IllegalStateException: com.yahoo.cloud.messaging.client.api.CmsClientException: org.apache.pulsar.client.api.PulsarClientException: HTTP get request failed: Unauthorized
            at com.yahoo.slingstone.event.pipeline.storm.spout.BasicLESSpout.initCMSV2(BasicLESSpout.java:196)
            at com.yahoo.slingstone.event.pipeline.storm.spout.BasicLESSpout.open(BasicLESSpout.java:140)
            at com.yahoo.slingstone.event.pipeline.storm.spout.GMPLESSpout.open(GMPLESSpout.java:33)
            at com.yahoo.slingstone.event.pipeline.batch.CommonSpout.open(CommonSpout.java:45)
            at backtype.storm.daemon.executor$fn__7093$fn__7108.invoke(executor.clj:584)
            at backtype.storm.util$async_loop$fn__551.invoke(util.clj:488)
            at clojure.lang.AFn.run(AFn.java:22)
            at java.lang.Thread.run(Thread.java:745)
        Caused by: com.yahoo.cloud.messaging.client.api.CmsClientException: org.apache.pulsar.client.api.PulsarClientException: HTTP get request failed: Unauthorized
            at com.yahoo.cloud.messaging.client.impl.CmsClientImpl.subscribe(CmsClientImpl.java:104)

Please make sure your role is of the format ``yahoo.griduser.<headlessusername>`` .



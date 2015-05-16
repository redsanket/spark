Oozie Monitoring
================

.. 04/23/15: Rewrote.
.. 05/15/15: Edited.

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
SLA-sensitive jobs and send out notifications for SLA mets and misses.

To learn how to use SLA monitoring, see `Oozie SLA monitoring <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_SLAMonitoring.html>`_.


JMS Notifications for Job and SLA
---------------------------------

JMS is the Java Messaging Service, which has an API 
for sending messages between two or more clients. You can use JMS
to fetch job status messages from the Yahoo Cloud Messaging Service (CMS).
CMS is a hosted service that internally uses `ActiveMQ <http://activemq.apache.org/>`_ 
and is YCA-protected access.

In the following sections, we will take a look at how to use JMS to connect to CMS, 
what code changes you need to make, and how to write a listener to fetch messages from CMS. 

We also recommend reading `JMS Notifications <http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_JMSNotifications.html>`_.

Connecting to CMS
~~~~~~~~~~~~~~~~~

CMS is a centralized multi-tenant cloud service that provides low latency, 
world-wide delivery, deliver-at-least-once, publish and subscribe semantics at Yahoo 
scale. It is 100% JMS compliant, and Yahoo plans for CMS to support post-JMS features 
such as different protocols, various language bindings as well as a new simplified 
high-performance asynchronous API.

See also the `Cloud Messaging Service - Client API Usage - Tutorial <http://twiki.corp.yahoo.com/view/Messaging/MessagingServiceTutorial#ACLs_AN1>`_ 
for more information.

.. _connect_cms-acls:

ACLs
****

Based on the colo you are using, ensure you have ACLs open to 
CMS on ports 4080, 61616, and 61617 on the following:: ``broker.messaging.gq1.yahoo.com``, 
``broker.messaging.ne1.yahoo.com``, or ``broker.messaging.bf1.yahoo.com`` 
The macro ``CLOUDMESSAGING::PROD_BROKER`` opens the ACL. 

If you are in SZ50 ``PROD_MAIN``, you do not need ACLs to talk to CMS in the same colo.
You do currently need to open ACLs to connect to CMS cross-colo. The new change to ACLs planned 
Yahoo-wide would mostly eliminate that.

Cross-Colo Access and SSL
*************************

If are connecting to CMS across colos, you should enable 
SSL. See `How can I use SSL to connect to CMS <http://twiki.corp.yahoo.com/view/Messaging/FAQs#How_can_I_use_SSL_to_connect_to_CMS_63>`_.
CMS team will get the `ycrypt <http://dist.corp.yahoo.com/by-package/ycrypt/>`_ 
exemption on that port (61617) but not the regular port (61616).

YCA
***

The messages published by grid services such as Oozie to the ``ygrid`` namespace in CMS can 
only by consumed from hosts in the role ``yahoo.griduser.ALL``, which is a super role 
including all the ``yahoo.griduser.<headlessusername>`` roles. 
If your headless user role is not included in the ``yahoo.griduser.ALL`` role, 
file a ticket with Grid SE and have it included.

Most of the grid users will already have their launcher boxes configured in a role 
of the format ``yahoo.griduser.<headlessusername>``. For example: ``yahoo.griduser.apollog``. 

If you do not have your launcher box there, add it. This is the same when using the ``v2`` namespace.

.. note:: You cannot listen to CMS messages from gateways as long running processes are 
          not allowed to run on gateways. Also ACLs to CMS are not open from a grid gateway, 
          and they are not part of the YCA roles, which have permissions to consume messages. 
          So, to listen to CMS, you need to have your own host.

Namespace and Topic Name
************************

Oozie migrated to the ``v2`` namespace after the v4.3.1 and v4.4.1 releases.
The topic name using the ``v2`` namespace is ``topic://grid/#{COLO}/oozie.#{CLUSTER}-#{COLOR}/user.user_name``.
For example, the headless user ``apollog`` in Mithril Blue cluster would use the following: ``topic://grid/gq1/oozie.mithril-blue/user.apollog`` 

.. note:: In the ``v1`` namespace, the topic name was ``ygrid:oozie.${CLUSTER}-${COLOR}.user.user_name``.

The topic name prefix can be programmatically obtained using Oozie API. See the example 
:ref:`Writing a Listener to Consume Messages <write_listener>`.
  
  .. code-block:: java 

     KerbOozieClient oc = new KerbOozieClient(url);
     JMSConnectionInfo jmsInfo = oc.getJMSConnectionInfo();
     String topicPrefix = jmsInfo.getTopicPrefix();


Code Changes to Work With CMS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To connect to CMS at Yahoo, you will need to make a few additional steps to the 
`sample code <http://oozie.apache.org/docs/4.0.0/DG_JMSNotifications.html#Example>`_ 
provided in  the Apache documentation. 


#. Place the ``cloud_messaging_client.jar`` in the classpath by downloading it as a Maven artifact or 
   installing yinst package.

#. Add the following dependency to your ``pom.xml``:

   .. code-block:: xml

      <dependency>
        <groupId>yahoo.yinst.cloud_messaging_client</groupId>
        <artifactId>cloud-messaging-client</artifactId>
        <version>0.3</version>
        <scope>provided</scope>
      </dependency>


#. To configure for YCA authentication, the hosts configured in the ``griduser`` namespace 
   role in RolesDB can consume the Oozie JMS messages.

   - Install ``yjava_yca`` (0.20.x or higher) and ``yca_client_certs`` on your client.
   - Add ``-Djava.library.path=/home/y/lib or /home/y/lib64`` (if 64-bit JDK) as argument 
     while launching your Java program and have ``/home/y/lib/yjava_yca.jar`` in the ``CLASSPATH``.
   - Set the ``java.naming.security.principal`` ``JNDI`` property to the YCA role name ``yahoo.griduser.ALL``.

  .. code-block:: java 

     Properties jndiProperties = jmsInfo.getJNDIProperties();
     jndiProperties.put("java.naming.security.principal", "yahoo.griduser.ALL");

.. _write_listener:

Writing a Listener to Consume Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Consuming Notifications <http://oozie.apache.org/docs/4.0.0/DG_JMSNotifications.html#Consuming_Notifications>`_ 
offers a guideline for writing Java code to listen for the JMS message ``broken`` (CMS in this case) and 
consume messages about your Oozie jobs.

Below is a working code snippet to connect through the Oozie client 
using Kerberos authentication and a JMS message listener.

.. code-block:: java

   import java.io.IOException;
   import java.util.Properties;
   import java.util.Scanner;

   import javax.naming.*;
   import javax.jms.*;

   import org.apache.oozie.AppType;
   import org.apache.oozie.client.JMSConnectionInfo;
   import org.apache.oozie.client.OozieClient;
   import org.apache.oozie.client.AuthOozieClient;
   import org.apache.oozie.client.OozieClientException;
   import org.apache.oozie.client.event.Event.MessageType;
   import org.apache.oozie.client.event.jms.JMSHeaderConstants;
   import org.apache.oozie.client.event.jms.JMSMessagingUtils;
   import org.apache.oozie.client.event.message.SLAMessage;
   import org.apache.oozie.client.event.message.WorkflowJobMessage;
   import org.apache.hadoop.security.authentication.client.Authenticator;
   import com.yahoo.oozie.security.authentication.client.KerberosAuthenticator;
   import java.net.URL;
   import java.util.HashMap;
   import java.util.Map;

   public class OozieMessages implements MessageListener {

     String url, topicStr;
     public static void main(String args[]) {
       try {
         OozieMessages m = new OozieMessages();
         m.url = args[0];
         m.topicStr = args[1];
         m.consumeMessages();
       }
       catch (Exception e) {
         e.printStackTrace(); //TODO handle
       }
     }
     public void consumeMessages() throws OozieClientException, JMSException, NamingException, InterruptedException {

       KerbOozieClient oc = new KerbOozieClient(url);
       JMSConnectionInfo jmsInfo = oc.getJMSConnectionInfo();
       Properties jndiProperties = jmsInfo.getJNDIProperties();
       jndiProperties.setProperty("java.naming.security.principal", "yahoo.griduser.ALL");
       Context jndiContext = new InitialContext(jndiProperties);
       System.out.println("*** [DEBUG] jndiContext properties: " + jndiContext.getEnvironment().toString());
       String connectionFactoryName = (String) jndiContext.getEnvironment().get("connectionFactoryNames");
       ConnectionFactory connectionFactory = (ConnectionFactory) jndiContext.lookup(connectionFactoryName);
       Connection connection = connectionFactory.createConnection();
       Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
       String topicPrefix = jmsInfo.getTopicPrefix();
       String topicPattern = jmsInfo.getTopicPattern(AppType.WORKFLOW_JOB);
       // Following code checks if the topic pattern is
       // 'username', then the topic name is set to the actual user submitting
       // the job
       String topicName = null;
       if (topicPattern.equals("${username}")) {
         topicName = topicStr;
       }
       // The topics naming convention is - ygrid:oozie.<cluster>.user.<username> where 
       // ygrid is the CMS namespace and the rest is the topic name.
       // For eg: ygrid:oozie.phazon-tan.user.gmon 
       Destination topic = session.createTopic(topicPrefix + topicName);
       MessageConsumer consumer = session.createConsumer(topic);
       consumer.setMessageListener(this);
       connection.start();
       System.out.println("*** Listener started......");
       // keep enough time to establish connection
       Thread.sleep(60 * 1000);
       System.out.println("*** Submit job now.....");
       Thread.sleep(120 * 1000);
       Scanner sc = new Scanner(System.in);
       System.out.println("*** Type 'exit' to stop listener....");
       while(true) {
         if (sc.nextLine().equalsIgnoreCase("exit")) {
           System.exit(0);
          }
       }
     }

     @Override
     public void onMessage(Message message) {
       try {
         if (message.getStringProperty(JMSHeaderConstants.MESSAGE_TYPE).equals(MessageType.SLA.name())) {
           SLAMessage slaMessage = JMSMessagingUtils.getEventMessage(message);
           System.out.println("*** [Message]: " + slaMessage.getSLAStatus());
         }
         else if (message.getStringProperty(JMSHeaderConstants.APP_TYPE).equals(AppType.WORKFLOW_JOB.name())) {
           WorkflowJobMessage wfJobMessage = JMSMessagingUtils.getEventMessage(message);
           System.out.println("*** [Message]: " + wfJobMessage.getEventStatus());
         }
       }
       catch (JMSException jmse) {
         jmse.printStackTrace(); //TODO handle
       }
       catch (IOException ioe) {
         ioe.printStackTrace(); //TODO handle
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

Troubleshooting
~~~~~~~~~~~~~~~

Connection Timed Out to Message Broker
**************************************

For example, the connection to ``prod1-broker10.messaging.bf1.yahoo.com:61616`` has timed out.
Make sure you have the necessary :ref:`ACL open as mentioned above <connect_cms-acls>`. Also, your box might be 
occluded behind a NAT, so you should use a gateway-like machine or launcher box.


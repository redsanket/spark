Oozie Monitoring
================


SLA Monitoring
--------------
Refer to Oozie SLA monitoring
http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_SLAMonitoring.html

JMS Notifications for Job and SLA
---------------------------------
Refer to Oozie Notifications
http://kryptonitered-oozie.red.ygrid.yahoo.com:4080/oozie/docs/DG_JMSNotifications.html

At Yahoo, we publish job status messages to Cloud Messaging Service (CMS) which is 
a hosted service that internally uses ActiveMQ and is YCA protected access.

Connecting to CMS
~~~~~~~~~~~~~~~~~

http://twiki.corp.yahoo.com/view/Messaging/CloudMessagingService is the homepage of the Yahoo! Cloud Messaging Service.

ACLs:
****

- Ensure you have ACLs open to CMS - broker.messaging.gq1.yahoo.com, broker.messaging.ne1.yahoo.com or broker.messaging.bf1.yahoo.com 
  based on the colo you are in. Ports: 4080, 61616 and 61617. The Macro to open ACL 
  to is CLOUDMESSAGING::PROD_BROKER. Refer to http://twiki.corp.yahoo.com/view/Messaging/MessagingServiceTutorial#ACLs_AN1 for more information.
- If you are in SZ50 PROD_MAIN, you do not need ACLs to talk to CMS in the same colo.
  You do currently need to open ACLs to connect to CMS cross-colo. The new change to ACLs planned Yahoo! wide would mostly eliminate that.

Cross-colo Access and SSL:
**************************

- If you are connecting to CMS cross-colo please do enable SSL (See the [[http://twiki.corp.yahoo.com/view/Messaging/FAQs#How_can_I_use_SSL_to_connect_to_CMS_63][CMS FAQ]) 
  in your client, as CMS team will get ycrypt exemption on that port (61617) but not the regular port (61616).

YCA:
****

- The messages published by grid services like Oozie to ygrid namespace in CMS can 
  only by consumed from hosts in the role yahoo.griduser.ALL which is a super role 
  including all the yahoo.griduser.<headlessusername> roles configured. By chance, 
  if your headless user role is not included in the griduser.ALL role file a ticket with Grid SE and get it included.
- Most of the grid users will already have their launcher boxes configured in a role 
  of the format yahoo.griduser.<headlessusername>. For eg: yahoo.griduser.apollog. 
  If you do not have your launcher box added there, please do add it. (Note that this is the same when using v2 name space)

.. note:: You cannot listen to CMS messages from gateways as long running processes are 
          not allowed to run on gateways. Also ACLs to CMS is not open from a Grid Gateway 
          and they are not part of the YCA roles which have permissions to consume messages. 
          So to listen to CMS you need to have your own host.

Name Space and Topic Name:
**************************

- Oozie migrates to v2 name space after v.4.3.1 and 4.4.1 releases.
- Topic name using v2 name space is topic://grid/#{COLO}/oozie.#{CLUSTER}-#{COLOR}/user.user_name
  for exmaple, "topic://grid/gq1/oozie.mithril-blue/user.apollog" when using headless, apollog in mithril blue cluster.
  (in v1 name space, topic name was ygrid:oozie.${CLUSTER}-${COLOR}.user.user_name)
- topic name prefix can be programmatically obtained using oozie API (find complete example in "Writing a listener to consume messages")
  
  .. code-block:: java 

     KerbOozieClient oc = new KerbOozieClient(url);
     JMSConnectionInfo jmsInfo = oc.getJMSConnectionInfo();
     String topicPrefix = jmsInfo.getTopicPrefix();


Code Changes to Work With CMS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Few additional steps required to connect to CMS from the sample code provided in Apache documentation are

- Have cloud_messaging_client.jar in the classpath - by downloading as maven artifact or installing yinst package.

  .. code-block:: xml

     <dependency>
         <groupId>yahoo.yinst.cloud_messaging_client</groupId>
         <artifactId>cloud-messaging-client</artifactId>
         <version>0.3</version>
         <scope>provided</scope>
     </dependency>


Configure for YCA authentication. Hosts configured in griduser namespace role in rolesdb can consume the Oozie JMS messages.

- Install ``yjava_yca`` (0.20.x or higher) and ``yca_client_certs`` on your client.
- Add -Djava.library.path=/home/y/lib or /home/y/lib64 (if 64-bit jdk) as argument while launching your java program and have /home/y/lib/yjava_yca.jar in the classpath.
- Set the java.naming.security.principal JNDI property to the yca role name - yahoo.griduser.ALL.

  .. code-block:: java 

     Properties jndiProperties = jmsInfo.getJNDIProperties();

     jndiProperties.put("java.naming.security.principal", "yahoo.griduser.ALL");


Writing a Listener to Consume Messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Twiki - Consuming Oozie JMS Notifications gives a guideline into writing Java code 
to listen on the JMS message broken (CMS in this case) and consume messages about 
your Oozie jobs.

Here's a working code snippet to connect via Oozie client using Kerberos authentication, 
and use a JMS message listener.

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


Troubleshooting: Connection timed out to message broker e.g. prod1-broker10.messaging.bf1.yahoo.com:61616 
Make sure you have the necessary ACL open as mentioned above, or your box might be 
occluded behind a NAT and you should use a gateway-like machine or launcher box.


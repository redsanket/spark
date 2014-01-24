package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.*;
	
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.JobState;
import hadooptest.workflow.hadoop.job.SleepJob;

import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.yarn.api.records.QueueInfo;
//import org.apache.hadoop.yarn.client.YarnClientImpl; // H0.23
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.SerialTests;

@Category(SerialTests.class)
public class TestQueueAcls extends TestSession {
	
    protected FullyDistributedCluster cluster =
            (FullyDistributedCluster) TestSession.cluster;
    protected int waitTime = 2;
    
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		
        // Backup the default configuration directory on the Resource Manager
		FullyDistributedCluster cluster =
	            (FullyDistributedCluster) TestSession.cluster;
        cluster.getConf(HadoopCluster.RESOURCE_MANAGER).backupConfDir();
	}

    public void resetRM(String rmConfig)
            throws Exception {
        
        // Copy files to the custom configuration directory on the
        // Resource Manager component host.
        cluster.getConf(HadoopCluster.RESOURCE_MANAGER).copyFileToConfDir(
                rmConfig,
                "capacity-scheduler.xml");
        cluster.hadoopDaemon(Action.STOP, HadoopCluster.RESOURCE_MANAGER);
        cluster.hadoopDaemon(Action.START, HadoopCluster.RESOURCE_MANAGER);

        TestSession.logger.info("Get Yarn Client:");
        YarnClientImpl yarnClient = TestSession.cluster.getYarnClient();
        TestSession.logger.info("Get All Queues:");
        List<QueueInfo> queues =  yarnClient.getAllQueues(); 
        assertNotNull("Expected cluster queue(s) not found!!!", queues);        
        TestSession.logger.info("queues='" +
            Arrays.toString(queues.toArray()) + "'");        
    }

    public void checkQueueAcls(String user, Hashtable<String, String> expAcls)
	        throws Exception {	    
        String[] jobCmd = {
                TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"),
                "--config", TestSession.cluster.getConf().getHadoopConfDir(),
                "queue", 
                "-showacls" };
        String[] jobOutput = TestSession.exec.runProcBuilderSecurity(jobCmd, user);
        if (!jobOutput[0].equals("0")) {
            TestSession.logger.info("Got unexpected non-zero exit code: " + jobOutput[0]);
            TestSession.logger.info("stdout" + jobOutput[1]);
            TestSession.logger.info("stderr" + jobOutput[2]);                       
        }

        TestSession.logger.info("Check for expected queue acls for " + 
                "user '" + user + "':");
        String regex = "Queue acls for user :\\s+" + user;
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(jobOutput[1]);
        assertTrue("Expected output for queue acls for user '" + user + 
                "' is not found!!!", 
                matcher.find());
        
        Enumeration<String> userKey = expAcls.keys();
        while(userKey.hasMoreElements()) {
            String key = userKey.nextElement();
            String value = expAcls.get(key);
            TestSession.logger.info("Check for expected queue acls for user '" +
                    user + "' queue '" + key + "'");
            regex = key+"\\s+"+value;
            pattern = Pattern.compile(regex);
            matcher = pattern.matcher(jobOutput[1]);
            assertTrue("Expected queue acls for user '" + user + "' queue '" +
                    key + "' does not match: actual queue acls='" + value + "'",
                    matcher.find());
        }
	}
	

    /*
     * A test for queue ACL
     * Use custom capacity scheduler file #1
     */
    @Test
    public void testQueueAcls1() throws Exception {
        resetRM(TestSession.conf.getProperty("WORKSPACE") + 
                "/conf/QueueAcls/capacity-scheduler_queue_acls_1.xml");
        Hashtable<String, String> expAcls = new Hashtable<String, String>();
        expAcls.put("root", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("a", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("a1", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("a2", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("b", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("c", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("c1", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls.put("c2", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        checkQueueAcls("hadoopqa", expAcls);
        checkQueueAcls("hadoop1", expAcls);
        checkQueueAcls("hadoop5", expAcls);        
    }

    /*
     * A test for queue ACL
     * Use custom capacity scheduler file #2
     */
    @Test
    public void testQueueAcls2() throws Exception {
        resetRM(TestSession.conf.getProperty("WORKSPACE") + 
                "/conf/QueueAcls/capacity-scheduler_queue_acls_2.xml");
        Hashtable<String, String> expAcls2 = new Hashtable<String, String>();
        expAcls2.put("root", "ADMINISTER_QUEUE");
        expAcls2.put("a", "");
        expAcls2.put("a1", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls2.put("a2", "ADMINISTER_QUEUE");
        expAcls2.put("b", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        expAcls2.put("c", "");
        expAcls2.put("c1", "ADMINISTER_QUEUE");
        expAcls2.put("c2", "ADMINISTER_QUEUE");
        checkQueueAcls("hadoopqa", expAcls2);

        int waitTime = 2;

        TestSession.logger.info("Test queue acls inheritance (bug 4620788)");
        SleepJob sleepJob2 = new SleepJob();
        sleepJob2.setQueue("a2");
        sleepJob2.setUser("hadoopqa");
        sleepJob2.start();
        assertTrue("Sleep job was not assigned an ID within 10 seconds.", 
                sleepJob2.waitForID(10));
        assertTrue("Sleep job ID for sleep job is invalid.", 
                sleepJob2.verifyID());
        assertTrue("Job should fail but succeeded.",
                sleepJob2.waitFor(JobState.FAILED, waitTime));
        /* TODO: Verify expected error message:
         String errorMessage=
        "org.apache.hadoop.security.AccessControlException: " +
        "User $user cannot submit applications to queue root.a.a2";
        */

        /* TODO: for some reason having this job 'sleepJob' before the previous
         * 'sleepJob2 will cause sleepJob2 to throw exception and exit the test.
         * Should investigate. 
         */
        TestSession.logger.info("Test queue acls inheritance (bug 4620788)");
        SleepJob sleepJob = new SleepJob();
        sleepJob.setUser("hadoopqa");
        sleepJob.setQueue("a1");
        sleepJob.setNumMappers(1);
        sleepJob.setNumReducers(1);
        sleepJob.setMapDuration(100);
        sleepJob.setReduceDuration(100);
        sleepJob.start();
        assertTrue("Sleep job was not assigned an ID within 10 seconds.", 
                sleepJob.waitForID(10));
        assertTrue("Sleep job ID for sleep job is invalid.", 
                sleepJob.verifyID());
        assertTrue("Job did not succeed.",
                sleepJob.waitForSuccess(waitTime));        
    }

    /*
     * A test for queue ACL
     * Use custom capacity scheduler file #3
     */
    @Test
    public void testQueueAcls3() throws Exception {
        resetRM(TestSession.conf.getProperty("WORKSPACE") + 
                "/conf/QueueAcls/capacity-scheduler_queue_acls_3.xml");
        Hashtable<String, String> expAcls3 = new Hashtable<String, String>();
        expAcls3.put("root", "");
        expAcls3.put("a", "");
        expAcls3.put("a1", "");
        expAcls3.put("a2", "SUBMIT_APPLICATIONS");
        expAcls3.put("b", "ADMINISTER_QUEUE");
        expAcls3.put("c", "ADMINISTER_QUEUE");
        expAcls3.put("c1", "ADMINISTER_QUEUE");
        expAcls3.put("c2", "ADMINISTER_QUEUE,SUBMIT_APPLICATIONS");
        checkQueueAcls("hadoopqa", expAcls3);

        Hashtable<String, String> expAcls4 = new Hashtable<String, String>();
        expAcls4.put("root", "");
        expAcls4.put("a", "");
        expAcls4.put("a1", "ADMINISTER_QUEUE");
        expAcls4.put("a2", "");
        expAcls4.put("b", "ADMINISTER_QUEUE");
        expAcls4.put("c", "SUBMIT_APPLICATIONS");
        expAcls4.put("c1", "SUBMIT_APPLICATIONS");
        expAcls4.put("c2", "SUBMIT_APPLICATIONS");
        checkQueueAcls("hadoop2", expAcls4);
        
        TestSession.logger.info("Test queue acls inheritance (bug 4620788)");
        SleepJob sleepJob3 = new SleepJob();
        sleepJob3.setQueue("b");
        sleepJob3.setUser("hadoopqa");
        sleepJob3.start();
        assertTrue("Sleep job was not assigned an ID within 10 seconds.", 
                sleepJob3.waitForID(10));
        assertTrue("Sleep job ID for sleep job is invalid.", 
                sleepJob3.verifyID());
        assertTrue("Job should fail but succeeded.",
                sleepJob3.waitFor(JobState.FAILED, waitTime));
        /*
         * TODO: Verify expected error message:
        String errorMmessage =
                "org.apache.hadoop.security.AccessControlException: " +
                "User $user cannot submit applications to queue root.b";
        */
    }

}
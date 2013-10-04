package hadooptest.hadoop.regression;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopComponent;
import hadooptest.cluster.hadoop.HadoopCluster.Action;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Random;

@Category(SerialTests.class)
public class TestRollingUpgradeDaemon extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
    @Test
    public void testResetDaemons() throws Exception {
        // Initialize max duration, default is "5" minutes.
        int maxDurationMin =
                Integer.parseInt(System.getProperty("DURATION", "5"));
        TestSession.logger.info("Run for '" + maxDurationMin + "' minutes.");
        long maxDurationMs = maxDurationMin*60*1000; 
        long startTime = System.currentTimeMillis();
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        int elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
        String filePath = "/tmp/foo";
        File file = new File(filePath);
        int index = 0;
        while ((!file.exists()) && (elapsedTime < maxDurationMs))  {
            // Reset each components
            TestSession.logger.info("---> Reset Daemons #" + ++index + ":");

            int maxSec = 180;
            Random random = new Random();
            Hashtable<String, HadoopComponent> hadoopComponents = 
                    TestSession.cluster.getComponents();
            Enumeration<String> components = hadoopComponents.keys();
            while(components.hasMoreElements()) {
                resetDaemon(
                        (String) components.nextElement(),
                        random.nextInt(maxSec+1));
            }
            
            currentTime = System.currentTimeMillis();
            elapsedTime = currentTime - startTime;
            elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
            TestSession.logger.info("Elapsed time is: '" + elapsedTimeMin + 
                    "' minutes.");
            
            if (file.exists()){
                TestSession.logger.info("Found stop file.");
            }
            // file.createNewFile();
        }
        if (file.exists()) {
            file.delete();            
        }     
    }
                
    public void resetDaemon(String component, int waitSec) {
        if (component.equals(HadoopCluster.GATEWAY)) { return; }            

        TestSession.logger.info("--> Reset component '" + component + "'");             
        // hadoopComponents.get(component).printNodes();

        // TODO: Skip for now
        if (component.equals(HadoopCluster.RESOURCE_MANAGER)) {
            return; 
        }
        
        try {    
            TestSession.logger.info("--> Stop component '" + component + "'");
            cluster.hadoopDaemon(Action.STOP, component);
            /*
            if (component.equals(HadoopCluster.DATANODE)) {
                cluster.hadoopDaemon(Action.STOP, HadoopCluster.RESOURCE_MANAGER);
            }
            */
                
            // TODO: Need a mechanism to swap component version for upgrade
                
            // Sleep a random number of seconds in the range of 0 to maxSec
            TestSession.logger.info("Sleep for a random '" + waitSec +
                    "' seconds.");
            Thread.sleep(waitSec*1000);
                
            TestSession.logger.info("--> Start component '" + component + "'");
            cluster.hadoopDaemon(Action.START, component);
            /*
            if (component.equals(HadoopCluster.DATANODE)) {
                cluster.hadoopDaemon(Action.START, HadoopCluster.RESOURCE_MANAGER);
            }
            */

            if (component.equals(HadoopCluster.NAMENODE)) {
                assertTrue("Cluster is not off of safemode after cluster reset", TestSession.cluster.waitForSafemodeOff());                 
            }               
            assertTrue("Cluster is not fully up after cluster reset", TestSession.cluster.isFullyUp());
        }                        
        catch (Exception e) {
            TestSession.logger.error("Exception failure.", e);
            fail();
        }   
    }   
    
}
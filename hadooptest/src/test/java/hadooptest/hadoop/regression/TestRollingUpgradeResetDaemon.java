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
public class TestRollingUpgradeResetDaemon extends TestSession {

	@BeforeClass
	public static void startTestSession() throws IOException {
		TestSession.start();
	}
	
    @Test
    public void testResetDaemons() throws Exception {
        long maxDurationMin = 30;
        long maxDurationMs = maxDurationMin*60*1000; 
        long startTime = System.currentTimeMillis();
        long currentTime = System.currentTimeMillis();
        long elapsedTime = currentTime - startTime;
        int elapsedTimeMin = (int) (elapsedTime / 1000) / 60;
        String filePath = "/tmp/foo";
        File file = new File(filePath);
        while ((!file.exists()) && (elapsedTime < maxDurationMs))  {
            // Do Something...
            resetDaemons();
            
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
                
	public void resetDaemons() {
	    TestSession.logger.info("---> ResetDaemons: ");
	    Random r = new Random();
	    int maxSec = 180;
	    int waitSec;
	    
	    // Get the components
        Hashtable<String, HadoopComponent> hadoopComponents = 
                TestSession.cluster.getComponents();
        Enumeration<String> components = hadoopComponents.keys();
        while(components.hasMoreElements()) {
            String component = (String) components.nextElement();	            
            if (component.equals(HadoopCluster.GATEWAY)) { continue; }
            
            if (component.equals(HadoopCluster.RESOURCE_MANAGER)) {
                continue; 
            }
            
            // hadoopComponents.get(component).printNodes();
            TestSession.logger.info("--> Reset component '" + component + "'");
	            
            try {    
                TestSession.logger.info("--> Stop component '" + component + "'");
                cluster.hadoopDaemon(Action.STOP, component);

                /*
                if (component.equals(HadoopCluster.DATANODE)) {
                    cluster.hadoopDaemon(Action.STOP, HadoopCluster.RESOURCE_MANAGER);
                }
                */
                
                // TODO: swap component version
                
	            // Sleep a random number of seconds in the range of 0 to maxSec
	            waitSec = r.nextInt(maxSec+1);
	            TestSession.logger.info("Sleep for '" + waitSec + "' seconds.");
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
	
}
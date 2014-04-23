package hadooptest.dfs.regression;

import hadooptest.dfs.utils.HAfailoverUtility;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.ResourceManagerHttpUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.*;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestHAfailoverBasic extends TestSession {

	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupTestConf();
		checkIfHAenabled();       // make sure cluster is configured for failover
		checkNamenodeIfconfig();  // check the up/down state of each NN's alias net if
		checkNamenodeHAserviceState(); // check ha1 and ha2 active/standby states
		
		// we need to run as the hdfs priv user, hdfsqa
		TestSession.cluster.setSecurityAPI("keytab-hdfsqa", "user-hdfsqa");
	}

    protected static void setupTestConf() throws Exception {
        //FullyDistributedCluster cluster =
                //(FullyDistributedCluster) TestSession.cluster;
        
        String nnHost = TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
        TestSession.logger.debug("nnHost is: "+ nnHost);

    }
    
	protected static void checkIfHAenabled() {

		//String clusterName = TestSession.conf.getProperty("CLUSTER_NAME");
		//TestSession.logger.debug("CLUSTER_NAME is: "+clusterName);
		
        // make sure the cluster has ip-failover HA enabled
        if (getIfHAEnabled()) 
        	TestSession.logger.info("IP-Failover HA is enabled");
        else
        {
        	assertTrue("IP-failover is not enabled", getIfHAEnabled());
        	fail();
        }
	}
	
    protected static void checkNamenodeIfconfig () throws Exception{
    	
    	HAfailoverUtility hafoutil = new HAfailoverUtility();
    	
    	String cluster = TestSession.conf.getProperty("CLUSTER_NAME");
    	String nnHost1 = TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
    	String nnHost2 = "gsbl90106.blue.ygrid.yahoo.com";

    	TestSession.logger.debug("Check ifconfig for ha1 namenode "+nnHost1+" reports: " + 
    			hafoutil.getNamenodeIfconfigStatus(cluster, nnHost1));
    	
    	TestSession.logger.debug("Check ifconfig for ha2 namenode "+nnHost2+" reports: " + 
    			hafoutil.getNamenodeIfconfigStatus(cluster, nnHost2));
    }
	
	protected static void checkNamenodeHAserviceState() throws Exception {
		String service_ha1 = "ha1";
		String service_ha2 = "ha2";
		String cluster = TestSession.conf.getProperty("CLUSTER_NAME");
		String nnHost = TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
		HAfailoverUtility hafoutil = new HAfailoverUtility();

		TestSession.logger.debug("GetNamenodeIfStatus2: cluster "+cluster+" host "+nnHost+
				" for service "+service_ha1+" reports it is: "+ 
				hafoutil.getNamenodeHAserviceState(cluster, nnHost, service_ha1));
		
		TestSession.logger.debug("GetNamenodeIfStatus2: cluster "+cluster+" host "+nnHost+
				" for service "+service_ha2+" reports it is: "+ 
				hafoutil.getNamenodeHAserviceState(cluster, nnHost, service_ha2));
	}
	
	// utility to check if the cluster is configured to support failover
	protected static boolean getIfHAEnabled() {
		// get the value of the dfs.nameservices in hdfs-ha.xml, if it's set to
		// flubber-alias* then cluster has been configured for HA failover in QE,
		// if not then the cluster is unlikely to support failover even if the
		// deployed with HA enabled
		String getValue = TestSession.cluster.getConf().get("dfs.nameservices");
		
		Pattern p = Pattern.compile("flubber-alias");
		Matcher m = p.matcher(getValue);
		
		if (m.find()) {
			return true;
		}
		else {
			return false;
		}
	}

	// utility to try failover using HAfailoverUtility
	protected String tryFailover(String cluster, String newservice) throws Exception
	{
		HAfailoverUtility hafoutil = new HAfailoverUtility();

		TestSession.logger.debug("Trying to failover cluster "+cluster+" to service "+newservice);	

		// do it
		String result = hafoutil.failover(cluster, newservice);
		if (result.equals(newservice)) {
			return newservice;
		} else {
			TestSession.logger.error("TryFailover failed, returned: "+result);
			return "fail";
		}
		
	}

	
    @Test
	public void TestHAfailoverInitialFO() {

		String clusterName = TestSession.conf.getProperty("CLUSTER_NAME");		
		String newservice = "ha2";
		
		TestSession.logger.debug("Going to try to failover cluster "+clusterName+
				" to new service "+newservice);

		try {
			String result = tryFailover(clusterName, newservice);
			if (result.equals(newservice)) {
				TestSession.logger.info("Failover reports success!");
			}
			else
			{
				TestSession.logger.error("Failed! failover attempt reports: "+result);
				fail();
			}
		} catch (Exception e) {
			TestSession.logger.error("Failover attempted but failed with exception!");
			fail();
		}
		
	}
		

}
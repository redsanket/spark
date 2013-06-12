package hadooptest.hadoop.regression;

import static org.junit.Assert.*;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFS extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	public String getHdfsBaseUrl() throws Exception {
		return "hdfs://" + 
		        TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
	}
	
	public void fsls(String path) throws Exception {
		fsls(path, null);
	}

	public void fsls(String path, String[] args) throws Exception {
		TestSession.logger.debug("Show HDFS path: '" + path + "':");
		FsShell fsShell = TestSession.cluster.getFsShell();
		String URL = getHdfsBaseUrl() + path;
 
		String[] cmd;
		if (args == null) {
			cmd = new String[] {"-ls", URL};
		} else {
			ArrayList<String> list = new ArrayList<String>();
			list.add("-ls");
			list.addAll(Arrays.asList(args));
			list.add(URL);
			cmd = (String[]) list.toArray(new String[0]);
		}
		TestSession.logger.info(TestSession.cluster.getConf().getHadoopProp("HDFS_BIN") +
					" dfs " + StringUtils.join(cmd, " "));
 		fsShell.run(cmd);
	}

	/*
	* A test for running fs ls
	*/
	@Test
	public void runTestFsLs() throws Exception {
		TestSession.logger.info("Run FS Test");

		FileSystem fs = TestSession.cluster.getFS();
		String testDir = getHdfsBaseUrl() + "/";
		if (fs.exists(new Path(testDir))) {
			TestSession.logger.info("Found test directory: " + testDir);
		}
		assertTrue("Root directory '/' does not exist. ",
		        fs.exists(new Path(testDir)));
		fsls("/", new String[] {"-d"});
	}

	
	/*
	 * A test for running fs rm and mkdir
	 */
	@Test
	public void runTestFsDir() throws Exception {
	    // Define the test directory
	    DFS dfs = new DFS();
	    String testDir_ = "/user/" +
	            System.getProperty("user.name") + "/sort";
	    String testDir = dfs.getBaseUrl() + testDir_;
	    
	    // Delete it existing test directory if exists
	    FileSystem fs = TestSession.cluster.getFS();
	    FsShell fsShell = TestSession.cluster.getFsShell();                
	    if (fs.exists(new Path(testDir))) {
	        TestSession.logger.info("Delete existing test directory: " +
	            testDir);
	        fsShell.run(new String[] {"-rm", "-r", testDir});           
	    }
	    
	    // Create or re-create the test directory.
	    TestSession.logger.info("Test create new test directory: " + testDir);
	    fsShell.run(new String[] {"-mkdir", "-p", testDir});
        fsls(testDir_, new String[] {"-d"});
	    assertTrue("Test directory '" + testDir + "' does not exist. ",
	            fs.exists(new Path(testDir)));

        TestSession.logger.info("Test delete existing test directory: " +
                testDir);
        fsShell.run(new String[] {"-rm", "-r", testDir});           
        assertFalse("Test directory '" + testDir +
                "' does exist but should not. ",
                fs.exists(new Path(testDir)));

	}	    
}

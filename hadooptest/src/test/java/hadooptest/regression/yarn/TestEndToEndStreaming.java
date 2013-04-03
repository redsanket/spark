package hadooptest.regression.yarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.config.TestConfiguration;
import hadooptest.job.StreamingJob;
import hadooptest.job.JobState;

public class TestEndToEndStreaming extends TestSession {
	
	private String userName = System.getProperty("user.name");
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		
		setupTestConf();
	}
	
	@Test public void testCacheArchives10() throws Exception { testCacheArchivesCore(10, "/tmp/streaming", ".jar"); }
	@Test public void testCacheArchives20() throws Exception { testCacheArchivesCore(20, "/tmp/streaming", ".tar"); }
	@Test public void testCacheArchives30() throws Exception { testCacheArchivesCore(30, "/tmp/streaming", ".tar.gz"); }
	@Test public void testCacheArchives40() throws Exception { testCacheArchivesCore(40, "/tmp/streaming", ".tgz"); }
	@Test public void testCacheArchives50() throws Exception { testCacheArchivesCore(50, "/tmp/streaming", ".zip"); }
	@Test public void testCacheArchives60() throws Exception { testCacheArchivesCore(60, "/user/" + userName + "/streaming", ".jar"); }
	@Test public void testCacheArchives70() throws Exception { testCacheArchivesCore(70, "/user/" + userName + "/streaming", ".tar"); }
	@Test public void testCacheArchives80() throws Exception { testCacheArchivesCore(80, "/user/" + userName + "/streaming", ".tar.gz"); }
	@Test public void testCacheArchives90() throws Exception { testCacheArchivesCore(90, "/user/" + userName + "/streaming", ".tgz"); }
	@Test public void testCacheArchives100() throws Exception { testCacheArchivesCore(100, "/user/" + userName + "/streaming", ".zip"); }
	
	/*
	@Test
	public void testArchives() {
		
	}
	
	@Test
	public void testCacheFiles() {
		
	}
	
	@Test
	public void testFiles() {
		
	}
	*/
	
	private void testCacheArchivesCore(int testcaseID, String publicPrivateCache, String archive) {
		try {
			this.setupHdfsDir("/tmp/streaming/" + testcaseID);
			this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
			this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
			this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + testcaseID);

			String cacheInCommand = publicPrivateCache + "/" + testcaseID;

			this.logger.info("Streaming-" + testcaseID + " - Test to check the -cacheArchive option for " + archive + " file on " + publicPrivateCache);

			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + testcaseID + "/cachedir" + archive), 
					cacheInCommand + "/cachedir" + archive);
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + testcaseID + "/input.txt"), 
					"/tmp/streaming/streaming-" + testcaseID + "/input.txt");

			StreamingJob job = new StreamingJob();
			job.setNumMappers(1);
			job.setNumReducers(1);
			job.setName("streamingTest-" + testcaseID);
			job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
			job.setInputFile(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + testcaseID + "/input.txt");
			job.setMapper("\"xargs cat\"");
			job.setReducer("cat");
			job.setOutputPath(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + testcaseID + "/Output");
			job.setCacheArchivePath(this.getHdfsBaseUrl() + cacheInCommand + "/cachedir" + archive + "#testlink");

			job.start();

			assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
					job.waitForID(30));
			assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
					job.verifyID());

			assertTrue("Streaming job did not succeed", job.waitFor(JobState.SUCCEEDED, 240));

			this.validateOutput(testcaseID);
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
	
	private void validateOutput(int testcaseID) throws Exception {
		FileSystem fs = TestSession.cluster.getFS();
		FileStatus[] elements = fs.listStatus(new Path(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + testcaseID + "/Output"));
		
		String partFilePathStr = null;
		for (FileStatus element : elements) {
			TestSession.logger.info("Checking part file: " + element.getPath());

			if (element.getPath().toString().contains("part-")) {
				partFilePathStr = element.getPath().toString();	
				break;
			}
		}
		
		if (partFilePathStr == null) {
			TestSession.logger.error("Did not find the part file for the test.");
			fail();
		}

		String[] catCmd = {
				TestSession.cluster.getConf().getHadoopProp("HDFS_BIN"),
				"--config", TestSession.cluster.getConf().getHadoopProp("HADOOP_CONF_DIR"),
				"dfs", "-cat", partFilePathStr	
		};
		
		String[] catOutput = TestSession.exec.runHadoopProcBuilder(catCmd);
		if (!catOutput[0].equals("0")) {
			TestSession.logger.info("Got unexpected non-zero exit code: " + catOutput[0]);
			TestSession.logger.info("stdout" + catOutput[1]);
			TestSession.logger.info("stderr" + catOutput[2]);			
		}
		
		String expectedOutputStr = "";
		try {
			expectedOutputStr = FileUtils.readFileToString(
					new File(getResourceFullPath("" +
							"data/streaming/streaming-" + testcaseID + "/expectedOutput")));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		String actualOutputStr = catOutput[1];
		TestSession.logger.debug("expected output str = \n'" + expectedOutputStr + "'");
		TestSession.logger.debug("actual output str = \n'" + actualOutputStr + "'");
		assertEquals("Actual output is different than expected ouput.", expectedOutputStr, actualOutputStr);
	}
	
	/*
	 *  Check for the destination directory and create it if
     * is not present because 'dfs put' used to do that 
     */
	private void putLocalToHdfs(String source, String target) {
		try {
			TestSession.logger.debug("target=" + target);
			String targetDir = target.substring(0, target.lastIndexOf("/"));	
			TestSession.logger.debug("target path=" + targetDir);

			FsShell fsShell = TestSession.cluster.getFsShell();
			FileSystem fs = TestSession.cluster.getFS();

			String URL = "hdfs://" + TestSession.cluster.getNodes("namenode")[0] + "/";
			String homeDir = URL + "user/" + System.getProperty("user.name");
			String testDir = homeDir + "/" + targetDir;
			String testTarget = URL + "/" + target;
			if (!fs.exists(new Path(testDir))) {
				fsShell.run(new String[] {"-mkdir", "-p", testDir});
			}
			TestSession.logger.debug("dfs -put " + source + " " + testTarget);
			fsShell.run(new String[] {"-put", source, testTarget});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setupHdfsDir(String path) {
		try{
			FileSystem fs = TestSession.cluster.getFS();
			FsShell fsShell = TestSession.cluster.getFsShell();		
			String testDir = getHdfsBaseUrl() + path;
			if (fs.exists(new Path(testDir))) {
				TestSession.logger.info("Delete existing test directory: " + testDir);
				fsShell.run(new String[] {"-rm", "-r", testDir});			
			}
			TestSession.logger.info("Create new test directory: " + testDir);
			fsShell.run(new String[] {"-mkdir", "-p", testDir});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String getHdfsBaseUrl() {
		try {
			return "hdfs://" + TestSession.cluster.getNodes("namenode")[0];
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private String getResourceFullPath(String relativePath) {
		String fullPath = "";
		
		try {
			URL url = this.getClass().getClassLoader().getResource(relativePath);
			fullPath = url.getPath();
			TestSession.logger.debug("Resource URL path=" + fullPath);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
        return fullPath;
	}
	
	private static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster = (FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;

		/* 
		 * NOTE: Add a check via the Hadoop API or jmx to determine if a single
		 * queue is already in place. If so, skip the following as to not waste
		 *  time.
		 */
		
		// Backup the default configuration directory on the Resource Manager
		// component host.
		cluster.getConf().backupConfDir(component);	

		// Copy files to the custom configuration directory on the
		// Resource Manager component host.
		String sourceFile = TestSession.conf.getProperty("WORKSPACE") +
				"/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
		cluster.getConf().copyFileToConfDir(component, sourceFile,
				"capacity-scheduler.xml");
		cluster.hadoopDaemon("stop", component);
		cluster.hadoopDaemon("start", component);
	}
}

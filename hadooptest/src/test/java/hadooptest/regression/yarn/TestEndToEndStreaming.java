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

	@Test public void testCacheArchives10() throws Exception { fileOnCache(10, "/tmp/streaming", ".jar"); }
	@Test public void testCacheArchives20() throws Exception { fileOnCache(20, "/tmp/streaming", ".tar"); }
	@Test public void testCacheArchives30() throws Exception { fileOnCache(30, "/tmp/streaming", ".tar.gz"); }
	@Test public void testCacheArchives40() throws Exception { fileOnCache(40, "/tmp/streaming", ".tgz"); }
	@Test public void testCacheArchives50() throws Exception { fileOnCache(50, "/tmp/streaming", ".zip"); }
	@Test public void testCacheArchives60() throws Exception { fileOnCache(60, "/user/" + userName + "/streaming", ".jar"); }
	@Test public void testCacheArchives70() throws Exception { fileOnCache(70, "/user/" + userName + "/streaming", ".tar"); }
	@Test public void testCacheArchives80() throws Exception { fileOnCache(80, "/user/" + userName + "/streaming", ".tar.gz"); }
	@Test public void testCacheArchives90() throws Exception { fileOnCache(90, "/user/" + userName + "/streaming", ".tgz"); }
	@Test public void testCacheArchives100() throws Exception { fileOnCache(100, "/user/" + userName + "/streaming", ".zip"); }
	
	@Test public void testCacheArchives110() throws Exception { symlinkOnBadCache(110, "/tmp/streaming"); }
	@Test public void testCacheArchives120() throws Exception { symlinkOnBadCache(120, "/user/" + userName + "/streaming"); }

	@Test public void testCacheArchives130() throws Exception { noSymlinkOnCache(130, "/tmp/streaming"); }
	@Test public void testCacheArchives140() throws Exception { noSymlinkOnCache(140, "/user/" + userName + "/streaming"); }
	
	//@Test public void testArchives160() throws Exception { archivesFileOnFS(160, "/tmp/streaming", ".jar", "file://"); }
	@Test public void testArchives170() throws Exception { archivesFileOnFS(170, "/tmp/streaming", ".jar", this.getHdfsBaseUrl()); }
	//@Test public void testArchives180() throws Exception { archivesFileOnFS(180, "/tmp/streaming", ".tar", "file://"); }
	@Test public void testArchives190() throws Exception { archivesFileOnFS(190, "/tmp/streaming", ".tar", this.getHdfsBaseUrl()); }
	//@Test public void testArchives200() throws Exception { archivesFileOnFS(200, "/tmp/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives210() throws Exception { archivesFileOnFS(210, "/tmp/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	//@Test public void testArchives220() throws Exception { archivesFileOnFS(220, "/tmp/streaming", ".tgz", "file://"); }
	@Test public void testArchives230() throws Exception { archivesFileOnFS(230, "/tmp/streaming", ".tgz", this.getHdfsBaseUrl()); }
	//@Test public void testArchives240() throws Exception { archivesFileOnFS(240, "/tmp/streaming", ".zip", "file://"); }
	@Test public void testArchives250() throws Exception { archivesFileOnFS(250, "/tmp/streaming", ".zip", this.getHdfsBaseUrl()); }
	//@Test public void testArchives260() throws Exception { archivesFileOnFS(260, "/user/" + userName + "/streaming", ".jar", "file://"); }
	@Test public void testArchives270() throws Exception { archivesFileOnFS(270, "/user/" + userName + "/streaming", ".jar", this.getHdfsBaseUrl()); }
	//@Test public void testArchives280() throws Exception { archivesFileOnFS(280, "/user/" + userName + "/streaming", ".tar", "file://"); }
	@Test public void testArchives290() throws Exception { archivesFileOnFS(290, "/user/" + userName + "/streaming", ".tar", this.getHdfsBaseUrl()); }
	//@Test public void testArchives300() throws Exception { archivesFileOnFS(300, "/user/" + userName + "/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives310() throws Exception { archivesFileOnFS(310, "/user/" + userName + "/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	//@Test public void testArchives320() throws Exception { archivesFileOnFS(320, "/user/" + userName + "/streaming", ".tgz", "file://"); }
	@Test public void testArchives330() throws Exception { archivesFileOnFS(330, "/user/" + userName + "/streaming", ".tgz", this.getHdfsBaseUrl()); }
	//@Test public void testArchives340() throws Exception { archivesFileOnFS(340, "/user/" + userName + "/streaming", ".zip", "file://"); }
	@Test public void testArchives350() throws Exception { archivesFileOnFS(350, "/user/" + userName + "/streaming", ".zip", this.getHdfsBaseUrl()); }
	
	private void archivesFileOnFS(int testcaseID, String publicPrivateCache,
			String archive, String fileSystem) throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			// make a local directory to store local fs
			// cacheInCommand needs to be prepended by the local fs path
			// put the input.txt and cachedir in the path
			cacheInCommand = "/data/streaming-" + testcaseID;
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;

			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/input.txt"), 
							"/tmp/streaming/streaming-" + testcaseID + 
					"/input.txt");
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/cachedir" + archive), 
							cacheInCommand + "/cachedir" + archive);
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archive option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + archive + 
				" file.");

		StreamingJob job = new StreamingJob();
		job.setNumMappers(1);
		job.setNumReducers(1);
		job.setName("streamingTest-" + testcaseID);
		job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
		job.setInputFile(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/input.txt");
		job.setMapper("\"xargs cat\"");
		job.setReducer("cat");
		job.setOutputPath(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/Output");
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/cachedir" + archive + "#testlink");

		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}

	private void noSymlinkOnCache(int testcaseID, String publicPrivateCache) 
			throws Exception {

		String archive = "cachedir.zip";

		logger.info("Streaming-" + testcaseID + " - Test to check the " + 
				"-cacheArchive when no symlink is specified on " + 
				publicPrivateCache);

		this.putLocalToHdfs(
				this.getResourceFullPath("data/streaming/streaming-" +
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = new StreamingJob();
		job.setNumMappers(1);
		job.setNumReducers(1);
		job.setName("streamingTest-" + testcaseID);
		job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
		job.setInputFile(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/input.txt");
		job.setMapper("\"xargs cat\"");
		job.setReducer("cat");
		job.setOutputPath(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/Output");
		job.setCacheArchivePath(this.getHdfsBaseUrl() + publicPrivateCache + 
				"/" + archive);

		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("You need to specify the uris as " + 
					"scheme://path#linkname,Please specify a different " + 
					"link name for all of your caching URIs")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void symlinkOnBadCache(int testcaseID, String publicPrivateCache) 
			throws Exception {

		String archive = "nonExistentcachedir.zip";

		logger.info("Streaming-" + testcaseID + " - Test to check the " + 
				"-cacheArchive option for non existent symlink on " + 
				publicPrivateCache);

		this.putLocalToHdfs(
				this.getResourceFullPath("data/streaming/streaming-" +
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = new StreamingJob();
		job.setNumMappers(1);
		job.setNumReducers(1);
		job.setName("streamingTest-" + testcaseID);
		job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
		job.setInputFile(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/input.txt");
		job.setMapper("\"xargs cat\"");
		job.setReducer("cat");
		job.setOutputPath(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/Output");
		job.setCacheArchivePath(this.getHdfsBaseUrl() + publicPrivateCache + 
				"/" + archive + "#testlink");

		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("Error launching job , bad input path : " +
					"File does not exist:")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void fileOnCache(int testcaseID, 
			String publicPrivateCache, 
			String archive) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);

		String cacheInCommand = publicPrivateCache + "/" + testcaseID;

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheArchive option for " + archive + 
				" file on " + publicPrivateCache);

		this.putLocalToHdfs(
				this.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/cachedir" + archive), 
						cacheInCommand + "/cachedir" + archive);
		this.putLocalToHdfs(
				this.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = new StreamingJob();
		job.setNumMappers(1);
		job.setNumReducers(1);
		job.setName("streamingTest-" + testcaseID);
		job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
		job.setInputFile(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/input.txt");
		job.setMapper("\"xargs cat\"");
		job.setReducer("cat");
		job.setOutputPath(this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + 
				testcaseID + "/Output");
		job.setCacheArchivePath(this.getHdfsBaseUrl() + cacheInCommand + 
				"/cachedir" + archive + "#testlink");

		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void validateOutput(int testcaseID) throws Exception {
		FileSystem fs = TestSession.cluster.getFS();
		FileStatus[] elements = fs.listStatus(new Path(this.getHdfsBaseUrl() + 
				"/tmp/streaming/streaming-" + testcaseID + "/Output"));
		
		String partFilePathStr = null;
		for (FileStatus element : elements) {
			logger.info("Checking part file: " + element.getPath());

			if (element.getPath().toString().contains("part-")) {
				partFilePathStr = element.getPath().toString();	
				break;
			}
		}
		
		if (partFilePathStr == null) {
			logger.error("Did not find the part file for the " + 
					"test.");
			fail();
		}

		String[] catCmd = {
				cluster.getConf().getHadoopProp("HDFS_BIN"),
				"--config", 
				cluster.getConf().getHadoopProp("HADOOP_CONF_DIR"),
				"dfs", "-cat", partFilePathStr	
		};
		
		String[] catOutput = TestSession.exec.runHadoopProcBuilder(catCmd);
		if (!catOutput[0].equals("0")) {
			logger.info("Got unexpected non-zero exit code: " + 
					catOutput[0]);
			logger.info("stdout" + catOutput[1]);
			logger.info("stderr" + catOutput[2]);			
		}
		
		String expectedOutputStr = "";
		try {
			expectedOutputStr = FileUtils.readFileToString(
					new File(getResourceFullPath("" +
							"data/streaming/streaming-" + testcaseID + 
							"/expectedOutput")));
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
		
		String actualOutputStr = catOutput[1];
		logger.debug("expected output str = \n'" + 
				expectedOutputStr + "'");
		logger.debug("actual output str = \n'" + 
				actualOutputStr + "'");
		assertEquals("Actual output is different than expected ouput.", 
				expectedOutputStr, actualOutputStr);
	}
	
	/*
	 *  Check for the destination directory and create it if
     * is not present because 'dfs put' used to do that 
     */
	private void putLocalToHdfs(String source, String target) {
		try {
			logger.debug("target=" + target);
			String targetDir = target.substring(0, target.lastIndexOf("/"));	
			logger.debug("target path=" + targetDir);

			FsShell fsShell = cluster.getFsShell();
			FileSystem fs = cluster.getFS();

			String URL = "hdfs://" + 
					cluster.getNodes("namenode")[0] + "/";
			String homeDir = URL + "user/" + System.getProperty("user.name");
			String testDir = homeDir + "/" + targetDir;
			String testTarget = URL + "/" + target;
			if (!fs.exists(new Path(testDir))) {
				fsShell.run(new String[] {"-mkdir", "-p", testDir});
			}
			logger.debug("dfs -put " + source + " " + testTarget);
			fsShell.run(new String[] {"-put", source, testTarget});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void setupHdfsDir(String path) {
		try{
			FileSystem fs = cluster.getFS();
			FsShell fsShell = cluster.getFsShell();		
			String testDir = getHdfsBaseUrl() + path;
			if (fs.exists(new Path(testDir))) {
				logger.info("Delete existing test directory: " + 
						testDir);
				fsShell.run(new String[] {"-rm", "-r", testDir});			
			}
			logger.info("Create new test directory: " + testDir);
			fsShell.run(new String[] {"-mkdir", "-p", testDir});
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String getHdfsBaseUrl() {
		try {
			return "hdfs://" + cluster.getNodes("namenode")[0];
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private String getResourceFullPath(String relativePath) {
		String fullPath = "";
		
		try {
			URL url = 
					this.getClass().getClassLoader().getResource(relativePath);
			fullPath = url.getPath();
			TestSession.logger.debug("Resource URL path=" + fullPath);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
        return fullPath;
	}
	
	private static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster = 
				(FullyDistributedCluster) TestSession.cluster;
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
		String sourceFile = conf.getProperty("WORKSPACE") +
				"/conf/SingleQueueConf/single-queue-capacity-scheduler.xml";
		cluster.getConf().copyFileToConfDir(component, sourceFile,
				"capacity-scheduler.xml");
		cluster.hadoopDaemon("stop", component);
		cluster.hadoopDaemon("start", component);
	}
}

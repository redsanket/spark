package hadooptest.regression.yarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

import hadooptest.TestSession;
import hadooptest.job.StreamingJob;

public class EndToEndStreaming extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}
	
	@Test
	public void testCacheArchives() {
		int testcaseID = 10;
		String testcaseName = "";
		
		String userName = System.getProperty("user.name");
		
		String[] publicPrivateCache = { "/tmp/streaming", "/user/$USER_ID/streaming" };
		String[] archives = { ".jar", ".tar", ".tar.gz", ".tgz", ".zip" };
		
		int i, j;
		for(i = 0; i < publicPrivateCache.length; i++) {
			for(j = 0; j < archives.length; j++) {
				this.setupHdfsDir("/tmp/streaming/" + testcaseID);
				this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
				this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
				this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + testcaseID);
				
				String cacheInCommand = publicPrivateCache[i] + "/" + testcaseID;

				this.logger.info("Streaming-" + testcaseID + " - Test to check the -cacheArchive option for " + archives[j] + " file on " + publicPrivateCache[i]);
				
				this.putLocalToHdfs(
						this.getResourceFullPath("data/streaming/streaming-" + testcaseID + "/cachedir" + archives[j]), 
						cacheInCommand + "/cachedir" + archives[j]);
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
				job.setCacheArchivePath(this.getHdfsBaseUrl() + cacheInCommand + "/cachedir" + archives[j] + "#testlink");
				
				job.start();
				
				assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
						job.waitForID(30));
				assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
						job.verifyID());
				
				this.validateOutput(testcaseID);

				testcaseID = testcaseID + 10;
			}
		}
	}
	
	@Test
	public void testArchives() {
		
	}
	
	@Test
	public void testCacheFiles() {
		
	}
	
	@Test
	public void testFiles() {
		
	}
	
	private void validateOutput(int testcaseID) {
		String[] catCmd = {
				TestSession.cluster.getConf().getHadoopProp("HDFS_BIN"),
				"--config", TestSession.cluster.getConf().getHadoopProp("HADOOP_CONF_DIR"),
				"dfs", "-cat", this.getHdfsBaseUrl() + "/tmp/streaming/streaming-" + testcaseID + "/Output/*"				
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
}

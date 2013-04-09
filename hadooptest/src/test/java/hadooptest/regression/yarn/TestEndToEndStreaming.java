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
	
	@Test public void testArchives160() throws Exception { archivesFileOnFS(160, "/tmp/streaming", ".jar", "file://"); }
	@Test public void testArchives170() throws Exception { archivesFileOnFS(170, "/tmp/streaming", ".jar", this.getHdfsBaseUrl()); }
	@Test public void testArchives180() throws Exception { archivesFileOnFS(180, "/tmp/streaming", ".tar", "file://"); }
	@Test public void testArchives190() throws Exception { archivesFileOnFS(190, "/tmp/streaming", ".tar", this.getHdfsBaseUrl()); }
	@Test public void testArchives200() throws Exception { archivesFileOnFS(200, "/tmp/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives210() throws Exception { archivesFileOnFS(210, "/tmp/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	@Test public void testArchives220() throws Exception { archivesFileOnFS(220, "/tmp/streaming", ".tgz", "file://"); }
	@Test public void testArchives230() throws Exception { archivesFileOnFS(230, "/tmp/streaming", ".tgz", this.getHdfsBaseUrl()); }
	@Test public void testArchives240() throws Exception { archivesFileOnFS(240, "/tmp/streaming", ".zip", "file://"); }
	@Test public void testArchives250() throws Exception { archivesFileOnFS(250, "/tmp/streaming", ".zip", this.getHdfsBaseUrl()); }
	@Test public void testArchives260() throws Exception { archivesFileOnFS(260, "/user/" + userName + "/streaming", ".jar", "file://"); }
	@Test public void testArchives270() throws Exception { archivesFileOnFS(270, "/user/" + userName + "/streaming", ".jar", this.getHdfsBaseUrl()); }
	@Test public void testArchives280() throws Exception { archivesFileOnFS(280, "/user/" + userName + "/streaming", ".tar", "file://"); }
	@Test public void testArchives290() throws Exception { archivesFileOnFS(290, "/user/" + userName + "/streaming", ".tar", this.getHdfsBaseUrl()); }
	@Test public void testArchives300() throws Exception { archivesFileOnFS(300, "/user/" + userName + "/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives310() throws Exception { archivesFileOnFS(310, "/user/" + userName + "/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	@Test public void testArchives320() throws Exception { archivesFileOnFS(320, "/user/" + userName + "/streaming", ".tgz", "file://"); }
	@Test public void testArchives330() throws Exception { archivesFileOnFS(330, "/user/" + userName + "/streaming", ".tgz", this.getHdfsBaseUrl()); }
	@Test public void testArchives340() throws Exception { archivesFileOnFS(340, "/user/" + userName + "/streaming", ".zip", "file://"); }
	@Test public void testArchives350() throws Exception { archivesFileOnFS(350, "/user/" + userName + "/streaming", ".zip", this.getHdfsBaseUrl()); }

	@Test public void testArchives360() throws Exception { archivesSymlinkOnBadCache(360, "/tmp/streaming", "file://"); }
	@Test public void testArchives370() throws Exception { archivesSymlinkOnBadCache(370, "/tmp/streaming", this.getHdfsBaseUrl()); }
	@Test public void testArchives380() throws Exception { archivesSymlinkOnBadCache(380, "/user/" + userName + "/streaming", "file://"); }
	@Test public void testArchives390() throws Exception { archivesSymlinkOnBadCache(390, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }
	
	@Test public void testArchives400() throws Exception { archivesNoSymlinkOnCache(400, "/tmp/streaming", ".jar", "file://"); }
	@Test public void testArchives410() throws Exception { archivesNoSymlinkOnCache(410, "/tmp/streaming", ".jar", this.getHdfsBaseUrl()); }
	@Test public void testArchives420() throws Exception { archivesNoSymlinkOnCache(420, "/tmp/streaming", ".tar", "file://"); }
	@Test public void testArchives430() throws Exception { archivesNoSymlinkOnCache(430, "/tmp/streaming", ".tar", this.getHdfsBaseUrl()); }
	@Test public void testArchives440() throws Exception { archivesNoSymlinkOnCache(440, "/tmp/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives450() throws Exception { archivesNoSymlinkOnCache(450, "/tmp/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	@Test public void testArchives460() throws Exception { archivesNoSymlinkOnCache(460, "/tmp/streaming", ".tgz", "file://"); }
	@Test public void testArchives470() throws Exception { archivesNoSymlinkOnCache(470, "/tmp/streaming", ".tgz", this.getHdfsBaseUrl()); }
	@Test public void testArchives480() throws Exception { archivesNoSymlinkOnCache(480, "/tmp/streaming", ".zip", "file://"); }
	@Test public void testArchives490() throws Exception { archivesNoSymlinkOnCache(490, "/tmp/streaming", ".zip", this.getHdfsBaseUrl()); }
	@Test public void testArchives500() throws Exception { archivesNoSymlinkOnCache(500, "/user/" + userName + "/streaming", ".jar", "file://"); }
	@Test public void testArchives510() throws Exception { archivesNoSymlinkOnCache(510, "/user/" + userName + "/streaming", ".jar", this.getHdfsBaseUrl()); }
	@Test public void testArchives520() throws Exception { archivesNoSymlinkOnCache(520, "/user/" + userName + "/streaming", ".tar", "file://"); }
	@Test public void testArchives530() throws Exception { archivesNoSymlinkOnCache(530, "/user/" + userName + "/streaming", ".tar", this.getHdfsBaseUrl()); }
	@Test public void testArchives540() throws Exception { archivesNoSymlinkOnCache(540, "/user/" + userName + "/streaming", ".tar.gz", "file://"); }
	@Test public void testArchives550() throws Exception { archivesNoSymlinkOnCache(550, "/user/" + userName + "/streaming", ".tar.gz", this.getHdfsBaseUrl()); }
	@Test public void testArchives560() throws Exception { archivesNoSymlinkOnCache(560, "/user/" + userName + "/streaming", ".tgz", "file://"); }
	@Test public void testArchives570() throws Exception { archivesNoSymlinkOnCache(570, "/user/" + userName + "/streaming", ".tgz", this.getHdfsBaseUrl()); }
	@Test public void testArchives580() throws Exception { archivesNoSymlinkOnCache(580, "/user/" + userName + "/streaming", ".zip", "file://"); }
	@Test public void testArchives590() throws Exception { archivesNoSymlinkOnCache(590, "/user/" + userName + "/streaming", ".zip", this.getHdfsBaseUrl()); }
	
	//
	// Tests 610, 630, 650, and 670 are currently commented out, because
	// -cacheFile is now deprecated and these cases are no longer supported by
	// Hadoop (at least as of 0.23.6).
	//
	//@Test public void testCacheFiles610() throws Exception { cacheFilesFileOnFS(610, "/tmp/streaming", "InputFile", "file://"); }
	@Test public void testCacheFiles620() throws Exception { cacheFilesFileOnFS(620, "/tmp/streaming", "InputFile", this.getHdfsBaseUrl()); }
	//@Test public void testCacheFiles630() throws Exception { cacheFilesFileOnFS(630, "/tmp/streaming", "InputDir", "file://"); }
	@Test public void testCacheFiles640() throws Exception { cacheFilesFileOnFS(640, "/tmp/streaming", "InputDir", this.getHdfsBaseUrl()); }
	//@Test public void testCacheFiles650() throws Exception { cacheFilesFileOnFS(650, "/user/" + userName + "/streaming", "InputFile", "file://"); }
	@Test public void testCacheFiles660() throws Exception { cacheFilesFileOnFS(660, "/user/" + userName + "/streaming", "InputFile", this.getHdfsBaseUrl()); }
	//@Test public void testCacheFiles670() throws Exception { cacheFilesFileOnFS(670, "/user/" + userName + "/streaming", "InputDir", "file://"); }
	@Test public void testCacheFiles680() throws Exception { cacheFilesFileOnFS(680, "/user/" + userName + "/streaming", "InputDir", this.getHdfsBaseUrl()); }

	@Test public void testCacheFiles690() throws Exception { cacheFilesSymlinkOnBadCache(690, "/tmp/streaming", "file://"); }
	@Test public void testCacheFiles700() throws Exception { cacheFilesSymlinkOnBadCache(700, "/tmp/streaming", this.getHdfsBaseUrl()); }
	@Test public void testCacheFiles710() throws Exception { cacheFilesSymlinkOnBadCache(710, "/user/" + userName + "/streaming", "file://"); }
	@Test public void testCacheFiles720() throws Exception { cacheFilesSymlinkOnBadCache(720, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }
	
	@Test public void testCacheFiles730() throws Exception { cacheFilesNoSymlinkOnCache(730, "/tmp/streaming", "InputFile", "file://"); }
	@Test public void testCacheFiles740() throws Exception { cacheFilesNoSymlinkOnCache(740, "/tmp/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testCacheFiles750() throws Exception { cacheFilesNoSymlinkOnCache(750, "/tmp/streaming", "InputDir", "file://"); }
	@Test public void testCacheFiles760() throws Exception { cacheFilesNoSymlinkOnCache(760, "/tmp/streaming", "InputDir", this.getHdfsBaseUrl()); }
	@Test public void testCacheFiles770() throws Exception { cacheFilesNoSymlinkOnCache(770, "/user/" + userName + "/streaming", "InputFile", "file://"); }
	@Test public void testCacheFiles780() throws Exception { cacheFilesNoSymlinkOnCache(780, "/user/" + userName + "/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testCacheFiles790() throws Exception { cacheFilesNoSymlinkOnCache(790, "/user/" + userName + "/streaming", "InputDir", "file://"); }
	@Test public void testCacheFiles800() throws Exception { cacheFilesNoSymlinkOnCache(800, "/user/" + userName + "/streaming", "InputDir", this.getHdfsBaseUrl()); }

	@Test public void testFiles810() throws Exception { filesFilesOnFS(810, "/tmp/streaming", "InputFile", "file://"); }
	@Test public void testFiles820() throws Exception { filesFilesOnFS(820, "/tmp/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testFiles830() throws Exception { filesFilesOnFS(830, "/tmp/streaming", "InputDir", "file://"); }
	@Test public void testFiles840() throws Exception { filesFilesOnFS(840, "/tmp/streaming", "InputDir", this.getHdfsBaseUrl()); }
	@Test public void testFiles850() throws Exception { filesFilesOnFS(850, "/user/" + userName + "/streaming", "InputFile", "file://"); }
	@Test public void testFiles860() throws Exception { filesFilesOnFS(860, "/user/" + userName + "/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testFiles870() throws Exception { filesFilesOnFS(870, "/user/" + userName + "/streaming", "InputDir", "file://"); }
	@Test public void testFiles880() throws Exception { filesFilesOnFS(880, "/user/" + userName + "/streaming", "InputDir", this.getHdfsBaseUrl()); }
	
	@Test public void testFiles890() throws Exception { filesSymlinkOnBadCache(890, "/tmp/streaming", "file://"); }
	@Test public void testFiles900() throws Exception { filesSymlinkOnBadCache(900, "/tmp/streaming", this.getHdfsBaseUrl()); }
	@Test public void testFiles910() throws Exception { filesSymlinkOnBadCache(910, "/user/" + userName + "/streaming", "file://"); }
	@Test public void testFiles920() throws Exception { filesSymlinkOnBadCache(920, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }
	
	@Test public void testFiles930() throws Exception { filesNoSymlinkOnCache(930, "/tmp/streaming", "InputFile", "file://"); }
	@Test public void testFiles940() throws Exception { filesNoSymlinkOnCache(940, "/tmp/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testFiles950() throws Exception { filesNoSymlinkOnCache(950, "/tmp/streaming", "InputDir", "file://"); }
	@Test public void testFiles960() throws Exception { filesNoSymlinkOnCache(960, "/tmp/streaming", "InputDir", this.getHdfsBaseUrl()); }
	@Test public void testFiles970() throws Exception { filesNoSymlinkOnCache(970, "/user/" + userName + "/streaming", "InputFile", "file://"); }
	@Test public void testFiles980() throws Exception { filesNoSymlinkOnCache(980, "/user/" + userName + "/streaming", "InputFile", this.getHdfsBaseUrl()); }
	@Test public void testFiles990() throws Exception { filesNoSymlinkOnCache(990, "/user/" + userName + "/streaming", "InputDir", "file://"); }
	@Test public void testFiles1000() throws Exception { filesNoSymlinkOnCache(1000, "/user/" + userName + "/streaming", "InputDir", this.getHdfsBaseUrl()); }
	
	@Test public void testFiles1010() throws Exception { filesNonExistentInput(1010, "/tmp/streaming", "file://"); }
	@Test public void testFiles1020() throws Exception { filesNonExistentInput(1020, "/tmp/streaming", this.getHdfsBaseUrl()); }
	@Test public void testFiles1030() throws Exception { filesNonExistentInput(1030, "/user/" + userName + "/streaming", "file://"); }
	@Test public void testFiles1040() throws Exception { filesNonExistentInput(1040, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }
	
	// Currently not working properly.  The special characters are failing task attempts.
	//@Test public void testFiles1050() throws Exception { filesSymlinkSpecialChars(1050, "/tmp/streaming", "file://"); }
	//@Test public void testFiles1060() throws Exception { filesSymlinkSpecialChars(1060, "/tmp/streaming", this.getHdfsBaseUrl()); }
	//@Test public void testFiles1070() throws Exception { filesSymlinkSpecialChars(1070, "/user/" + userName + "/streaming", "file://"); }
	//@Test public void testFiles1080() throws Exception { filesSymlinkSpecialChars(1080, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }

	@Test public void testFiles1090() throws Exception { filesSymlinkSpecialCharsFail(1090, "/tmp/streaming", "file://"); }
	@Test public void testFiles1100() throws Exception { filesSymlinkSpecialCharsFail(1100, "/tmp/streaming", this.getHdfsBaseUrl()); }
	@Test public void testFiles1110() throws Exception { filesSymlinkSpecialCharsFail(1110, "/user/" + userName + "/streaming", "file://"); }
	@Test public void testFiles1120() throws Exception { filesSymlinkSpecialCharsFail(1120, "/user/" + userName + "/streaming", this.getHdfsBaseUrl()); }

	private void filesSymlinkSpecialCharsFail(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		String file = "InputFile";
		
		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for symlinks with " + 
				"special characters such as ! @ $ & * ( ) - _ + = for " + 
				"file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink#%^");

		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("java.lang.IllegalArgumentException")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void filesSymlinkSpecialChars(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		String file = "InputFile";
		
		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for symlinks with " + 
				"special characters such as ! @ $ & * ( ) - _ + = for " + 
				"file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink\'!@$&*()-_+=\'");

		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void filesNonExistentInput(int testcaseID, 
			String publicPrivateCache, String fileSystem)
					throws Exception {
		
		String archive = "nonExistentInput";

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for non existent " + 
				"file/directory on " + fileSystem + " in " + 
				cacheInCommand + ".");

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + archive);
		
		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("java.io.FileNotFoundException")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void filesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String file, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option without specifying " + 
				"symlink for file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file);
		
		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void filesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String archive = "nonExistentInput";
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = cachedirPath.substring(0, 
					cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for non existent " + 
				"symlink on " + fileSystem + " in " + cacheInCommand);

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");
		
		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("java.io.FileNotFoundException")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void filesFilesOnFS(int testcaseID, String publicPrivateCache, 
			String file, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + file + ".");

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
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink");
		
		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void cacheFilesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String file, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option without specifying " + 
				"symlink for file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

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
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + file);
		
		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("You need to specify the uris as ")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void cacheFilesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String archive = "nonExistentInput";
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = cachedirPath.substring(0, 
					cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option for non existent " + 
				"symlink on " + fileSystem + " in " + cacheInCommand);

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
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");
		
		String[] output = job.submitUnthreaded();

		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("Error launching job , bad input path :")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void cacheFilesFileOnFS(int testcaseID, String publicPrivateCache, 
			String file, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/expectedOutput");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("expectedOutput"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + file + ".");
		
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
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink");
		
		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void archivesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String archive, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/cachedir" + archive);
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("cachedir"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/cachedir" + archive), 
							cacheInCommand + "/cachedir" + archive);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archives option for file with no " + 
				"symlink on " + fileSystem + " in " + cacheInCommand + 
				" for " + archive + " file");

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
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/cachedir" + archive);
		
		job.start();

		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}
	
	private void archivesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String archive = "nonExistentcachedir.zip";

		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/cachedir.zip");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("cachedir"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;			
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archives option for non existent " + 
				"symlink on " + fileSystem + " in " + cacheInCommand + ".");

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
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");

		String[] output = job.submitUnthreaded();
		
		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains("java.io.FileNotFoundException")) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void archivesFileOnFS(int testcaseID, String publicPrivateCache,
			String archive, String fileSystem) throws Exception {

		this.setupHdfsDir("/tmp/streaming/" + testcaseID);
		this.setupHdfsDir("/tmp/streaming/streaming-" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/" + testcaseID);
		this.setupHdfsDir("/user/" + userName + "/streaming/streaming-" + 
				testcaseID);
		
		String cacheInCommand = publicPrivateCache;
		if (fileSystem.equals("file://")) {
			
			String cachedirPath = this.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/cachedir" + archive);
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("cachedir"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;

			this.putLocalToHdfs(
					this.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/cachedir" + archive), 
							cacheInCommand + "/cachedir" + archive);
		}
		
		this.putLocalToHdfs(
				this.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archives option for file on " + 
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

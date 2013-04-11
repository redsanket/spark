package hadooptest.regression.yarn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.YarnClientImpl;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import hadooptest.ParallelMethodTests;
import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.DFS;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.config.TestConfiguration;
import hadooptest.job.StreamingJob;
import hadooptest.job.JobState;

/**
 * YARN regression tests to exercise Hadoop streaming.
 */
@Category(ParallelMethodTests.class)
public class TestEndToEndStreaming extends TestSession {
	
	private static final String USER_NAME = System.getProperty("user.name");
	private static final String CACHE_TMP = "/tmp/streaming";
	private static final String CACHE_USER = "/user/" + USER_NAME + "/streaming";
	private static final String LOCALFS_BASE = "file://";
	private static final String JAR = ".jar";
	private static final String TAR = ".tar";
	private static final String TARGZ = ".tar.gz";
	private static final String TGZ = ".tgz";
	private static final String ZIP = ".zip";
	private static final String INPUTFILE = "InputFile";
	private static final String INPUTDIR = "InputDir";

	private static DFS hdfs;
	private static String hdfsBaseURL;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		
		hdfs = new DFS();
		hdfsBaseURL = hdfs.getBaseUrl();
		setupTestConf();
	}

	@Test public void testCacheArchives10() throws Exception { cacheArchivesFileOnCache(10, CACHE_TMP, JAR); }
	@Test public void testCacheArchives20() throws Exception { cacheArchivesFileOnCache(20, CACHE_TMP, TAR); }
	@Test public void testCacheArchives30() throws Exception { cacheArchivesFileOnCache(30, CACHE_TMP, TARGZ); }
	@Test public void testCacheArchives40() throws Exception { cacheArchivesFileOnCache(40, CACHE_TMP, TGZ); }
	@Test public void testCacheArchives50() throws Exception { cacheArchivesFileOnCache(50, CACHE_TMP, ZIP); }
	@Test public void testCacheArchives60() throws Exception { cacheArchivesFileOnCache(60, CACHE_USER, JAR); }
	@Test public void testCacheArchives70() throws Exception { cacheArchivesFileOnCache(70, CACHE_USER, TAR); }
	@Test public void testCacheArchives80() throws Exception { cacheArchivesFileOnCache(80, CACHE_USER, TARGZ); }
	@Test public void testCacheArchives90() throws Exception { cacheArchivesFileOnCache(90, CACHE_USER, TGZ); }
	@Test public void testCacheArchives100() throws Exception { cacheArchivesFileOnCache(100, CACHE_USER, ZIP); }
	
	@Test public void testCacheArchives110() throws Exception { cacheArchivesSymlinkOnBadCache(110, CACHE_TMP); }
	@Test public void testCacheArchives120() throws Exception { cacheArchivesSymlinkOnBadCache(120, CACHE_USER); }

	@Test public void testCacheArchives130() throws Exception { cacheArchivesNoSymlinkOnCache(130, CACHE_TMP); }
	@Test public void testCacheArchives140() throws Exception { cacheArchivesNoSymlinkOnCache(140, CACHE_USER); }
	
	@Test public void testArchives160() throws Exception { archivesFileOnFS(160, CACHE_TMP, JAR, LOCALFS_BASE); }
	@Test public void testArchives170() throws Exception { archivesFileOnFS(170, CACHE_TMP, JAR, hdfsBaseURL); }
	@Test public void testArchives180() throws Exception { archivesFileOnFS(180, CACHE_TMP, TAR, LOCALFS_BASE); }
	@Test public void testArchives190() throws Exception { archivesFileOnFS(190, CACHE_TMP, TAR, hdfsBaseURL); }
	@Test public void testArchives200() throws Exception { archivesFileOnFS(200, CACHE_TMP, TARGZ, LOCALFS_BASE); }
	@Test public void testArchives210() throws Exception { archivesFileOnFS(210, CACHE_TMP, TARGZ, hdfsBaseURL); }
	@Test public void testArchives220() throws Exception { archivesFileOnFS(220, CACHE_TMP, TGZ, LOCALFS_BASE); }
	@Test public void testArchives230() throws Exception { archivesFileOnFS(230, CACHE_TMP, TGZ, hdfsBaseURL); }
	@Test public void testArchives240() throws Exception { archivesFileOnFS(240, CACHE_TMP, ZIP, LOCALFS_BASE); }
	@Test public void testArchives250() throws Exception { archivesFileOnFS(250, CACHE_TMP, ZIP, hdfsBaseURL); }
	@Test public void testArchives260() throws Exception { archivesFileOnFS(260, CACHE_USER, JAR, LOCALFS_BASE); }
	@Test public void testArchives270() throws Exception { archivesFileOnFS(270, CACHE_USER, JAR, hdfsBaseURL); }
	@Test public void testArchives280() throws Exception { archivesFileOnFS(280, CACHE_USER, TAR, LOCALFS_BASE); }
	@Test public void testArchives290() throws Exception { archivesFileOnFS(290, CACHE_USER, TAR, hdfsBaseURL); }
	@Test public void testArchives300() throws Exception { archivesFileOnFS(300, CACHE_USER, TARGZ, LOCALFS_BASE); }
	@Test public void testArchives310() throws Exception { archivesFileOnFS(310, CACHE_USER, TARGZ, hdfsBaseURL); }
	@Test public void testArchives320() throws Exception { archivesFileOnFS(320, CACHE_USER, TGZ, LOCALFS_BASE); }
	@Test public void testArchives330() throws Exception { archivesFileOnFS(330, CACHE_USER, TGZ, hdfsBaseURL); }
	@Test public void testArchives340() throws Exception { archivesFileOnFS(340, CACHE_USER, ZIP, LOCALFS_BASE); }
	@Test public void testArchives350() throws Exception { archivesFileOnFS(350, CACHE_USER, ZIP, hdfsBaseURL); }

	@Test public void testArchives360() throws Exception { archivesSymlinkOnBadCache(360, CACHE_TMP, LOCALFS_BASE); }
	@Test public void testArchives370() throws Exception { archivesSymlinkOnBadCache(370, CACHE_TMP, hdfsBaseURL); }
	@Test public void testArchives380() throws Exception { archivesSymlinkOnBadCache(380, CACHE_USER, LOCALFS_BASE); }
	@Test public void testArchives390() throws Exception { archivesSymlinkOnBadCache(390, CACHE_USER, hdfsBaseURL); }
	
	@Test public void testArchives400() throws Exception { archivesNoSymlinkOnCache(400, CACHE_TMP, JAR, LOCALFS_BASE); }
	@Test public void testArchives410() throws Exception { archivesNoSymlinkOnCache(410, CACHE_TMP, JAR, hdfsBaseURL); }
	@Test public void testArchives420() throws Exception { archivesNoSymlinkOnCache(420, CACHE_TMP, TAR, LOCALFS_BASE); }
	@Test public void testArchives430() throws Exception { archivesNoSymlinkOnCache(430, CACHE_TMP, TAR, hdfsBaseURL); }
	@Test public void testArchives440() throws Exception { archivesNoSymlinkOnCache(440, CACHE_TMP, TARGZ, LOCALFS_BASE); }
	@Test public void testArchives450() throws Exception { archivesNoSymlinkOnCache(450, CACHE_TMP, TARGZ, hdfsBaseURL); }
	@Test public void testArchives460() throws Exception { archivesNoSymlinkOnCache(460, CACHE_TMP, TGZ, LOCALFS_BASE); }
	@Test public void testArchives470() throws Exception { archivesNoSymlinkOnCache(470, CACHE_TMP, TGZ, hdfsBaseURL); }
	@Test public void testArchives480() throws Exception { archivesNoSymlinkOnCache(480, CACHE_TMP, ZIP, LOCALFS_BASE); }
	@Test public void testArchives490() throws Exception { archivesNoSymlinkOnCache(490, CACHE_TMP, ZIP, hdfsBaseURL); }
	@Test public void testArchives500() throws Exception { archivesNoSymlinkOnCache(500, CACHE_USER, JAR, LOCALFS_BASE); }
	@Test public void testArchives510() throws Exception { archivesNoSymlinkOnCache(510, CACHE_USER, JAR, hdfsBaseURL); }
	@Test public void testArchives520() throws Exception { archivesNoSymlinkOnCache(520, CACHE_USER, TAR, LOCALFS_BASE); }
	@Test public void testArchives530() throws Exception { archivesNoSymlinkOnCache(530, CACHE_USER, TAR, hdfsBaseURL); }
	@Test public void testArchives540() throws Exception { archivesNoSymlinkOnCache(540, CACHE_USER, TARGZ, LOCALFS_BASE); }
	@Test public void testArchives550() throws Exception { archivesNoSymlinkOnCache(550, CACHE_USER, TARGZ, hdfsBaseURL); }
	@Test public void testArchives560() throws Exception { archivesNoSymlinkOnCache(560, CACHE_USER, TGZ, LOCALFS_BASE); }
	@Test public void testArchives570() throws Exception { archivesNoSymlinkOnCache(570, CACHE_USER, TGZ, hdfsBaseURL); }
	@Test public void testArchives580() throws Exception { archivesNoSymlinkOnCache(580, CACHE_USER, ZIP, LOCALFS_BASE); }
	@Test public void testArchives590() throws Exception { archivesNoSymlinkOnCache(590, CACHE_USER, ZIP, hdfsBaseURL); }
	
	//
	// Tests 610, 630, 650, and 670 are currently commented out, because
	// -cacheFile is now deprecated and these cases are no longer supported by
	// Hadoop (at least as of 0.23.6).
	//
	//@Test public void testCacheFiles610() throws Exception { cacheFilesFileOnFS(610, CACHE_TMP, INPUTFILE, LOCALFS_BASE); }
	@Test public void testCacheFiles620() throws Exception { cacheFilesFileOnFS(620, CACHE_TMP, INPUTFILE, hdfsBaseURL); }
	//@Test public void testCacheFiles630() throws Exception { cacheFilesFileOnFS(630, CACHE_TMP, INPUTDIR, LOCALFS_BASE); }
	@Test public void testCacheFiles640() throws Exception { cacheFilesFileOnFS(640, CACHE_TMP, INPUTDIR, hdfsBaseURL); }
	//@Test public void testCacheFiles650() throws Exception { cacheFilesFileOnFS(650, CACHE_USER, INPUTFILE, LOCALFS_BASE); }
	@Test public void testCacheFiles660() throws Exception { cacheFilesFileOnFS(660, CACHE_USER, INPUTFILE, hdfsBaseURL); }
	//@Test public void testCacheFiles670() throws Exception { cacheFilesFileOnFS(670, CACHE_USER, INPUTDIR, LOCALFS_BASE); }
	@Test public void testCacheFiles680() throws Exception { cacheFilesFileOnFS(680, CACHE_USER, INPUTDIR, hdfsBaseURL); }

	@Test public void testCacheFiles690() throws Exception { cacheFilesSymlinkOnBadCache(690, CACHE_TMP, LOCALFS_BASE); }
	@Test public void testCacheFiles700() throws Exception { cacheFilesSymlinkOnBadCache(700, CACHE_TMP, hdfsBaseURL); }
	@Test public void testCacheFiles710() throws Exception { cacheFilesSymlinkOnBadCache(710, CACHE_USER, LOCALFS_BASE); }
	@Test public void testCacheFiles720() throws Exception { cacheFilesSymlinkOnBadCache(720, CACHE_USER, hdfsBaseURL); }
	
	@Test public void testCacheFiles730() throws Exception { cacheFilesNoSymlinkOnCache(730, CACHE_TMP, INPUTFILE, LOCALFS_BASE); }
	@Test public void testCacheFiles740() throws Exception { cacheFilesNoSymlinkOnCache(740, CACHE_TMP, INPUTFILE, hdfsBaseURL); }
	@Test public void testCacheFiles750() throws Exception { cacheFilesNoSymlinkOnCache(750, CACHE_TMP, INPUTDIR, LOCALFS_BASE); }
	@Test public void testCacheFiles760() throws Exception { cacheFilesNoSymlinkOnCache(760, CACHE_TMP, INPUTDIR, hdfsBaseURL); }
	@Test public void testCacheFiles770() throws Exception { cacheFilesNoSymlinkOnCache(770, CACHE_USER, INPUTFILE, LOCALFS_BASE); }
	@Test public void testCacheFiles780() throws Exception { cacheFilesNoSymlinkOnCache(780, CACHE_USER, INPUTFILE, hdfsBaseURL); }
	@Test public void testCacheFiles790() throws Exception { cacheFilesNoSymlinkOnCache(790, CACHE_USER, INPUTDIR, LOCALFS_BASE); }
	@Test public void testCacheFiles800() throws Exception { cacheFilesNoSymlinkOnCache(800, CACHE_USER, INPUTDIR, hdfsBaseURL); }

	@Test public void testFiles810() throws Exception { filesFilesOnFS(810, CACHE_TMP, INPUTFILE, LOCALFS_BASE); }
	@Test public void testFiles820() throws Exception { filesFilesOnFS(820, CACHE_TMP, INPUTFILE, hdfsBaseURL); }
	@Test public void testFiles830() throws Exception { filesFilesOnFS(830, CACHE_TMP, INPUTDIR, LOCALFS_BASE); }
	@Test public void testFiles840() throws Exception { filesFilesOnFS(840, CACHE_TMP, INPUTDIR, hdfsBaseURL); }
	@Test public void testFiles850() throws Exception { filesFilesOnFS(850, CACHE_USER, INPUTFILE, LOCALFS_BASE); }
	@Test public void testFiles860() throws Exception { filesFilesOnFS(860, CACHE_USER, INPUTFILE, hdfsBaseURL); }
	@Test public void testFiles870() throws Exception { filesFilesOnFS(870, CACHE_USER, INPUTDIR, LOCALFS_BASE); }
	@Test public void testFiles880() throws Exception { filesFilesOnFS(880, CACHE_USER, INPUTDIR, hdfsBaseURL); }
	
	@Test public void testFiles890() throws Exception { filesSymlinkOnBadCache(890, CACHE_TMP, LOCALFS_BASE); }
	@Test public void testFiles900() throws Exception { filesSymlinkOnBadCache(900, CACHE_TMP, hdfsBaseURL); }
	@Test public void testFiles910() throws Exception { filesSymlinkOnBadCache(910, CACHE_USER, LOCALFS_BASE); }
	@Test public void testFiles920() throws Exception { filesSymlinkOnBadCache(920, CACHE_USER, hdfsBaseURL); }
	
	@Test public void testFiles930() throws Exception { filesNoSymlinkOnCache(930, CACHE_TMP, INPUTFILE, LOCALFS_BASE); }
	@Test public void testFiles940() throws Exception { filesNoSymlinkOnCache(940, CACHE_TMP, INPUTFILE, hdfsBaseURL); }
	@Test public void testFiles950() throws Exception { filesNoSymlinkOnCache(950, CACHE_TMP, INPUTDIR, LOCALFS_BASE); }
	@Test public void testFiles960() throws Exception { filesNoSymlinkOnCache(960, CACHE_TMP, INPUTDIR, hdfsBaseURL); }
	@Test public void testFiles970() throws Exception { filesNoSymlinkOnCache(970, CACHE_USER, INPUTFILE, LOCALFS_BASE); }
	@Test public void testFiles980() throws Exception { filesNoSymlinkOnCache(980, CACHE_USER, INPUTFILE, hdfsBaseURL); }
	@Test public void testFiles990() throws Exception { filesNoSymlinkOnCache(990, CACHE_USER, INPUTDIR, LOCALFS_BASE); }
	@Test public void testFiles1000() throws Exception { filesNoSymlinkOnCache(1000, CACHE_USER, INPUTDIR, hdfsBaseURL); }
	
	@Test public void testFiles1010() throws Exception { filesNonExistentInput(1010, CACHE_TMP, LOCALFS_BASE); }
	@Test public void testFiles1020() throws Exception { filesNonExistentInput(1020, CACHE_TMP, hdfsBaseURL); }
	@Test public void testFiles1030() throws Exception { filesNonExistentInput(1030, CACHE_USER, LOCALFS_BASE); }
	@Test public void testFiles1040() throws Exception { filesNonExistentInput(1040, CACHE_USER, hdfsBaseURL); }
	
	//@Test public void testFiles1050() throws Exception { filesSymlinkSpecialChars(1050, CACHE_TMP, LOCALFS_BASE); }
	//@Test public void testFiles1060() throws Exception { filesSymlinkSpecialChars(1060, CACHE_TMP, hdfsBaseURL); }
	//@Test public void testFiles1070() throws Exception { filesSymlinkSpecialChars(1070, CACHE_USER, LOCALFS_BASE); }
	//@Test public void testFiles1080() throws Exception { filesSymlinkSpecialChars(1080, CACHE_USER, hdfsBaseURL); }

	@Test public void testFiles1090() throws Exception { filesSymlinkSpecialCharsFail(1090, CACHE_TMP, LOCALFS_BASE); }
	@Test public void testFiles1100() throws Exception { filesSymlinkSpecialCharsFail(1100, CACHE_TMP, hdfsBaseURL); }
	@Test public void testFiles1110() throws Exception { filesSymlinkSpecialCharsFail(1110, CACHE_USER, LOCALFS_BASE); }
	@Test public void testFiles1120() throws Exception { filesSymlinkSpecialCharsFail(1120, CACHE_USER, hdfsBaseURL); }
	
	private void filesSymlinkSpecialCharsFail(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		String file = INPUTFILE;
		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for symlinks with " + 
				"special characters such as ! @ $ & * ( ) - _ + = for " + 
				"file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");
		
		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink#%^");

		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, "java.lang.IllegalArgumentException");
	}
	
	private void filesSymlinkSpecialChars(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		String file = INPUTFILE;
		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for symlinks with " + 
				"special characters such as ! @ $ & * ( ) - _ + = for " + 
				"file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink\'!@$&*()-_+=\'");

		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void filesNonExistentInput(int testcaseID, 
			String publicPrivateCache, String fileSystem)
					throws Exception {
		
		String archive = "nonExistentInput";
		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for non existent " + 
				"file/directory on " + fileSystem + " in " + 
				cacheInCommand + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + archive);
		
		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, "java.io.FileNotFoundException");
	}
	
	private void filesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String file, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option without specifying " + 
				"symlink for file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file);
		
		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void filesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {
		
		this.setupHDFSTestDirs(testcaseID);
		String archive = "nonExistentInput";
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = cachedirPath.substring(0, 
					cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for non existent " + 
				"symlink on " + fileSystem + " in " + cacheInCommand);

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");
		
		String[] output = job.submitUnthreaded();
		
		this.checkForOutputError(output, "java.io.FileNotFoundException");
	}
	
	private void filesFilesOnFS(int testcaseID, String publicPrivateCache, 
			String file, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -files option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + file + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setFilesPath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink");
		
		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void cacheFilesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String file, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("input.txt"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option without specifying " + 
				"symlink for file on " + fileSystem + " in " + cacheInCommand + 
				" for " + file + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + file);
		
		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, "You need to specify the uris as ");
	}
	
	private void cacheFilesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String archive = "nonExistentInput";
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/input.txt");
			cacheInCommand = cachedirPath.substring(0, 
					cachedirPath.indexOf("input.txt"));
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option for non existent " + 
				"symlink on " + fileSystem + " in " + cacheInCommand);

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");
		
		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, 
				"Error launching job , bad input path :");
	}
	
	private void cacheFilesFileOnFS(int testcaseID, String publicPrivateCache, 
			String file, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/expectedOutput");
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("expectedOutput"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/" + file), 
							cacheInCommand + "/" + file);		
		}

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheFile option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + file + ".");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheFilePath(fileSystem + cacheInCommand + 
				"/" + file + "#testlink");
		
		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void archivesNoSymlinkOnCache(int testcaseID, 
			String publicPrivateCache, String archive, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/cachedir" + archive);
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("cachedir"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;	
			
			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/cachedir" + archive), 
							cacheInCommand + "/cachedir" + archive);		
		}
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archives option for file with no " + 
				"symlink on " + fileSystem + " in " + cacheInCommand + 
				" for " + archive + " file");

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/cachedir" + archive);
		
		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void archivesSymlinkOnBadCache(int testcaseID, 
			String publicPrivateCache, String fileSystem) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String archive = "nonExistentcachedir.zip";
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			String cachedirPath = Util.getResourceFullPath(
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

		this.putInputFileHDFS(testcaseID);

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/" + archive + "#testlink");

		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, "java.io.FileNotFoundException");
	}
	
	private void archivesFileOnFS(int testcaseID, String publicPrivateCache,
			String archive, String fileSystem) throws Exception {

		this.setupHDFSTestDirs(testcaseID);
		String cacheInCommand = publicPrivateCache;
		
		if (fileSystem.equals(LOCALFS_BASE)) {
			
			String cachedirPath = Util.getResourceFullPath(
					"data/streaming/streaming-" + testcaseID + 
					"/cachedir" + archive);
			cacheInCommand = 
					cachedirPath.substring(0, cachedirPath.indexOf("cachedir"));
		}
		else {
			cacheInCommand = publicPrivateCache + "/" + testcaseID;

			hdfs.putFileLocalToHdfs(
					Util.getResourceFullPath("data/streaming/streaming-" + 
							testcaseID + "/cachedir" + archive), 
							cacheInCommand + "/cachedir" + archive);
		}

		this.putInputFileHDFS(testcaseID);
		
		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -archives option for file on " + 
				fileSystem + " in " + cacheInCommand + " for " + archive + 
				" file.");

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setArchivePath(fileSystem + cacheInCommand + 
				"/cachedir" + archive + "#testlink");

		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}

	private void cacheArchivesNoSymlinkOnCache(int testcaseID, String publicPrivateCache) 
			throws Exception {

		String archive = "cachedir.zip";

		logger.info("Streaming-" + testcaseID + " - Test to check the " + 
				"-cacheArchive when no symlink is specified on " + 
				publicPrivateCache);

		hdfs.putFileLocalToHdfs(
				Util.getResourceFullPath("data/streaming/streaming-" +
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheArchivePath(hdfsBaseURL + publicPrivateCache + 
				"/" + archive);

		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, "You need to specify the uris as " + 
				"scheme://path#linkname,Please specify a different " + 
				"link name for all of your caching URIs");
	}
	
	private void cacheArchivesSymlinkOnBadCache(int testcaseID, String publicPrivateCache) 
			throws Exception {

		String archive = "nonExistentcachedir.zip";

		logger.info("Streaming-" + testcaseID + " - Test to check the " + 
				"-cacheArchive option for non existent symlink on " + 
				publicPrivateCache);

		hdfs.putFileLocalToHdfs(
				Util.getResourceFullPath("data/streaming/streaming-" +
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheArchivePath(hdfsBaseURL + publicPrivateCache + 
				"/" + archive + "#testlink");

		String[] output = job.submitUnthreaded();

		this.checkForOutputError(output, 
				"Error launching job , bad input path : " +
				"File does not exist:");
	}
	
	private void cacheArchivesFileOnCache(int testcaseID, 
			String publicPrivateCache, 
			String archive) 
					throws Exception {

		this.setupHDFSTestDirs(testcaseID);

		String cacheInCommand = publicPrivateCache + "/" + testcaseID;

		logger.info("Streaming-" + testcaseID + 
				" - Test to check the -cacheArchive option for " + archive + 
				" file on " + publicPrivateCache);

		hdfs.putFileLocalToHdfs(
				Util.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/cachedir" + archive), 
						cacheInCommand + "/cachedir" + archive);
		hdfs.putFileLocalToHdfs(
				Util.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");

		StreamingJob job = this.setupDefaultStreamingJob(testcaseID);
		job.setCacheArchivePath(hdfsBaseURL + cacheInCommand + 
				"/cachedir" + archive + "#testlink");

		job.start();
		this.waitForSuccessAndValidate(testcaseID, job);
	}
	
	private void validateOutput(int testcaseID) throws Exception {
		FileSystem fs = TestSession.cluster.getFS();
		FileStatus[] elements = fs.listStatus(new Path(hdfsBaseURL + 
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
					new File(Util.getResourceFullPath("" +
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
	
	public static void setupTestConf() throws Exception {
		FullyDistributedCluster cluster =
				(FullyDistributedCluster) TestSession.cluster;
		String component = TestConfiguration.RESOURCE_MANAGER;

		YarnClientImpl yarnClient = new YarnClientImpl();
		yarnClient.init(TestSession.getCluster().getConf());
		yarnClient.start();

		List<QueueInfo> queues =  yarnClient.getAllQueues(); 
		assertNotNull("Expected cluster queue(s) not found!!!", queues);		
		TestSession.logger.info("queues='" +
        	Arrays.toString(queues.toArray()) + "'");
		if ((queues.size() == 1) &&
			(Float.compare(queues.get(0).getCapacity(), 1.0f) == 0)) {
				TestSession.logger.debug("Cluster is already setup properly." +
						"Nothing to do.");
				return;
		}
		
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
	
	private void setupHDFSTestDirs(int testcaseID) throws Exception {
		hdfs.mkdir("/tmp/streaming/" + testcaseID);
		hdfs.mkdir("/tmp/streaming/streaming-" + testcaseID);
		hdfs.mkdir("/user/" + USER_NAME + "/streaming/" + testcaseID);
		hdfs.mkdir("/user/" + USER_NAME + "/streaming/streaming-" + 
				testcaseID);
	}
	
	private void waitForSuccessAndValidate(int testcaseID, StreamingJob job) 
			throws Exception {
		assertTrue("Streaming job was not assigned an ID within 30 seconds.", 
				job.waitForID(30));
		assertTrue("Sleep job ID for sleep job (default user) is invalid.", 
				job.verifyID());

		assertTrue("Streaming job did not succeed", 
				job.waitFor(JobState.SUCCEEDED, 240));

		this.validateOutput(testcaseID);
	}

	private void checkForOutputError(String[] output, String error) 
			throws Exception {
		boolean foundError = false;
		for (int i = 0; i < output.length; i++) {
			logger.debug("OUTPUT" + i + ": " + output[i]);
			if (output[i].contains(error)) {
				foundError = true;
				break;
			}
		}

		assertTrue("Streaming job failure output string is not " + 
				"correctly formed.", foundError);
	}
	
	private void putInputFileHDFS(int testcaseID) throws Exception {
		hdfs.putFileLocalToHdfs(
				Util.getResourceFullPath("data/streaming/streaming-" + 
						testcaseID + "/input.txt"), 
						"/tmp/streaming/streaming-" + testcaseID + 
				"/input.txt");
	}
	
	private StreamingJob setupDefaultStreamingJob(int testcaseID) 
			throws Exception {
		StreamingJob job = new StreamingJob();
		job.setNumMappers(1);
		job.setNumReducers(1);
		job.setName("streamingTest-" + testcaseID);
		job.setYarnOptions("-Dmapreduce.job.acl-view-job=*");
		job.setInputFile(hdfsBaseURL + "/tmp/streaming/streaming-" + 
				testcaseID + "/input.txt");
		job.setMapper("\"xargs cat\"");
		job.setReducer("cat");
		job.setOutputPath(hdfsBaseURL + "/tmp/streaming/streaming-" + 
				testcaseID + "/Output");
		
		return job;
	}
}

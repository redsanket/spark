package hadooptest.hadoop.regression.dfs;

import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.hadoop.regression.dfs.DfsCliCommands.GenericCliResponseBO;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Force;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.Recursive;
import hadooptest.hadoop.regression.dfs.DfsTestsBaseClass.SkipTrash;

import java.io.IOException;
import java.net.InetAddress;
import java.util.EnumSet;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.CreateOpts;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.fail;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestAppend {
    private String testName;
	private static String FILE_SYSTEM_ENTITY_DIRECTORY = "DIRECTORY";
	private static final String APPEND_OUTDIR = "/tmp/M";
	private static final String LOCAL_SOURCE_FILES = "/homes/hdfsqa/hdfsMNNData/";
	private static final int DEFAULT_BLOCK_SIZE = 1024;
	private static final int DEFAULT_NUM_BLOCKS = 10000;

	private static final int numBlocks = DEFAULT_NUM_BLOCKS;
	private static final int blockSize = DEFAULT_BLOCK_SIZE;
	private static final int DefaultBufferSize = numBlocks * blockSize; // 10MB

	private static final String APPEND_ROOT_DIR = "/";
	private static final int counter = 0x10011001;
	private static final boolean dumpmemFlag = false;

	private static int sand = 0; // used in generation of data (in -rw and -wo
									// test)

	// Command Line Options
	boolean quickTestOption = false;
	boolean readOnlyOption = false;
	boolean writeOnlyOption = false;
	boolean bothReadWriteOption = false;
	boolean toPauseOption = false;
	int pauseIntervalMsecOption = 1000; // pause 1 second during each loop by
										// default
	int ntimesOption = 1; // do once by default
	long chunkSizeOption = 100; // default is to write 100 byte a chunk.
	static String fnameOption = null;

	boolean openAndCloseOption = false; // during read cycle: open and close the
										// file during each read
	boolean newReadTheadOption = false; // spawn new thread to read during
										// AppendReadWrite if this option is set
	boolean overwriteOption = false; // OVERWRITE flag (like TRUNCATE)
	boolean appendOption = false; // APPEND flag with file on create
	boolean createOption = false; // CREATE FLAG
	boolean verboseOption = false; // verbose mode
	boolean useFSOption = false; // use FileSystem to do append (default is use
									// FileContext)
	static boolean useFCOption = true; // use FileContext to do append (which is
								// default any way)
	boolean readFullyOption = false; // in read: if set, read every byte until
										// the end of file to arrive at the
										// visible length of file
	boolean alwaysHflushOption = false; // in read: if set, read every byte
										// until the end of file to arrive at
										// the visible length of file
	boolean noHflushOption = false; // in read: if set, read every byte until
									// the end of file to arrive at the visible
									// length of file
	boolean readPassVEndOption = false; // in read: if set, read until the end
										// (else only till Visible/Written)
	boolean posReadOption = false; // use position read if this option is set
									// (default is sequential read)
	boolean blockLocOption = false; // dump out the block location of the file
									// (essentially last block)

	boolean miscOption = false; // run misc command, need next argument
	String miscCmdOption; // Misc Cmd to run

	int nbyteWritten = 0;
	int nbyteVisible = 0;

	static final String cmdUsage = "Usage: [-useFS|-useFC] [-qt|-ro|-wo|-rw] [-overwrite]  [-append] [-create] [-oc] [-nrt] [-readfully]"
			+ " [-verbose] [-p pmsec] [-n ntimes] [-s nbyte] [-f filename] \n"
			+ "Default: -p = 1000 for 1 sec; -n = 1. All boolean default to false.\n"
			+ "The -f filename must be provided.\n"
			+ "[-useFS | -useFC]: use FileSystem or FileContext (default) in file operations\n"
			+ "-qt: quick test\n"
			+ "-ro: read-only; -wo: write-only; -rw: read and write\n"
			+ "-overwrite: overwrite if file already exist\n"
			+ "-append: append if file already exist\n"
			+ "-create: create if file does not exist\n"
			+ "-oc: open and close file during each read or write cycle\n"
			+ "-nrt: spawn new read thread for read during -rw test. No effect in -ro test.\n"
			+ "-readFully: if set, reader will read from begin of file to end. Default is to read 1K from end.\n"
			+ "-alwaysHflush: if set, all write will be followed by hflush (default is do hflush on alternate writes).\n"
			+ "-noHflush: if set, no hflush will be invoked. (default is to do hflush on alternate writes).\n"
			+ "-readPassVEnd: if set, read pass Visible End. (default is to read till Visible).\n"
			+ "-n: iterate ntimes for read or write\n"
			+ "[-posRead | -seqRead]: Use Sequential Read or the default position read to read data\n"
			+ "-blockLoc: dump out the block location and host of the files around the last block\n"
			+ "-p pause pmsec: pause pmsec during each iteration in msec. (0 means no pause).\n"
			+ "-s chunkSize: size of write at a time in byte (default 100 byte)\n"
			+ "-verbose: verbose mode for debugging";

	private static FileContext mfc; // my fc
	private static FileSystem mfs; // my fs
	private Configuration mconfig;

	private long tsBeforeHflush; // time stamp before and after hflush call
	private long tsAfterHflush;
	private long tsBeforeWrite;
	private long tsAfterWrite;

	private long tsBeforeOpen4Read;
	private long tsAfterOpen4Read;
	private long tsAfterRead;

	private long visibleLenFromReadStream; // set by the reader to be used by
											// the writer in rw mode test
	private long byteReadFromThread; // set by the reader of a new thread to be
										// used by the writer in rw mode test

	private String[] inputParams;

	@BeforeClass
	public static void setupData() throws Exception {
		TestSession.start();
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String cluster = System.getProperty("CLUSTER_NAME");
		GenericCliResponseBO genericResponseBO = dfsCommonCli.test(
				new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, cluster, APPEND_OUTDIR,
				FILE_SYSTEM_ENTITY_DIRECTORY);
		if (genericResponseBO.process.exitValue() == 0) {
			TestSession.logger.info("Here deleting dir...!!");
			dfsCommonCli.rm(new HashMap<String, String>(),
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.WEBHDFS, cluster, Recursive.YES,
					Force.YES, SkipTrash.YES, APPEND_OUTDIR);
		}
		dfsCommonCli.mkdir(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, cluster, APPEND_OUTDIR);
		doChmodRecursively(cluster, APPEND_OUTDIR);
		dfsCommonCli.copyFromLocal(new HashMap<String, String>(),
				HadooptestConstants.UserNames.HDFSQA,
				HadooptestConstants.Schema.WEBHDFS, cluster,
				LOCAL_SOURCE_FILES, APPEND_OUTDIR);

	}

	public static void doChmodRecursively(String cluster, String dirHierarchy)
			throws Exception {
		DfsCliCommands dfsCommonCli = new DfsCliCommands();
		String pathSoFar = "/";
		for (String aDir : dirHierarchy.split("/")) {
			if (aDir.isEmpty())
				continue;
			TestSession.logger.info("Processing split:" + aDir);
			pathSoFar = pathSoFar + aDir + "/";
			TestSession.logger.info("PathSoFar:" + pathSoFar);
			dfsCommonCli.chmod(new HashMap<String, String>(),
					HadooptestConstants.UserNames.HDFSQA,
					HadooptestConstants.Schema.WEBHDFS, cluster, pathSoFar,
					"777", Recursive.NO);
		}
	}

	@Before
	public void setup() throws IOException {
		setupFS();
		initDebugInfo();
	}
	
    @AfterClass
    public static void blockLocAfterTests() throws IOException {
        TestSession.logger.info("Dump out Block Location");
        dumpFileBlockLocations(fnameOption);
    }
    
    @Test public void testAppend102() throws Exception { runTest("102", "-useFC -rw -n 10 -s 1000 -p 1000 -f " + APPEND_OUTDIR + "/Z101" ); }
    @Test public void testAppend103() throws Exception { runTest("103", "-useFC -rw -n 10 -s 10000 -p 1000 -f " + APPEND_OUTDIR + "/Z102" ); }
    @Test public void testAppend104() throws Exception { runTest("104", "-useFC -rw -n 10 -s 100000 -p 1000 -f " + APPEND_OUTDIR + "/Z103" ); }
    @Test public void testAppend105() throws Exception { runTest("105", "-useFC -rw -n 10 -s 1000000 -p 1000 -f " + APPEND_OUTDIR + "/Z104" ); }
    
    @Test public void testAppend401() throws Exception { runTest("401", "-useFC -rw -n 9 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z401" ); }
    @Test public void testAppend402() throws Exception { runTest("402", "-useFC -rw -n 100 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z402" ); }
    @Test public void testAppend411() throws Exception { runTest("411", "-useFC -rw -n 9 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z411" ); }
    @Test public void testAppend412() throws Exception { runTest("412", "-useFC -rw -n 100 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z412" ); }
    @Test public void testAppend421() throws Exception { runTest("421", "-useFC -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z421" ); }
    @Test public void testAppend422() throws Exception { runTest("422", "-useFC -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z422" ); }
    @Test public void testAppend431() throws Exception { runTest("431", "-useFC -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z431" ); }
    @Test public void testAppend432() throws Exception { runTest("432", "-useFC -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z432" ); }
    @Test public void testAppend441() throws Exception { runTest("441", "-useFC -rw -n 9 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z441" ); }
    @Test public void testAppend451() throws Exception { runTest("451", "-useFC -readFully -rw -n 9 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z451" ); }
    @Test public void testAppend452() throws Exception { runTest("452", "-useFC -readFully -rw -n 100 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z452" ); }
    @Test public void testAppend461() throws Exception { runTest("461", "-useFC -readFully -rw -n 9 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z461" ); }
    @Test public void testAppend462() throws Exception { runTest("462", "-useFC -readFully -rw -n 100 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z462" ); }
    @Test public void testAppend457() throws Exception { runTest("457", "-useFC -readFully -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z471" ); }
    @Test public void testAppend471() throws Exception { runTest("471", "-useFC -readFully -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z472" ); }
    @Test public void testAppend481() throws Exception { runTest("481", "-useFC -readFully -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z481" ); }
    @Test public void testAppend482() throws Exception { runTest("482", "-useFC -readFully -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z482" ); }
    @Test public void testAppend496() throws Exception { runTest("496", "-useFC -readFully -rw -n 9 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z496" ); }
    @Test public void testAppend501() throws Exception { runTest("501", "-useFC -seqRead -rw -n 9 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z501" ); }
    @Test public void testAppend502() throws Exception { runTest("502", "-useFC -seqRead -rw -n 100 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z502" ); }
    @Test public void testAppend511() throws Exception { runTest("511", "-useFC -seqRead -rw -n 9 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z511" ); }
    @Test public void testAppend512() throws Exception { runTest("512", "-useFC -seqRead -rw -n 100 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z512" ); }
    @Test public void testAppend521() throws Exception { runTest("521", "-useFC -seqRead -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z521" ); }
    @Test public void testAppend522() throws Exception { runTest("522", "-useFC -seqRead -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z522" ); }
    @Test public void testAppend531() throws Exception { runTest("531", "-useFC -seqRead -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z531" ); }
    @Test public void testAppend532() throws Exception { runTest("532", "-useFC -seqRead -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z532" ); }
    @Test public void testAppend541() throws Exception { runTest("541", "-useFC -seqRead -rw -n 9 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z541" ); }
    @Test public void testAppend542() throws Exception { runTest("542", "-useFC -seqRead -rw -n 100 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z542" ); }
    @Test public void testAppend551() throws Exception { runTest("551", "-useFC -readFully -seqRead -rw -n 9 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z551" ); }
    @Test public void testAppend552() throws Exception { runTest("552", "-useFC -readFully -seqRead -rw -n 100 -s 100  -p 100  -f " + APPEND_OUTDIR + "/Z552" ); }
    @Test public void testAppend561() throws Exception { runTest("561", "-useFC -readFully -seqRead -rw -n 9 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z561" ); }
    @Test public void testAppend562() throws Exception { runTest("562", "-useFC -readFully -seqRead -rw -n 100 -s 1000 -p 100  -f " + APPEND_OUTDIR + "/Z562" ); }
    @Test public void testAppend571() throws Exception { runTest("571", "-useFC -readFully -seqRead -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z571" ); }
    @Test public void testAppend572() throws Exception { runTest("572", "-useFC -readFully -seqRead -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z572" ); }
    @Test public void testAppend581() throws Exception { runTest("581", "-useFC -readFully -seqRead -rw -n 9 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z581" ); }
    @Test public void testAppend582() throws Exception { runTest("582", "-useFC -readFully -seqRead -rw -n 100 -s 10000 -p 100  -f " + APPEND_OUTDIR + "/Z582" ); }
    @Test public void testAppend596() throws Exception { runTest("596", "-useFC -readFully -seqRead -rw -n 9 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z596" ); }
    @Test public void testAppend597() throws Exception { runTest("597", "-useFC -readFully -seqRead -rw -n 100 -s 1000000 -p 100  -f " + APPEND_OUTDIR + "/Z597" ); }
    @Test public void testAppend301() throws Exception { runTest("301", "-useFC -ro -oc -n 10 -p 1000 -readFully -f " + APPEND_OUTDIR + "/Z101" ); }
    @Test public void testAppend302() throws Exception { runTest("302", "-useFC -ro -oc -n 100 -p 1000 -readFully -f " + APPEND_OUTDIR + "/Z102" ); }
    
    public void runTest(String name, String args) throws IOException {
        this.testName = name;
        this.inputParams = args.split("\\s+");
        
        TestSession.logger.info("+++++++++++++++++++++++++++++++++++");
        TestSession.logger.info("Running " + this.testName + " called with: ");
        for (String temp : this.inputParams) {
            TestSession.logger.info(temp + " ");
        }
        TestSession.logger.info("+++++++++++++++++++++++++++++++++++");

        getCmdlineOptions(inputParams);
        setupFS();
        int stat = 0;
        // temporary hack to test any new options
        if (miscOption) {
            stat = testMisc();
        } else if (quickTestOption) {
            stat = testRWSanity();
        } else if (bothReadWriteOption) {
            stat = testReadWrite();
        } else if (readOnlyOption) {
            stat = testReadOnly();
        } else if (writeOnlyOption) {
            stat = testWriteOnly();
        } 

        if (stat == 0) {
            TestSession.logger.info("Tests done successfully, status = " + stat);
        } else {
            TestSession.logger.error("Tests failed, status = " + stat
                    + " ; Dump out Block Location");
            dumpFileBlockLocations(fnameOption);
            fail();
        }

    }
    
	// initialize the key context
	public void setupFS() throws IOException {
		mfc = FileContext.getFileContext();
		// mconfig = new Configuration();
		mconfig = new Configuration(true);
		mconfig.addResource(new Path(HadooptestConstants.Location.CORE_SITE_XML));
		mconfig.addResource(new Path(HadooptestConstants.Location.HDFS_SITE_XML));
		mconfig.addResource(new Path(HadooptestConstants.Location.YARN_SITE_XML));
		mconfig.addResource(new Path(
				HadooptestConstants.Location.MAPRED_SITE_XML));
		mconfig.addResource(HadooptestConstants.Location.CORE_SITE_XML);
		mconfig.addResource(HadooptestConstants.Location.HDFS_SITE_XML);
		mconfig.addResource(HadooptestConstants.Location.YARN_SITE_XML);
		mconfig.addResource(HadooptestConstants.Location.MAPRED_SITE_XML);
		// mconfig = TestSession.cluster.getConf();
		mfs = FileSystem.get(mconfig);
	}

	public void initDebugInfo() throws IOException {
		InetAddress iaddr = InetAddress.getLocalHost();
		String hostname = iaddr.getHostName();
		String ipString = iaddr.getHostAddress();
		if (verboseOption)
			TestSession.logger.info("Hostname =" + hostname + "; Host IP = "
					+ ipString);
	}

	void logWrite(long tafter, long tbefore, long byteVisible,
			long byteWritten, long loopCount, String msg, long duration) {
		long tdelta = tafter > tbefore ? tafter - tbefore : tbefore - tafter;
		TestSession.logger.info("TimeRW: tafter=" + tafter + ", tbefore=" + tbefore
				+ ", visible=" + byteVisible + ", written =" + byteWritten
				+ ", loopcount=" + loopCount + ", duration=" + duration
				+ ", delta |tafter-tbefore|=" + tdelta + " ; " + msg);
	}

	void logRead(long tafter, long tbefore, long byteRead, long byteVisible,
			long loopCount, String msg, long duration) {
		long tdelta = tafter > tbefore ? tafter - tbefore : tbefore - tafter;
		TestSession.logger.info("TimeRW: tafter=" + tafter + ", tbefore=" + tbefore
				+ ", visible=" + byteVisible + ",    read=" + byteRead
				+ ", loopcount=" + loopCount + ",  duration=" + duration
				+ ", delta |tafter-tbefore|=" + tdelta + "; " + msg);
	}

	/*************************************** thread **********************************/
	/**
	 * 
	 * helper runnable class to do the read: always open/close the file
	 */

	static class ReadThread implements Runnable {
		final int bufferChunkSize = DefaultBufferSize;

		String fname;
		TestAppend mta; // my TAppend class handle
		FileContext readfc;
		FileSystem readfs;
		long byteRead;
		long totalByteRead;

		byte[] buffer;

		ReadThread(String s, TestAppend ta) { // this is the file name to be
												// open and close during each
												// run
			fname = s;
			mta = ta;
			byteRead = 0;
			buffer = new byte[bufferChunkSize];
		}

		// always open and close the file here.
		public void run() {
			FSDataInputStream in = null;
			try {
				Path path = mta.getFullyQualifiedPath(fname);

				mta.tsBeforeOpen4Read = System.currentTimeMillis();
				in = mta.openInputStream(path);
				mta.tsAfterOpen4Read = System.currentTimeMillis();

				HdfsDataInputStream dfsin = (HdfsDataInputStream) in;
				mta.visibleLenFromReadStream = dfsin.getVisibleLength();

				long beginPosition;
				if (mta.readFullyOption || mta.visibleLenFromReadStream <= 1000) {
					beginPosition = 0;
				} else {
					beginPosition = mta.visibleLenFromReadStream - 1000;
				}
				TestSession.logger.info("Readonly test: read visible length="
						+ mta.visibleLenFromReadStream
						+ " ; read beginPosition =" + beginPosition);

				byteRead = mta.readUntilEnd(in, buffer, buffer.length,
						mta.fnameOption, beginPosition,
						mta.visibleLenFromReadStream, mta.readPassVEndOption,
						mta.posReadOption);
				mta.tsAfterRead = System.currentTimeMillis();
				mta.byteReadFromThread = byteRead + beginPosition;

			} catch (IOException e) {
				System.out
						.println("##### Caught Exception in ReadThread run(): Total byte read so far = "
								+ totalByteRead + "; file =" + fname);
				e.printStackTrace();
			} finally {
				try {
					if (in != null)
						in.close();
				} catch (IOException e) {
					System.out
							.println("##### Caught Exception in ReadThread run finally:  Total byte read so far = "
									+ totalByteRead + "; file =" + fname);
					e.printStackTrace();
				}
			}
			return;
		}
	}

	// this test always create a new thread, open and close the file. Uses only
	// very small buffer.
	public long readInAppendWriteInNewThread(String fname, long byteExpected)
			throws IOException {

		ReadThread rt = new ReadThread(fname, this); // todo: add a cmdline
														// switch to reuse the
														// thread?
		Thread readt = new Thread(rt);
		// TestSession.logger.info("Thread created to read: testReadInAppendWriteWithNewThread.");

		long totalByteRead = 0;
		readt.start();
		try {
			while (readt.isAlive()) {
				readt.join();
			}
		} catch (InterruptedException e) {
			System.out
					.println("##### Interrupted exception: testReadInAppendWriteWithNewThread. Return.");
		}
		totalByteRead = byteReadFromThread;

		return totalByteRead;
	}

	/******************** END Of THREAD ********************************************************************/

	/**
	 * Layer to abstract the common file operations to use either FileContext or
	 * FileSystem
	 */

	// length of a file (path name)
	public static long getFileLengthFromFc(Path path) throws IOException {
		FileStatus fileStatus;
		if (useFCOption) {
			fileStatus = mfc.getFileStatus(path);
		} else {
			fileStatus = mfs.getFileStatus(path);
		}
		long len = fileStatus.getLen();

		return len;
	}

	public long getVisibleFileLength(FSDataInputStream in) throws IOException {
		HdfsDataInputStream din = (HdfsDataInputStream) in;
		long len = din.getVisibleLength();
		return len;
	}

	// checksum of a file (pathname)
	public FileChecksum getFileChecksum(Path path) throws IOException {
		FileChecksum fileChecksum;
		if (useFCOption) {
			fileChecksum = mfc.getFileChecksum(path);
		} else {
			fileChecksum = mfs.getFileChecksum(path);
		}

		return fileChecksum;
	}

	// existence of a file
	public boolean ifExists(Path path) throws IOException {
		if (useFCOption) {
			return mfc.util().exists(path);
		} else {
			return mfs.exists(path);
		}
	}

	FSDataInputStream openInputStream(Path path) throws IOException {
		FSDataInputStream in;
		if (useFCOption) {
			if (verboseOption) {
				TestSession.logger.info("Using FileContext to open path for read: "
						+ path.toString());
			}
			in = mfc.open(path);
		} else {
			if (verboseOption) {
				TestSession.logger.info("Using FileSystem to open path for read: "
						+ path.toString());
			}
			in = mfs.open(path);
		}
		return in;
	}

	// HDFS does not have open for write. Need to use create() or append
	public FSDataOutputStream createAppendFile(Path path,
			boolean overwriteOption, boolean appendOption) throws IOException {
		if (useFCOption)
			return createAppendFileFC(path, overwriteOption, appendOption);
		else
			return createAppendFileFS(path, overwriteOption, appendOption);
	}

	public static Path getFullyQualifiedPath(String pathString) {
		Path path;
		if (useFCOption) {
			path = mfc.makeQualified(new Path(APPEND_ROOT_DIR, pathString));
		} else {
			path = mfs.makeQualified(new Path(APPEND_ROOT_DIR, pathString));
		}
		return path;
	}

	public static BlockLocation[] dumpFileBlockLocations(String fname)
			throws IOException {
		Path path = getFullyQualifiedPath(fname);
		long filelen = getFileLengthFromFc(path);
		return dumpFileBlockLocations(path, 0, filelen);
	}

	public static BlockLocation[] dumpFileBlockLocations(Path path, long start,
			long len) throws IOException {
		BlockLocation[] bl;
		if (useFCOption) {
			bl = mfc.getFileBlockLocations(path, start, len);
		} else {
			bl = mfs.getFileBlockLocations(path, start, len);
		}

		if (bl.length <= 0)
			return null;

		TestSession.logger.info("Total Number of blocks=" + bl.length
				+ ", block length=" + bl[0].getLength() + ", Host="
				+ bl[0].getHosts() + ", path=" + path);

		for (int i = 0; i < bl.length; i++) {
			long offset = bl[i].getOffset();

			String[] names = bl[i].getNames(); // String[] topoPaths =
												// bl[i].getTopologyPaths();
			for (String dn : names)
				TestSession.logger.info("Block index=" + i + ", DataNode=" + dn
						+ ", Offset=" + offset + ", path=" + path);
		}
		return bl;
	}

	public byte[] setFileData(int numOfBlocks, long blockSize) {
		byte[] data = new byte[(int) (numOfBlocks * blockSize)];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 10);
		}
		return data;
	}

	/*
	 * Create files and populate with data of size numBlocks * blocksize
	 */
	public void createFileWithData(FileContext fc, Path path, int numBlocks,
			CreateOpts... options) throws IOException {

		FSDataOutputStream out = fc.create(path, EnumSet.of(CreateFlag.CREATE),
				options);
		byte[] data = setFileData(numBlocks, blockSize);
		out.write(data, 0, data.length);
		out.close();
	}

	public void createFileWithData(FileContext fc, String name, int numBlocks,
			int blockSize) throws IOException {
		Path path = getFullyQualifiedPath(name);
		TestSession.logger.info("  DEBUG: createFileWithData to create file: "
				+ name + "; path = " + path.toString());
		createFileWithData(fc, path, numBlocks,
				CreateOpts.blockSize(blockSize), CreateOpts.createParent());
	}

	/*
	 * Other helper functions
	 */
	// pause current Thread for
	public int pauseCurrentThread(int millisec) throws IOException {
		try {
			Thread.currentThread().sleep(millisec);
		} catch (InterruptedException e) {
			System.out
					.println("##### Caught Interrupted Exception in pauseCurrentThread: ");
			e.printStackTrace();
		}
		return 0;
	}

	// quick way to dump memory when the content are printable
	void dumpMem(byte[] data, long n) {
		String s = new String(data);
		TestSession.logger.info("Dump: \n" + s);
	}

	/*
	 * Sanity check just create and read 2K worth of data
	 */
	public void testCreate2KB(String fname) throws IOException {
	    System.out.println("Testing the creation of file " + fname);

	    Path path = getFullyQualifiedPath(fname);
	    createFileWithData(mfc, fname, numBlocks, blockSize);
	    System.out.println("Checksum of the file: " + fname + " = "
	            + mfc.getFileChecksum(path));
	}

	/*
	 * Test reading of file: open and close. Return the number of bytes read.
	 */
	public void testRead2KB(String fname) throws IOException {
	    System.out.println("Testing the reading of file " + fname);

	    Path path = getFullyQualifiedPath(fname);
	    FSDataInputStream in = mfc.open(path);
	    byte[] buffer = new byte[DefaultBufferSize];

	    int byteRead = in.read(0, buffer, 0, DefaultBufferSize);
	    System.out.println("Number of byte read from file: " + path.toString()
	            + "; Byte = " + byteRead);
	    in.close();
	}

	// do not read pass Visible End
	long readUntilVisibleEnd(FSDataInputStream in, byte[] buffer, long size,
			String fname, long pos, long visibleLen, boolean posRead)
			throws IOException {
		int chunkNumber = 0;
		long totalByteRead = 0;
		long currentPosition = pos;
		int byteRead = 0;
		long byteLeftToRead = visibleLen - currentPosition;
		int byteToReadThisRound = 0;
		long currentFpos;
		String typeOfRead;
		String readParam = "";

		if (!posRead) {
			in.seek(pos);
			typeOfRead = ", Seq Read ";
		} else {
			typeOfRead = ", Pos Read ";
		}
		currentFpos = in.getPos();

		if (verboseOption)
			TestSession.logger.info("Reader Until Visible End - begin: position: "
					+ pos + " ; currentOffset = " + currentPosition
					+ " ; bufferSize =" + buffer.length + " ; VisibleLen ="
					+ visibleLen + " ; current file Position =" + currentFpos
					+ typeOfRead + " ; Filename = " + fname);

		if (pos >= visibleLen)
			return 0;

		try {
			while (byteLeftToRead > 0 && currentPosition < visibleLen) {
				byteToReadThisRound = (int) (byteLeftToRead >= buffer.length ? buffer.length
						: byteLeftToRead);
				readParam = ", ReadParam - CurrentPostion=" + currentPosition
						+ ", offset=0, size=" + byteToReadThisRound;

				if (posRead) {
					byteRead = in.read(currentPosition, buffer, 0,
							byteToReadThisRound);
				} else {
					currentFpos = in.getPos();
					byteRead = in.read(buffer, 0, byteToReadThisRound);
				}

				if (byteRead <= 0)
					break;
				chunkNumber++;
				totalByteRead += byteRead;
				currentPosition += byteRead;
				byteLeftToRead = visibleLen - currentPosition;

				if (verboseOption) {
					TestSession.logger.info("Reader: Number of byte read="
							+ byteRead + "; toatlByteRead=" + totalByteRead
							+ "; currentPosition=" + currentPosition
							+ "; chunkNumber=" + chunkNumber
							+ "; byteLeftToRead=" + byteLeftToRead
							+ "; File Pos before read=" + currentFpos
							+ "; File name=" + fname);

					if (dumpmemFlag)
						dumpMem(buffer, totalByteRead);
				}
			}
		} catch (IOException e) {
			throw new IOException(
					"Exception caught in readUntilVisibleEnd (pass visibleEnd): Reader  currentOffset = "
							+ currentPosition
							+ " ; totalByteRead ="
							+ totalByteRead
							+ " ; latest byteRead = "
							+ byteRead
							+ " ; visibleLen= "
							+ visibleLen
							+ " ; byteLeftToRead = "
							+ byteLeftToRead
							+ " ; bufferLen = "
							+ buffer.length
							+ " ; chunkNumber= "
							+ chunkNumber
							+ " ; input pos = "
							+ pos
							+ " ; byteToReadThisRound = "
							+ byteToReadThisRound
							+ " ; File pos before read = "
							+ currentFpos
							+ " ; Filename = " + fname + readParam, e);

		}
		if (verboseOption)
			TestSession.logger.info("Reader end:   position=" + pos
					+ "; currentOffset=" + currentPosition
					+ "; totalByteRead =" + totalByteRead
					+ " ; UntilVisibleEnd of Filename = " + fname);

		return totalByteRead;
	}

	// read into buffer repeatedly until the end of the file, return total
	// number of bytes read
	// if size > buffer size, will read buffer.size one at at time (thus later
	// data will overwrite the earlier data in buffer)

	long readUntilEnd(FSDataInputStream in, byte[] buffer, long size,
			String fname, long pos, long visibleLen, boolean readPassVEnd,
			boolean posRead) throws IOException {

		if (!readPassVEnd) {
			return readUntilVisibleEnd(in, buffer, size, fname, pos,
					visibleLen, posRead);
		}

		int chunkNumber = 0;
		long totalByteRead = 0;
		long currentPosition = pos;
		int byteRead = 0;
		long byteLeftToRead = visibleLen - currentPosition;
		int byteToReadThisRound = buffer.length;
		long currentFpos;
		String readParam = "";
		String typeOfRead;

		if (!posRead) {
			in.seek(pos);
			typeOfRead = ", Seq Read ";
		} else {
			typeOfRead = ", Pos Read ";
		}
		currentFpos = in.getPos();

		if (verboseOption)
			TestSession.logger.info("Reader Until End - begin: position: " + pos
					+ " ; currentOffset = " + currentPosition
					+ " ; bufferSize =" + buffer.length + " ; VisibleLen ="
					+ visibleLen + " ; current file Position =" + currentFpos
					+ typeOfRead + " ; Filename = " + fname);
		try {
			while (true) {

				readParam = ", ReadParam - CurrentPostion=" + currentPosition
						+ ", offset=0, size=" + byteToReadThisRound;
				if (posRead) {
					byteRead = in.read(currentPosition, buffer, 0,
							byteToReadThisRound);
				} else {
					currentFpos = in.getPos();
					byteRead = in.read(buffer, 0, byteToReadThisRound);
				}

				if (byteRead <= 0)
					break;
				chunkNumber++;
				totalByteRead += byteRead;
				currentPosition += byteRead;
				byteLeftToRead = visibleLen - currentPosition;

				if (verboseOption) {
					TestSession.logger.info("Reader: Number of byte read="
							+ byteRead + "; toatlByteRead=" + totalByteRead
							+ "; currentPosition=" + currentPosition
							+ "; chunkNumber=" + chunkNumber
							+ "; byteLeftToRead=" + byteLeftToRead
							+ "; File pos before read=" + currentFpos
							+ "; File name=" + fname);
					if (dumpmemFlag)
						dumpMem(buffer, totalByteRead);
				}
			}
		} catch (IOException e) {
			throw new IOException(
					"Exception caught in readUntilEnd (pass visibleEnd): Reader  currentOffset = "
							+ currentPosition + " ; totalByteRead ="
							+ totalByteRead + " ; latest byteRead = "
							+ byteRead + " ; visibleLen= " + visibleLen
							+ " ; byteLeftToRead = " + byteLeftToRead
							+ " ; bufferLen = " + buffer.length
							+ " ; chunkNumber= " + chunkNumber
							+ " ; input pos = " + pos
							+ " ; byteToReadThisRound = " + byteToReadThisRound
							+ " ; File pos before read = " + currentFpos
							+ " ; Filename = " + fname + readParam, e);

		}

		if (verboseOption)
			TestSession.logger.info("Reader end:   position=" + pos
					+ "; currentOffset=" + currentPosition + "; totalByteRead="
					+ totalByteRead + "; UntilEnd of Filename = " + fname);

		return totalByteRead;
	}

	// this test always open and close the file.
	public long readInAppendWrite(String fname, long byteExpected)
			throws IOException {
		long totalByteRead = 0;

		Path path = getFullyQualifiedPath(fname);

		// read in a different thread. Do it in a different routine.
		if (newReadTheadOption) {
			totalByteRead = readInAppendWriteInNewThread(fname, byteExpected);
			return totalByteRead;
		}

		// read in the same thread as write
		FSDataInputStream in = null;
		try {
			tsBeforeOpen4Read = System.currentTimeMillis();
			in = openInputStream(path);
			tsAfterOpen4Read = System.currentTimeMillis();

			byte[] buffer = new byte[DefaultBufferSize];

			visibleLenFromReadStream = getVisibleFileLength(in);
			long beginPosition;
			if (readFullyOption || visibleLenFromReadStream <= 10000) {
				beginPosition = 0;
			} else {
				beginPosition = visibleLenFromReadStream - 10000;
			}
			totalByteRead = readUntilEnd(in, buffer, buffer.length, fname,
					beginPosition, visibleLenFromReadStream,
					readPassVEndOption, posReadOption);
			in.close();

			long totalByteVisible = totalByteRead + beginPosition;
			return totalByteVisible;

		} catch (IOException e) {
			System.out
					.println("##### Caught Exception in readInAppendWrite. Total Byte Read so far = "
							+ totalByteRead);
			e.printStackTrace();
			if (in != null) {
				in.close();
			}
			throw new IOException(); // let the caller knows that exception
										// happens
		}
	}

	/*
	 * Write out length bytes: first 8 byte is the hex representation of the int
	 * marker Length: >= 9, but less than 1024 TODO: if length is large, break
	 * into smaller chunk to write
	 */
	public long appendChunk(FSDataOutputStream out, int marker, long length,
			boolean flushOrNot) throws IOException {
		// TestSession.logger.info("WRITER: marker=" + Integer.toHexString(marker) +
		// "; length = " + length);

		if (length < 10) // minimum to write is at least 10 byte
			return 0;

		int bufferSize = 100 * DEFAULT_BLOCK_SIZE;
		byte[] buffer = new byte[bufferSize];

		// 10011001
		// 0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz0123456789:;<=>?@ABCDEFGHIJ
		// 10011002
		// 123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz0123456789:;<=>?@ABCDEFGHIJK
		int bitmask = 0x0000000F;
		for (int j = 0; j < 8; j++) {
			int nibble = ((marker >>> (28 - 4 * j)) & bitmask);
			buffer[j] = (byte) (nibble >= 10 ? nibble - 10 + 'A' : nibble + '0');
			// TestSession.logger.info("Nibble is " + Integer.toHexString(nibble) +
			// "; bitmask = " +
			// Integer.toHexString(bitmask) + "; buffer[j] = " +
			// Integer.toHexString(buffer[j]));
		}
		buffer[8] = ' ';

		// now write the rest
		try {
			int totalByteWritten = 0;
			int bufferIndex = 9;

			for (int k = 9; k < (length - 1); k++) {
				buffer[bufferIndex++] = (byte) ('0' + (k - 9 + sand) % 75);
				if (bufferIndex >= bufferSize) {
					out.write(buffer, 0, bufferSize);
					totalByteWritten += bufferSize;
					bufferIndex = 0;
				}
			}

			buffer[bufferIndex++] = '\n';
			totalByteWritten += bufferIndex;

			out.write(buffer, 0, bufferIndex);
			tsAfterWrite = System.currentTimeMillis(); // if no flush, use

			if (flushOrNot) {
				tsBeforeHflush = System.currentTimeMillis();
				out.hflush();
				tsAfterHflush = System.currentTimeMillis();
			}
			if (verboseOption)
				TestSession.logger.info("AppendChunk: Total byte written="
						+ totalByteWritten + "; input chunk size = " + length);

			sand++; // next chunk will offset the data pattern by 1
			return totalByteWritten;
		} catch (IOException e) {
			throw new IOException("##### IO Exception in appending file. "
					+ out.toString(), e);
		}
	}

	/*
	 * Test Append with read and write: writer ignores the openAndClose option
	 * here Return 0 upon success, else count of failures in negative number
	 */
	public int testAppendWriteAndRead(String fname) throws IOException {
		int countOfFailures = 0;
		long byteVisibleToRead = 0;
		boolean readAfterWrite = true;
		FSDataOutputStream out = null;

		try {
			Path path = getFullyQualifiedPath(fname);
			TestSession.logger.info("Testing testAppendWriteAndRead " + fname
					+ "; using Filecontext: " + useFCOption
					+ "; fully qualified path = " + path);

			long existingFileLength = 0;
			if (ifExists(path) && appendOption && !overwriteOption)
				existingFileLength = getFileLengthFromFc(path);

			// FSDataOutputStream out = mfc.create(path,
			// EnumSet.of(CreateFlag.APPEND, CreateFlag.CREATE));
			out = createAppendFile(path, overwriteOption, appendOption);

			long chunkSize = chunkSizeOption;
			int lcounter = counter;
			long totalByteWritten = existingFileLength;
			long totalByteVisible = existingFileLength;
			long totalByteWrittenButNotVisible = 0;

			if (blockLocOption) {
				dumpFileBlockLocations(path, 0, existingFileLength);
			}

			boolean toFlush;
			for (int i = 0; i < ntimesOption; i++) {
				toFlush = isOKToHflush(i);

				appendChunk(out, lcounter++, chunkSize, toFlush);

				totalByteWritten += chunkSize;

				if (toFlush) {
					totalByteVisible += chunkSize
							+ totalByteWrittenButNotVisible;
					totalByteWrittenButNotVisible = 0;
					logWrite(tsAfterHflush, tsBeforeHflush, totalByteVisible,
							totalByteWritten, i, "Flush " + fnameOption,
							tsAfterHflush - tsBeforeWrite);
				} else {
					totalByteWrittenButNotVisible += chunkSize;
					logWrite(tsAfterHflush, tsAfterWrite, totalByteVisible,
							totalByteWritten, i, "NoFlush " + fnameOption,
							tsAfterWrite - tsBeforeWrite);
				}

				if (readAfterWrite) {
					byteVisibleToRead = readInAppendWrite(fname,
							totalByteVisible); // testReadInAppendWrite always
												// open and close file
					String readmsg;
					if (byteVisibleToRead >= totalByteVisible
							&& byteVisibleToRead <= totalByteWritten) {
						readmsg = "Pass: reader sees expected size of file "
								+ fnameOption + "  [pass]"; // : " +
															// byteVisibleToRead);
					} else {
						countOfFailures++;
						readmsg = "Fail: reader sees is different size of file "
								+ fnameOption + " [fail]"; // : " +
															// byteVisibleToRead);
					}

					logRead(tsAfterOpen4Read, tsBeforeOpen4Read,
							byteVisibleToRead, visibleLenFromReadStream, i,
							readmsg, tsAfterRead - tsAfterOpen4Read);
				}
				if (pauseIntervalMsecOption > 0)
					pauseCurrentThread(pauseIntervalMsecOption);
			}

			// test the automatic flush after close
			appendChunk(out, lcounter++, chunkSize, false);
			totalByteWritten += chunkSize;
			totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
			totalByteWrittenButNotVisible += 0;

			out.close();
			long now = System.currentTimeMillis();
			logWrite(tsAfterHflush, tsBeforeHflush, totalByteVisible,
					totalByteWritten, 0, "Close Flush " + fnameOption, now
							- tsBeforeWrite);

			byteVisibleToRead = readInAppendWrite(fname, totalByteVisible);

			String readmsg;
			if (byteVisibleToRead == totalByteVisible) {
				readmsg = "Pass: reader sees expected size of file "
						+ fnameOption + " after close"; // : " +
														// byteVisibleToRead);
			} else {
				countOfFailures++;
				readmsg = "Fail: reader sees is different size of file "
						+ fnameOption + " after close"; // : " +
														// byteVisibleToRead);
			}
			long lenFromFc = getFileLengthFromFc(path);
			logRead(tsAfterOpen4Read, tsBeforeOpen4Read, byteVisibleToRead,
					lenFromFc, 0, readmsg, tsAfterRead - tsAfterOpen4Read);

		} catch (IOException e) {
			countOfFailures++;
			if (out != null)
				out.close();
			throw new IOException(
					"##### Caught Exception in testAppendWriteAndRead. Close file. Total Byte Read so far = "
							+ byteVisibleToRead, e);
		}
		return -countOfFailures;
	}

	// Use FileContext"
	// CREATE: can work with CREATE|APPEND, and CREATE|OVERWRITE (TRUNC)
	public FSDataOutputStream createAppendFileFC(Path path,
			boolean overwriteOption, boolean appendOption) {
		FSDataOutputStream out = null;
		

		// if exist, do OVERWRITE or APPEND based on -overwrite flag), else
		// create it with CREATE|APPEND
		try {
			if (ifExists(path)) {
				if (overwriteOption) {
					System.out
							.println("File already exists. Truncate file. FileContext Open OVERWRITE: "
									+ path);
					out = mfc.create(path, EnumSet.of(CreateFlag.OVERWRITE));
				} else if (appendOption) {
					System.out
							.println("File already exists. FileContext Open with APPEND only: "
									+ path);
					out = mfc.create(path, EnumSet.of(CreateFlag.APPEND));
				} else {
					System.out
							.println("File exist, but neither overwrite nor append is specified. Aborted."
									+ path);
					throw new IllegalArgumentException();
				}
			} else {
				// TestSession.logger.info("File does not exists. Open with CREATE + APPEND: "
				// + path);
				// out = mfc.create(path, EnumSet.of( CreateFlag.APPEND,
				// CreateFlag.CREATE));
				System.out
						.println("File does not exists. FileContext Open with CREATE: "
								+ path);
				out = mfc.create(path, EnumSet.of(CreateFlag.CREATE));
				TestSession.logger.info("Successfully created file, via mfc: "
						+ path);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return out;
	}

	// the new semantics:
	// CREATE: can work with CREATE|APPEND, and CREATE|OVERWRITE (TRUNC)
	public FSDataOutputStream createAppendFileFS(Path path,
			boolean overwriteOption, boolean appendOption) throws IOException {
		FSDataOutputStream out;

		// if exist, do OVERWRITE or APPEND based on -overwrite flag), else
		// create it with CREATE|APPEND
		if (ifExists(path)) {
			if (overwriteOption) {
				System.out
						.println("File already exists. Truncate file. FileSystem Open OVERWRITE: "
								+ path);
				out = mfs.create(path, overwriteOption);
			} else if (appendOption) {
				System.out
						.println("File already exists. FileSystem Open with APPEND  (mfs.append) only: "
								+ path);
				out = mfs.append(path);
			} else {
				System.out
						.println("File exist, but neither overwrite nor append is specified. Aborted."
								+ path);
				throw new IllegalArgumentException();
			}
		} else {
			// TestSession.logger.info("File does not exists. Open with CREATE + APPEND: "
			// + path);
			// out = mfc.create(path, EnumSet.of( CreateFlag.APPEND,
			// CreateFlag.CREATE));
			System.out
					.println("File does not exists. FileSystem Open with CREATE (mfs.createNewFile): "
							+ path);
			out = mfs.create(path, overwriteOption);
		}
		return out;
	}

	public void usageError() {
		TestSession.logger.info(cmdUsage);
		System.exit(100);

	}

	public void dumpOptions() {
		if (verboseOption) {
			TestSession.logger.info("INFO: fnameOption " + fnameOption);
			TestSession.logger.info("INFO: quickTestOption " + quickTestOption);
			TestSession.logger.info("INFO: readOnlyOption " + readOnlyOption);
			TestSession.logger.info("INFO: bothReadWriteOption "
					+ bothReadWriteOption);
			TestSession.logger.info("INFO: toPauseOption " + toPauseOption);
			TestSession.logger.info("INFO: useFCOption " + useFCOption);
			TestSession.logger.info("INFO: useFSOption " + useFSOption);
			TestSession.logger.info("INFO: pauseIntervalMsecOption "
					+ pauseIntervalMsecOption);
			TestSession.logger.info("INFO: ntimesOption " + ntimesOption);
			TestSession.logger.info("INFO: chunkSizeOption " + chunkSizeOption);
			System.out
					.println("INFO: openAndCloseOption " + openAndCloseOption);
			TestSession.logger.info("INFO: overwriteOption " + overwriteOption);
			TestSession.logger.info("INFO: appendOption " + appendOption);
			TestSession.logger.info("INFO: createOption " + createOption);
			TestSession.logger.info("INFO: verboseOption " + verboseOption);
			TestSession.logger.info("INFO: readFullyOption " + readFullyOption);
			System.out
					.println("INFO: alwaysHflushOption " + alwaysHflushOption);
			TestSession.logger.info("INFO: noHflushOption " + noHflushOption);
			System.out
					.println("INFO: readPassVEndOption " + readPassVEndOption);
			TestSession.logger.info("INFO: miscOption " + miscOption);
			TestSession.logger.info("INFO: miscCmdOption " + miscCmdOption);
			TestSession.logger.info("INFO: posReadOption " + posReadOption);
			TestSession.logger.info("INFO: blockLocOption " + blockLocOption);
		}
	}

	public void getCmdlineOptions(String[] args) {

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-qt")) {
				quickTestOption = true;
			} else if (args[i].equals("-ro")) { // options -ro, -rw, -wo are
												// mutually exclusive.
				readOnlyOption = true;
				writeOnlyOption = false;
				bothReadWriteOption = false;
			} else if (args[i].equals("-wo")) {
				writeOnlyOption = true;
				readOnlyOption = false;
				bothReadWriteOption = false;
			} else if (args[i].equals("-rw")) {
				bothReadWriteOption = true;
				readOnlyOption = false;
				writeOnlyOption = false;
			} else if (args[i].equals("-nrt")) {
				newReadTheadOption = true;
			} else if (args[i].equals("-oc")) {
				openAndCloseOption = true;
			} else if (args[i].equals("-overwrite")) {
				overwriteOption = true;
			} else if (args[i].equals("-append")) {
				appendOption = true;
			} else if (args[i].equals("-create")) {
				createOption = true;
			} else if (args[i].equals("-verbose")) {
				verboseOption = true;
			} else if (args[i].equals("-readFully")) {
				readFullyOption = true;
			} else if (args[i].equals("-useFS")) { // -useFS and -useFC are
													// mutually exclusive
				useFSOption = true;
				useFCOption = false;
			} else if (args[i].equals("-alwaysHflush")) { // -always Hflush and
															// no Hflush are
															// mutualluy
															// exclusive
				noHflushOption = false;
				alwaysHflushOption = true;
			} else if (args[i].equals("-noHflush")) {
				noHflushOption = true;
				alwaysHflushOption = false;
			} else if (args[i].equals("-readPassVEnd")) {
				readPassVEndOption = true;
			} else if (args[i].equals("-useFC")) {
				useFCOption = true;
				useFSOption = false;
			} else if (args[i].equals("-posRead")) {
				posReadOption = true;
			} else if (args[i].equals("-seqRead")) {
				posReadOption = false;
			} else if (args[i].equals("-blockLoc")) {
				blockLocOption = true;
			} else if (args[i].equals("-misc")) {
				miscOption = true;
				miscCmdOption = args[++i];
			} else if (args[i].equals("-p")) {
				toPauseOption = true;
				pauseIntervalMsecOption = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-s")) {
				chunkSizeOption = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-f")) {
				fnameOption = args[++i];
			} else if (args[i].equals("-n")) {
				ntimesOption = Integer.parseInt(args[++i]);
			} else {
				usageError();
			}
		}
		// only option must be set is fname. Default is to read.
		if (fnameOption == null)
			usageError();
		// allow verbose option to be turned on through env variable
		if (System.getenv("QE_APPEND_VERBOSE") != null)
			verboseOption = true;

		dumpOptions();

	}

	/**
	 * Test functions:
	 * 
	 * @param ta
	 *            : TAppend class object
	 */
	public int testReadWrite() {
		int errorStat = 0;

		TestSession.logger.info("testReadWrite " + fnameOption);

		try {
			// setupFS();
			errorStat = testAppendWriteAndRead(fnameOption); // test both read
																// and write in
																// a loop
		} catch (Exception e) {
			errorStat = -1;
			System.err.println("##### Caught  Exception in testReadWrite ");
			e.printStackTrace();
		}
		return errorStat;
	}

	// read is to discover the full length of a file.
	// To read every byte of a file could be slow. One way is to use binary
	// search to probe the end of a file
	// The other way is to get to the end of file from DFSDataInputStream Thus
	// the strategy is to:

	public int testReadOnly() {
		int errorStat = 0;
		String fname = fnameOption;
		int totalByteRead = 0;
		byte[] buffer = new byte[DefaultBufferSize];

		TestSession.logger.info("testReadOnly to file " + fnameOption + " ; for "
						+ ntimesOption + " times with pause "
						+ pauseIntervalMsecOption);

		try {

			long knownVisibleLength = -1;
			FSDataInputStream in = null;

			long beginPosition = 0;

			long byteRead = 0;
			long totalByteSeenHere = 0;

			Path path = getFullyQualifiedPath(fname);
			TestSession.logger.info("Testing testReadOnly " + fname
					+ "; fully qualified path = " + path);
			long lenFromFc = getFileLengthFromFc(path);

			if (blockLocOption) {
				BlockLocation[] bl = dumpFileBlockLocations(path,
						beginPosition, lenFromFc);
			}

			for (int i = 0; i < ntimesOption; i++) {
				if (in == null || openAndCloseOption) {

					tsBeforeOpen4Read = System.currentTimeMillis();
					in = mfc.open(path);
					tsAfterOpen4Read = System.currentTimeMillis();

					HdfsDataInputStream dfsin = (HdfsDataInputStream) in;

					knownVisibleLength = dfsin.getVisibleLength();

					if (readFullyOption) {
						beginPosition = 0;
					} else {
						beginPosition = knownVisibleLength < 1000 ? 0
								: knownVisibleLength - 1000;
					}

					totalByteSeenHere = beginPosition;
					TestSession.logger.info("Readonly test: read visible length="
							+ knownVisibleLength + " ; read beginPosition ="
							+ beginPosition + "; iteration = " + i);
				}

				// does not re-seek.
				byteRead = readUntilEnd(in, buffer, buffer.length, fnameOption,
						beginPosition, knownVisibleLength, readPassVEndOption,
						posReadOption);
				tsAfterRead = System.currentTimeMillis();
				long duration = tsAfterRead - tsAfterOpen4Read;

				totalByteSeenHere += byteRead;
				beginPosition += byteRead;
				lenFromFc = getFileLengthFromFc(path);
				String readmsg = " test readonly " + fnameOption
						+ "; current lenFromFc =" + Long.toString(lenFromFc);
				logRead(tsAfterOpen4Read, tsBeforeOpen4Read, totalByteSeenHere,
						knownVisibleLength, i, readmsg, duration);

				if (openAndCloseOption) {
					in.close();
				}

				if (pauseIntervalMsecOption > 0)
					pauseCurrentThread(pauseIntervalMsecOption);
			}

		} catch (Exception e) {
			errorStat = -2;
			System.out
					.println("##### Caught Exception in testReadOnly while reading.");
			e.printStackTrace();
			return errorStat;
		}
		TestSession.logger.info("READER: testReadOnly total byte read: "
				+ totalByteRead + "; after iterations = " + ntimesOption);

		return errorStat;
	}

	public boolean isOKToHflush(int counter) {
		if (alwaysHflushOption)
			return true;
		if (noHflushOption)
			return false;
		// otherwise, even: do hflush; odd: don;t do it
		return (counter % 2 == 0) ? true : false;

	}

	// open file in OVERWRITE|APPEND mode, then in a loop to do N time: write a
	// chunk, pause
	// ignore openAndCloseOption - wirte keeps file open during write
	public int testWriteOnly() {
		int stat = 0;
		String fname = fnameOption;
		FSDataOutputStream out = null;

		TestSession.logger.info("testWriteOnly to file " + fnameOption + " ; for "
				+ numBlocks + " times with pause " + pauseIntervalMsecOption);

		try {

			Path path = getFullyQualifiedPath(fname);
			TestSession.logger.info("Testing testWriteOnly " + fname
					+ "; fully qualified path = " + path);

			// FSDataOutputStream out = mfc.create(path,
			// EnumSet.of(CreateFlag.APPEND, CreateFlag.CREATE));
			long chunkSize = chunkSizeOption;
			int lcounter = counter;

			out = createAppendFile(path, overwriteOption, appendOption);

			long originalFileVisibleLength = getFileLengthFromFc(path);

			long fpos = out.getPos(); // data may already exist, if open in
										// append mode; but getPos() does not
										// work for append mode

			TestSession.logger.info("File original length ="
					+ originalFileVisibleLength + " ; getPos returned: " + fpos
					+ "; overWriteOption = " + overwriteOption
					+ "; appendOption =" + appendOption);

			if (blockLocOption) {
				BlockLocation[] bl = dumpFileBlockLocations(path,
						fpos > 1000 ? fpos - 1000 : 0, chunkSize);
			}

			if (overwriteOption) {
				originalFileVisibleLength = 0; // overwrite ==> truncate
			}
			long totalByteWritten = originalFileVisibleLength;
			long totalByteVisible = originalFileVisibleLength;
			int totalByteWrittenButNotVisible = 0;

			boolean toFlush;

			// loop n time, pause m msec each time
			for (int i = 0; i < ntimesOption; i++) {

				// toggle the flush flag. First time do flush
				toFlush = isOKToHflush(i);

				appendChunk(out, lcounter++, chunkSize, toFlush);
				totalByteWritten += chunkSize;

				if (toFlush) {
					totalByteVisible += chunkSize
							+ totalByteWrittenButNotVisible;
					totalByteWrittenButNotVisible = 0;
					logWrite(tsAfterHflush, tsBeforeHflush, totalByteVisible,
							totalByteWritten, i, fnameOption, tsAfterHflush
									- tsBeforeWrite);
				} else {
					totalByteWrittenButNotVisible += chunkSize;
					logWrite(tsAfterHflush, tsAfterWrite, totalByteVisible,
							totalByteWritten, i, fnameOption, tsAfterWrite
									- tsBeforeWrite);
				}

				if (pauseIntervalMsecOption > 0)
					pauseCurrentThread(pauseIntervalMsecOption);
			}
			out.close();

		} catch (Exception e) {
			stat = -1;
			System.out
					.println("##### ERROR: testWriteOnly: Caught Exception in writing to file: ");
			e.printStackTrace();
		} finally {
			if (out != null)
				try {
					out.close();
				} catch (IOException e) {
					System.out
							.println("##### Exception encountered during closing of file. Ignored.");
				}
		}
		return (stat);
	}

	// quick test: just write and read to verify permission and cluster health.
	public int testRWSanity() {

		int stat = 0;
		TestSession.logger.info("testRWSanity " + fnameOption);

		try {
			// test the creation of file, and write 2K worth of data, close,
			// then read it to verify the size
			//testCreate2KB(fnameOption);
			testRead2KB(fnameOption);
		} catch (IOException e) {
			stat = -1;
			TestSession.logger.error("##### Caught Exception in testRWSanity");
			e.printStackTrace();
		}
		return stat;
	}

	public void testReadBeyondEnd() throws IOException {

		TestSession.logger.info("Test to test the behavior when read from beyond the file length. File name = "
						+ fnameOption);

		Path path = getFullyQualifiedPath(fnameOption);
		FSDataInputStream in = openInputStream(path);
		long fileLen = getFileLengthFromFc(path);
		byte[] buffer = new byte[1000000];

		// read from begin to end end of file
		long beginPosition = 0;
		long byteRead = readUntilEnd(in, buffer, buffer.length, fnameOption,
				beginPosition, -1, true, true); // position read
		TestSession.logger.info("Read from begin to end of file: number of byte read in =  "
						+ byteRead);

		// read 100 byte from end of file
		beginPosition = fileLen - 100;
		byteRead = readUntilEnd(in, buffer, buffer.length, fnameOption,
				beginPosition, -1, true, true);
		TestSession.logger.info("Read file 100 byte from end: number of byte read in =  "
						+ byteRead);

		// read from exactly the end of the file
		byteRead = readUntilEnd(in, buffer, buffer.length, fnameOption,
				fileLen, -1, true, true);
		TestSession.logger.info("Read file from. End number of byte read in =  "
				+ byteRead);

		// read 200 byte beyond the end of the file
		byteRead = readUntilEnd(in, buffer, buffer.length, fnameOption,
				fileLen + 200, -1, true, true);
		TestSession.logger.info("Read file from End. number of byte read in =  "
				+ byteRead);
	}

	public int testBlockLocation(String fname) throws IOException {
	    Path path = getFullyQualifiedPath(fname);
	    long filelen = getFileLengthFromFc(path);

	    BlockLocation[] bl = dumpFileBlockLocations(path, 0, filelen);

	    return 0;
	}

	public int testSeekSeqRead() throws IOException {
	    setupFS();
	    long fpos;
	    Path path = getFullyQualifiedPath(fnameOption);
	    long filelen = getFileLengthFromFc(path);
	    FSDataInputStream in = openInputStream(path);
	    byte[] buffer = new byte[DefaultBufferSize];
	    int byteRead;

	    try {
	        System.out.println("Seek file to 10L");
	        in.seek(10L);
	        fpos = in.getPos();
	        System.out.println("getPos() returns: " + fpos);

	        // sequential read does move the file pointer
	        byteRead = in.read(buffer, 0, 100);
	        fpos = in.getPos();
	        System.out.println("After read of " + byteRead
	                + " byte, getPos() returns: " + fpos);
	        byteRead = in.read(buffer, 0, 200);
	        fpos = in.getPos();
	        System.out.println("After read of " + byteRead
	                + " byte, getPos() returns: " + fpos);

	        System.out
	        .println("Now Seek file to 1000 from the end where total length of file = "
	                + filelen);
	        in.seek(filelen - 1000);
	        fpos = in.getPos();
	        System.out.println("getPos() returns: " + fpos);

	        System.out.println("Now Seek file to exactly end of file: "
	                + filelen);
	        in.seek(filelen);
	        fpos = in.getPos();
	        System.out.println("getPos() returns: " + fpos);

	        System.out
	        .println("Now Seek file to 100000 beyond end of file. Expect Exception.");
	        in.seek(filelen + 100000);
	        System.out.println("OK in seeking beyond the end of file");
	        fpos = in.getPos();
	        System.out.println("End: getPos() returns: " + fpos);
	    } catch (IOException e) {
	        fpos = in.getPos();
	        System.out
	        .println("##### Caught Expected Exception in testSeek: getPos ="
	                + fpos);
	        return 0;
	    }

	    return -1;
	}
	
	public int testMisc() throws IOException {

	    try {
	        if (miscCmdOption.equals("ReadBeyondEnd")) {
	            testReadBeyondEnd();
	        } else if (miscCmdOption.equals("SeqRead")) {
	            testSeekSeqRead();
	        } else if (miscCmdOption.equals("BlockLocation")) {
	            testBlockLocation(fnameOption);
	        } else if (miscCmdOption.equals("All")) {
	            testReadBeyondEnd();
	            testSeekSeqRead();
	        }
	    } catch (IOException e) {
	        throw new IOException("Caught Exception in Misc Test", e);
	    }
	    return 0;
	}

}

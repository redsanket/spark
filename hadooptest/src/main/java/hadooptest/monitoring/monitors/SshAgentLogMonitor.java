package hadooptest.monitoring.monitors;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.constants.LoggingNomenclature;
import hadooptest.automation.utils.exceptionParsing.ExceptionParsingOrchestrator;
import hadooptest.automation.utils.exceptionParsing.ExceptionPeel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Session;

/**
 * <p>
 * Fetches the logs relevant from the start to finish of a test run. When the
 * test starts, this fetches the metadata {@link LogMetadata} for each file.
 * There is a predictable format of the logs, so if a log exists on the host the
 * start timestamp is marked and logs are fetched from that point onwards, until
 * the end of the test.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class SshAgentLogMonitor extends AbstractMonitor {
	Logger logger = Logger.getLogger(SshAgentLogMonitor.class);
	List<SshAgentLogMetadata> fileMetadataList;
	private volatile ConcurrentHashMap<String, Boolean> sshAgentRunFlags;
	private volatile ConcurrentMap<String, File> fileHandlesToLocalFileDump = new ConcurrentHashMap<String, File>();
	private volatile ConcurrentMap<String, PrintWriter> printWritersToLocalFileDump = new ConcurrentHashMap<String, PrintWriter>();
	private volatile ConcurrentMap<String, Thread> tailThreads = new ConcurrentHashMap<String, Thread>();
	private volatile ConcurrentHashMap<String, JavaSshClient> jschExecs = new ConcurrentHashMap<String, JavaSshClient>();
	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";
	ArrayList<String> filesContainingExceptions;
	static String USER = "user";
	static String PREFIX = "prefix";
	ExceptionMonitor exceptionMonitor;

	public SshAgentLogMonitor(String clusterName,
			HashMap<String, ArrayList<String>> sentComponentToHostMapping,
			Class<?> testClass, String testMethodName) {
		super(clusterName, sentComponentToHostMapping, testClass,
				testMethodName, 0,
				HadooptestConstants.Miscellaneous.SSH_AGENT_FILE);

		commandStrings = null;
		fileMetadataList = new ArrayList<SshAgentLogMetadata>();
		sshAgentRunFlags = new ConcurrentHashMap<String, Boolean>();
		filesContainingExceptions = new ArrayList<String>();
		/*
		 * ExceptionMonitor, for once the logs have been received.
		 */
		exceptionMonitor = new ExceptionMonitor(clusterName,
				sentComponentToHostMapping, testClass, testMethodName, -1,
				HadooptestConstants.Miscellaneous.EXCEPTIONS);

		initializeFileMetadata();

	}

	void initializeFileMetadata() {
		for (String aComponent : componentToHostMapping.keySet()) {
			logger.info("Ssh Agent Log processing for component:" + aComponent);
			for (String aHostname : componentToHostMapping.get(aComponent)) {
				if (aComponent
						.equalsIgnoreCase(HadooptestConstants.NodeTypes.NODE_MANAGER)) {
					// Since this is a repetition of datanodes
					continue;
				}
				SshAgentLogMetadata fileMetadata = new SshAgentLogMetadata(
						LoggingNomenclature.getLogPrefix(aComponent),
						LoggingNomenclature.getUser(aComponent), aComponent,
						aHostname);
				if (fileMetadata.fileExists) {
					fileMetadataList.add(fileMetadata);
					sshAgentRunFlags.put(fileMetadata.fileName, true);
					try {
						File localFileHandle = createLocalFileAndReturnHandleToLocalFile(fileMetadata.fileName);
						fileHandlesToLocalFileDump.put(fileMetadata.fileName,
								localFileHandle);
						printWritersToLocalFileDump.put(fileMetadata.fileName,
								new PrintWriter(localFileHandle));
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}

	public File createLocalFileAndReturnHandleToLocalFile(
			String completeRemoteFilePath) {

		String dumpLocation;
		dumpLocation = baseDirPathToDump + this.kind;
		int lastIndexOfBackSlash = completeRemoteFilePath.lastIndexOf('/');
		String dirAddition = completeRemoteFilePath.substring(0,
				lastIndexOfBackSlash);
		dumpLocation += dirAddition;
		logger.info("Tailed data would be stored locally, here:" + dumpLocation);
		new File(dumpLocation).mkdirs();
		String filenameWithPath = dumpLocation + "/"
				+ completeRemoteFilePath.substring(lastIndexOfBackSlash + 1);
		filesContainingExceptions.add(filenameWithPath);
		return new File(filenameWithPath);

	}

	@Override
	public void stopMonitoring() {
		logger.info("Stop monitoring received in SshAgentLogMonitor for "
				+ this.kind);
		monitoringUnderway = false;
		ChannelExec channelExec = null;
		Session session = null;

		for (String fileBeingMonitored : sshAgentRunFlags.keySet()) {
			JavaSshClient execJsch = jschExecs.get(fileBeingMonitored);
			if (execJsch != null){
				execJsch.keepRunning = false;
			}
			printWritersToLocalFileDump.get(fileBeingMonitored).close();
			if (execJsch !=null){
				channelExec = (ChannelExec) execJsch.channel;
				session = execJsch.session;
				TestSession.logger.debug("Disconnecting channel and session for:"
					+ fileBeingMonitored);
			}
			try {
				if (channelExec != null) {
					OutputStream out = channelExec.getOutputStream();
					/*
					 * Corresponds to sending CTL+C
					 */
					out.write(3);
					out.flush();
					/*
					 * Disconnect from the "tail -F", else this would leave
					 * zombie processes on the server.
					 */
					channelExec.disconnect();
					session.disconnect();
				}

			} catch (IOException e1) {
				TestSession.logger
						.debug("Exception Rx while executing test case:"
								+ tailThreads.get(fileBeingMonitored).getName());
				TestSession.logger.debug("channelExec was not null, it had id"
						+ channelExec);
				e1.printStackTrace();
			}

			/*
			 * Disconnect from the "tail -F", else this would leave zombie
			 * processes on the server.
			 */
//			channelExec.disconnect();
//			session.disconnect();

			sshAgentRunFlags.put(fileBeingMonitored, false);
			for (final SshAgentLogMetadata sshAgentLogMetadata : fileMetadataList) {
				tailThreads.get(sshAgentLogMetadata.fileName).interrupt();
			}

		}
		/*
		 * Once all the logs have been received, Scour all the exceptions
		 * received for the test and log them.
		 */
		ExceptionParsingOrchestrator exceptionO10R = new ExceptionParsingOrchestrator(
				filesContainingExceptions);

		try {
			exceptionO10R.peelAndRememberExceptions();
			List<ExceptionPeel> exceptionPeels = exceptionO10R
					.fetchExceptionPeels();
			exceptionMonitor.setExceptionPeels(exceptionPeels);
			exceptionMonitor.logExceptions();

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void fetchResourceUsageIntoMemory(int tick) throws IOException {
		TestSession.logger.info("SSH tick count:" + tick
				+ " && fileMetadataList size=" + fileMetadataList.size());
		Thread aThread = null;

		for (final SshAgentLogMetadata sshAgentLogMetadata : fileMetadataList) {
			aThread = null;
			if (sshAgentLogMetadata.fileExists) {
				aThread = new Thread() {
					@Override
					public void run() {
						StringBuilder sb = new StringBuilder();
						sb.append("tail -F ");
						sb.append(" ");
						sb.append(sshAgentLogMetadata.fileName);
						doJavaSSHClientExec(sshAgentLogMetadata.hostname,
								sb.toString(),
								HADOOPQA_AS_HDFSQA_IDENTITY_FILE,
								sshAgentLogMetadata.fileName);

					}
				};
				aThread.setName(sshAgentLogMetadata.fileName);
				tailThreads.put(sshAgentLogMetadata.fileName, aThread);
				aThread.start();
				// sshAgentInputStreams.put(fileBeingMonitored,
				// execJsch.inputStream);
			}
		}
		for (final SshAgentLogMetadata sshAgentLogMetadata : fileMetadataList) {
			try {
				tailThreads.get(sshAgentLogMetadata.fileName).join();
			} catch (InterruptedException e1) {
				System.out
						.println("Thread ["
								+ tailThreads.get(sshAgentLogMetadata.fileName)
										.getName()
								+ "] received interrupt, was it because the test ended??");
			}
		}

	}

	public void doJavaSSHClientExec(String host, String command,
			String identityFile, String fileBeingMonitored) {
		String user = "hdfsqa";
		JavaSshClient execJsch = new JavaSshClient(user, host, command,
				identityFile, fileBeingMonitored,
				printWritersToLocalFileDump.get(fileBeingMonitored));
		jschExecs.put(fileBeingMonitored, execJsch);
		execJsch.execute();

		logger.info("SSH Client thread is about to run command:" + command
				+ " on host:" + host);
	}

	@Override
	public void dumpData() {
		/*
		 * Unused
		 */
	}

}

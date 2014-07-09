package hadooptest.monitoring.monitors;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

/**
 * <p>
 * Before a test starts, this method goes and fetches the last timestamp in the
 * file, and sets it as a marker. Once the test ends the records are 'cut' from
 * the marked location through the end of the file and are copied by the
 * {@link LogMonitor}
 * </p>
 * 
 * @author tiwari
 * 
 */
public class SshAgentLogMetadata {
	String fileName;
	String loggingStartTime;
	boolean fileExists;
	String hostname;
	String logPrefix;
	Logger logger = Logger.getLogger(SshAgentLogMetadata.class);
	private static final String HADOOPQA_AS_HDFSQA_IDENTITY_FILE = "/homes/hadoopqa/.ssh/flubber_hadoopqa_as_hdfsqa";

	SshAgentLogMetadata(String logPrefix, String user, String componentName,
			String componentHostname) {
		this.hostname = componentHostname;
		this.logPrefix = logPrefix;
		this.fileName = HadooptestConstants.Log.LOG_LOCATION + user + "/"
				+ logPrefix + "-" + user + "-" + componentName + "-"
				+ componentHostname + HadooptestConstants.Log.DOT_LOG;
		if (fileExistOnHost(componentHostname)) {
			this.fileExists = true;

		} else {
			this.fileExists = false;
		}
	}

	synchronized boolean fileExistOnHost(String hostname) {
		int returnCode;
		StringBuilder sb = new StringBuilder();
		sb.append("test");
		sb.append(" ");
		sb.append("-f");
		sb.append(" ");
		sb.append(this.fileName);
		returnCode = doJavaSSHClientExec(hostname, sb.toString(),
				HADOOPQA_AS_HDFSQA_IDENTITY_FILE);
		if (returnCode != 0) {
			return false;
		} else {
			return true;
		}
	}

	public int doJavaSSHClientExec(String host, String command,
			String identityFile) {
		JSch jsch = new JSch();
		String user = "hdfsqa";
		TestSession.logger.debug("SSH Client is about to run command:" + command
				+ " on host:" + host);
		Session session;
		int exitStatus = -1;
		try {
			session = jsch.getSession(user, host, 22);
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			Channel channel = session.openChannel("exec");
			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);

			InputStream in = channel.getInputStream();

			channel.connect();

			byte[] tmp = new byte[1024];
			while (true) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
					TestSession.logger.trace(outputFragment);

				}
				if (channel.isClosed()) {
					TestSession.logger.trace("exit-status: "
							+ channel.getExitStatus());
					exitStatus = channel.getExitStatus();
					break;
				}
				// try {
				// Thread.sleep(1000);
				// } catch (Exception ee) {
				// }
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return exitStatus;
	}

	public class MyUserInfo implements UserInfo {

		public String getPassphrase() {
			// TODO Auto-generated method stub
			return null;
		}

		public String getPassword() {
			// TODO Auto-generated method stub
			return null;
		}

		public boolean promptPassphrase(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptPassword(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public boolean promptYesNo(String arg0) {
			// TODO Auto-generated method stub
			return false;
		}

		public void showMessage(String arg0) {
			// TODO Auto-generated method stub

		}

	}

}

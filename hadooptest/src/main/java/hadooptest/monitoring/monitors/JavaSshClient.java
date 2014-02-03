package hadooptest.monitoring.monitors;

import hadooptest.TestSession;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class JavaSshClient {
//	Logger logger = Logger.getLogger(SshAgentLogMonitor.class);

	private JSch jsch;
	public Session session;
	public Channel channel;
	public String host;
	public String command;
	public String identityFile;
	public String fileBeingMonitored;
	public String user;
	public volatile boolean keepRunning;
	public File fileHandleToLocalFileDump;
	public PrintWriter printWriterToLocalFile;

	public JavaSshClient(String user, String host, String command,
			String identityFile, String fileBeingMonitored,
			PrintWriter printWriterToLocalFile) {
		this.user = user;
		this.host = host;
		this.command = command;
		this.identityFile = identityFile;
		this.fileBeingMonitored = fileBeingMonitored;
		this.keepRunning = keepRunning;
		this.printWriterToLocalFile = printWriterToLocalFile;
		this.keepRunning = true;
		this.jsch = new JSch();

	}

	public void execute() {
		try {
			System.out.println("Execute called for " + fileBeingMonitored
					+ " <-----------------------");
			session = jsch.getSession(user, host, 22);
			System.out.println("Got Session" + session+" for thread[" + fileBeingMonitored
					+ "] completed. Returning!");
			jsch.addIdentity(identityFile);
			UserInfo ui = new MyUserInfo();
			session.setUserInfo(ui);
			session.setConfig("StrictHostKeyChecking", "no");
			session.connect();
			this.channel = session.openChannel("exec");
			if (channel == null){
				System.out.println("Channel was allocated NULL!");
			}

			((ChannelExec) channel).setCommand(command);
			channel.setInputStream(null);
			((ChannelExec) channel).setErrStream(System.err);
			((ChannelExec) channel).setPty(true);

			InputStream in = channel.getInputStream();
			channel.connect();
			System.out.println("Connected with host:" + host + " as user:"
					+ user);
			byte[] tmp = new byte[1024];

			while (keepRunning) {
				while (in.available() > 0) {
					int i = in.read(tmp, 0, 1024);
					if (i < 0)
						break;
					String outputFragment = new String(tmp, 0, i);
//					System.out.println(outputFragment);
					printWriterToLocalFile.print(outputFragment);
				}

				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: " + channel.getExitStatus());
					break;
				}
				try {
					Thread.sleep(1000);
				} catch (Exception ee) {

				}
			}
			channel.disconnect();
			session.disconnect();

		} catch (JSchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Thread [" + fileBeingMonitored
				+ "] completed. Returning!");

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

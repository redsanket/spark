package hadooptest.tez.ats;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.config.HttpClientConfig.httpClientConfig;
import static com.jayway.restassured.config.RestAssuredConfig.newConfig;
import static org.apache.http.client.params.ClientPNames.COOKIE_POLICY;
import static org.apache.http.client.params.CookiePolicy.BROWSER_COMPATIBILITY;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.automation.utils.http.HTTPHandle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.HashMap;

import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.http.client.params.ClientPNames;
import org.junit.After;
import org.junit.Before;

import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

public class ATSTestsBaseClass extends TestSession {
	public static boolean timelineserverStarted = false;


	
	public static HashMap<String, String> userCookies = new HashMap<String, String>();
	
	public enum EntityTypes {
		  TEZ_APPLICATION_ATTEMPT,
		  TEZ_CONTAINER_ID,
		  TEZ_DAG_ID,
		  TEZ_VERTEX_ID,
		  TEZ_TASK_ID,
		  TEZ_TASK_ATTEMPT_ID,
		}


	@Before
	public void getCookiesForAllUsers() throws Exception {
		HTTPHandle httpHandle = new HTTPHandle();
		String hitusr_1_cookie = null;
		String hitusr_2_cookie = null;
		String hitusr_3_cookie = null;
		String hitusr_4_cookie = null;

		hitusr_1_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_1);
		userCookies.put(HadooptestConstants.UserNames.HITUSR_1, hitusr_1_cookie);
		hitusr_2_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_2);
		userCookies.put(HadooptestConstants.UserNames.HITUSR_2, hitusr_2_cookie);
		hitusr_3_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_3);
		userCookies.put(HadooptestConstants.UserNames.HITUSR_3, hitusr_3_cookie);
		hitusr_4_cookie = httpHandle
				.loginAndReturnCookie(HadooptestConstants.UserNames.HITUSR_4);
		userCookies.put(HadooptestConstants.UserNames.HITUSR_4, hitusr_4_cookie);		

	}

	public void ensureTimelineserverStarted(String resourceManagerHost) throws Exception {

		String url = "http://" + resourceManagerHost + ":" + HadooptestConstants.Ports.HTTP_ATS_PORT + "/ws/v1/timeline/";
		int MAX_COUNT = 10;
		int count = 1;

		do {
			Thread.sleep(1000);
			try {
				Response response = given()
						.log()
						.all()
						.cookie(userCookies.get(HadooptestConstants.UserNames.HITUSR_1))
						.param("User-Agent", "Mozilla/5.0")
						.param("Accept",
								"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
						.param(ClientPNames.COOKIE_POLICY,
								CookiePolicy.RFC_2965)
						.param("Content-Type", "application/json")
						.config(newConfig().httpClient(
								httpClientConfig().setParam(COOKIE_POLICY,
										BROWSER_COMPATIBILITY)))
						.redirects()
						.follow(false).get(url);

				TestSession.logger.info("Response status Line:"
						+ response.getStatusLine());
				TestSession.logger.info("Response status code:"
						+ response.getStatusCode());

				for (Header aResponseHeader : response.getHeaders()) {
					TestSession.logger.info(aResponseHeader.getName() + " : "
							+ aResponseHeader.getValue());
				}

				TestSession.logger.info(response.body().asString());
				TestSession.logger.info("Response code:"
						+ response.getStatusCode() + " received.");

				if (response.getStatusCode() == 202) {
					break;
				}
			} catch (Exception e) {
				TestSession.logger
						.error("Exception:"
								+ e.getClass()
								+ " Received and the timeline server has not started yet. Loop count ["
								+ count + "/" + MAX_COUNT + "]");
				TestSession.logger.error(e.getCause());

			}
		} while (++count <= MAX_COUNT);

	}

	public void startTimelineServerOnRM(String rmHost) throws Exception {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh start timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
		timelineserverStarted = true;
		ensureTimelineserverStarted(rmHost);
		TestSession.logger.info("Timelineserver started");
	}

	public void stopTimelineServerOnRM(String rmHost) {
		String command = "/home/gs/gridre/yroot."
				+ System.getProperty("CLUSTER_NAME")
				+ "/share/hadoop/sbin/yarn-daemon.sh stop timelineserver";
		doJavaSSHClientExec(
				HadooptestConstants.UserNames.MAPREDQA,
				rmHost,
				command,
				HadooptestConstants.Location.Identity.HADOOPQA_AS_MAPREDQA_IDENTITY_FILE);
		timelineserverStarted = false;
		TestSession.logger.info("Timelineserver stopped");
	}

	String printResponseAndReturnItAsString(Process process)
			throws InterruptedException, IOException {
		StringBuffer sb = new StringBuffer();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				process.getInputStream()));
		String line;
		line = reader.readLine();
		while (line != null) {
			sb.append(line);
			sb.append("\n");
			TestSession.logger.debug(line);
			line = reader.readLine();
		}

		process.waitFor();
		return sb.toString();
	}

	public String doJavaSSHClientExec(String user, String host, String command,
			String identityFile) {
		JSch jsch = new JSch();
		TestSession.logger.info("SSH Client is about to run command:" + command
				+ " on host:" + host + "as user:" + user
				+ " using identity file:" + identityFile);
		Session session;
		StringBuilder sb = new StringBuilder();
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
					TestSession.logger.info(outputFragment);
					sb.append(outputFragment);
				}
				if (channel.isClosed()) {
					TestSession.logger.info("exit-status: "
							+ channel.getExitStatus());
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

		return sb.toString();
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

	String getHostNameFromIp(String ip) throws Exception {

		InetAddress iaddr = InetAddress.getByName(ip);
		System.out.println("And the Host name of the g/w is:"
				+ iaddr.getHostName());
		return iaddr.getHostName();

	}

	@After
	public void logTaskReportSummary() throws Exception {
	}

}

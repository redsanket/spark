package hadooptest.cluster;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class MultiClusterClient extends Thread {

	private static int SERVER_PORT;
	private static String SERVER_HOSTNAME;

	private boolean runClient = true;
	
	public MultiClusterClient(int port, String hostname) {
		super("MultiClusterClient");
		
		SERVER_PORT = port;
		SERVER_HOSTNAME = hostname;
	}
	
	public void stopClient() {
		runClient = false;
	}

	public void run() {

		Socket mcSocket = null;
		PrintWriter out = null;
		BufferedReader in = null;
		
		try {
			mcSocket = new Socket(SERVER_HOSTNAME, SERVER_PORT);
			out = new PrintWriter(mcSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(mcSocket.getInputStream()));
			out.println("Hi, this is a multi cluster client.");
		} catch (UnknownHostException e) {
			TestSession.logger.error("Don't know about hostname");
			throw new RuntimeException(e);
		} catch (IOException e) {
			TestSession.logger.error("Couldn't get I/O for the connection to hostname.");
			throw new RuntimeException(e);
		}

		String fromServer, outputLine;

		try {
			MultiClusterProtocol mcp = new MultiClusterProtocol();
			
			while ((fromServer = in.readLine()) != null) {					
				if (!runClient)
					break;
				
				TestSession.logger.info("Server: " + fromServer);
				outputLine = mcp.processInput(fromServer);
				if (outputLine != null) {
					out.println(outputLine);
				}	
				else {
					out.println("CLIENT_READY");
				}
			}

			out.close();
			in.close();
			mcSocket.close();
		}
		catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
}

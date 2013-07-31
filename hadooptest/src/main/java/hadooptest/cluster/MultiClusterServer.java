package hadooptest.cluster;

import coretest.TestSessionCore;
import coretest.Util;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class MultiClusterServer extends Thread {

	private static int SERVER_PORT;
	
	private boolean runServer = true;
	
	private PrintWriter out;
	private MultiClusterProtocol mcp;
	
	public MultiClusterServer(int port) {
		super("MultiClusterServer");
		
		SERVER_PORT = port;
		
		mcp = new MultiClusterProtocol();
	}
	
	public void stopServer() {
		runServer = false;
	}
	
	public void run() {
		ServerSocket serverSocket = null;
		Socket socket = null;

		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			socket = serverSocket.accept();
		} catch (IOException ioe) {
			TestSessionCore.logger.error("Could not listen on port: " + SERVER_PORT, ioe);
			throw new RuntimeException(ioe);
		}

		try {
			while (runServer) {
				out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(
						new InputStreamReader(
								socket.getInputStream()));

				String inputLine, outputLine;
				outputLine = mcp.processInput(null);
				TestSessionCore.logger.info(outputLine);

				boolean requestedVersion = false;

				while ((inputLine = in.readLine()) != null) {

					if (!requestedVersion) {
						requestedVersion = true;
						out.println("RETURN_VERSION");
					}
					
					TestSessionCore.logger.info("Client: " + inputLine);
					outputLine = mcp.processInput(inputLine);
					if(outputLine != null) {
						out.println(outputLine);
					}
						
					if (!runServer)
						break;
				}
				out.close();
				in.close();
				socket.close();
			}
		}
		catch (IOException ioe) {
			TestSessionCore.logger.error("Fatal IO error while listening for clients.", ioe);
			throw new RuntimeException(ioe);
		}

		try {
			serverSocket.close();
		}
		catch (IOException ioe) {
			TestSessionCore.logger.error("Unable to close the multi cluster server socket.", ioe);
			throw new RuntimeException(ioe);
		}
	}
	
	public String getClientDFSName(int timeout) throws InterruptedException {
		mcp.clientDFSName = "";
		out.println("DFS_GET_DEFAULT_NAME");
		
		while (mcp.clientDFSName == "") {
			Util.sleep(1);
			timeout--;
			TestSession.logger.info("Waiting for client DFS Name response.");
			
			if (timeout <= 0) { 
				TestSession.logger.debug("Waited for the client DFS name " + 
						"response for " + timeout + " seconds, and there " + 
						"was no reponse.  Timing out.");
				break; 
			}
		}
		
		return mcp.clientDFSName;
	}
	
	public void requestClientDfsRemoteLocalCopy(String src, String dest) {
		out.println(mcp.processInput("DFS_REMOTE_LOCAL_COPY " + src + " " + dest));
	}

	public void requestClientDfsRemoteDfsCopy(String srcDfs, String srcFile, String destDfs, String destFile) {
		out.println(mcp.processInput("DFS_REMOTE_DFS_COPY " + srcDfs + " " + srcFile + " " + destDfs + " " + destFile));
	}
}

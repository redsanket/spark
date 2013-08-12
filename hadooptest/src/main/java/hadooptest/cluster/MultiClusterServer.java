package hadooptest.cluster;

import hadooptest.TestSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import coretest.TestSessionCore;
import coretest.Util;

/**
 * A class which represents the server in the multi-cluster capability of the
 * framework.
 */
public class MultiClusterServer extends Thread {
	
	/** The server port **/
	private static int SERVER_PORT;
	
	/** Whether the server is running or not **/
	private boolean runServer = true;
	
	/** The output print writer for the server **/
	private PrintWriter out;
	
	/** The protocol to be used by the server **/
	private MultiClusterProtocol mcp;
	
	/** Whether the client is connected or not **/
	private boolean clientConnected = false;
	
	/**
	 * Class constructor.  Initializes the thread, sets the server port,
	 * and initializes the multi cluster protocol.
	 * 
	 * @param port The port that the server listens on.
	 */
	public MultiClusterServer(int port) {
		super("MultiClusterServer");
		
		SERVER_PORT = port;
		
		mcp = new MultiClusterProtocol();
	}
	
	/**
	 * Stops the multi cluster server.
	 */
	public void stopServer() {
		runServer = false;
	}
	
	/**
	 * The thread for the multi cluster server.
	 * 
	 * Listens for a multi cluster client to join, and converses with it
	 * using the multi cluster protocol.
	 */
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
					
					clientConnected = true;
					
					if (!runServer) {
						break;
					}
				
					if (!requestedVersion) {
						requestedVersion = true;
						out.println("RETURN_VERSION");
					}
					
					TestSessionCore.logger.info("Client: " + inputLine);
					outputLine = mcp.processInput(inputLine);
					if(outputLine != null) {
						out.println(outputLine);
					}

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
	
	/**
	 * Get the DFS name of the connected client.
	 * 
	 * @param timeout The period of time to wait for a response.
	 * 
	 * @return String the DFS name of the connected client.
	 * 
	 * @throws InterruptedException if unable to sleep the thread.
	 */
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
						"was no response.  Timing out.");
				break; 
			}
		}
		
		return mcp.clientDFSName;
	}
	
	/**
	 * Request that the connected client does a copy of a file local to the
	 * connected client filesystem, to the client DFS.
	 * 
	 * @param src the source location of the file on the client filesystem.
	 * @param dest the destination of the file in the client DFS.
	 */
	public void requestClientDfsRemoteLocalCopy(String src, String dest) {
		out.println(mcp.processInput("DFS_REMOTE_LOCAL_COPY " + src + " " + dest));
	}

	/**
	 * Request that the connected client does a copy of a file in the client's
	 * DFS, to another DFS.
	 * 
	 * @param srcDfs the client DFS.
	 * @param srcFile the location of the file in the client DFS.
	 * @param destDfs the destination DFS.
	 * @param destFile the location to put the file in the destination DFS.
	 */
	public void requestClientDfsRemoteDfsCopy(String srcDfs, String srcFile, String destDfs, String destFile) {
		out.println(mcp.processInput("DFS_REMOTE_DFS_COPY " + srcDfs + " " + srcFile + " " + destDfs + " " + destFile));
	}

	/**
	 * Find out if a file exists on a remote DFS on the connected client.
	 * 
	 * @param path the path to the file on the client DFS.
	 * @param timeout the time to wait for a response from the client.
	 * 
	 * @return boolean Whether or not the file exists.
	 * 
	 * @throws InterruptedException if we cannot sleep the thread.
	 */
	public boolean requestClientDfsLs(String path, int timeout) throws InterruptedException {
		out.println(mcp.processInput("DFS_REMOTE_DFS_LS_FILE_EXISTS " + path));
		
		while (mcp.fileExists == false) {
			Util.sleep(1);
			timeout--;
			TestSession.logger.info("Waiting for client DFS FS LS file exists response.");
			
			if (mcp.fileExists == true) {
				return true;
			}
			
			if (timeout <= 0) {
				TestSession.logger.debug("Waited for the client DFS LS file exists " + 
								"response for " + timeout + " seconds, and there " +
								"was no response.  Timing out.");
				break;
			}
			
		}
		
		return false;
	}
	
	/**
	 * Whether or not a client is connected to the server.
	 * 
	 * @return whether or not a client is connected to the server.
	 */
	public boolean isClientConnected() {
		return clientConnected;
	}
	
	/**
	 * Stop the attached client.
	 */
	public void requestClientStop() {
		out.println("CLIENT_STOP");
	}
	
	/**
	 * From the multi cluster server gateway, start a client on a remote
	 * gateway to another cluster.
	 * 
	 * @param gateway the gateway to start the client on.
	 * @param cluster the cluster to start the client on.
	 * @param user the user to start the client as.
	 * @param clientFrameworkConf the framework configuration file to use for
	 * 			the client.
	 * @throws IOException if there is a fatal problem running the command
	 * 			to start the client.
	 */
	public void remoteStartMultiClusterClient(String gateway, 
			String cluster, String user, String clientFrameworkConf) 
					throws IOException {
		
		TestSession.logger.info(
				"Starting the multicluster client on the remote gateway...");
		TestSession.logger.info("MultiCluster client gateway is: " + gateway);
		TestSession.logger.info("MultiCluster client cluster is: " + cluster);
		TestSession.logger.info("MultiCluster client user is: " + user);
		TestSession.logger.info(
				"MultiCluster client framework configuration file is: " + 
						clientFrameworkConf);
		
		String[] clientInitCmd = { "/home/y/bin/pdsh", "-w", 
				gateway, "pushd /tmp/hadooptest-" + user + "-" + cluster + 
				"/hadooptest/;/tmp/hadooptest-" + user + "-" + cluster + 
				"/hadooptest/scripts/run_hadooptest -c " + cluster + " -f " + 
				clientFrameworkConf + " -m -n -w /tmp/hadooptest-" + user + 
				"-" + cluster + 
				"/hadooptest/ -t TestMultiClusterClientConnection" };
		
		TestSession.exec.runProcBuilderGetProc(clientInitCmd);
	}
}

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
	
	public MultiClusterServer(int port) {
		super("MultiClusterServer");
		
		SERVER_PORT = port;
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
				MultiClusterProtocol mcp = new MultiClusterProtocol();
				outputLine = mcp.processInput(null);
				TestSessionCore.logger.info(outputLine);

				String clientVersion = "";
				String clientDFSName = "";

				while ((inputLine = in.readLine()) != null) {
					TestSessionCore.logger.info("Client: " + inputLine);
					outputLine = mcp.processInput(inputLine);
					out.println(outputLine);
					
					if (inputLine.contains("CLIENT HADOOP VERSION = ")) {
						clientVersion = inputLine;
					}
					
					if (clientVersion.equals("")) {
						out.println("RETURN_VERSION");
					}
					
					if (clientDFSName.equals("")) {
						out.println("DFS_GET_DEFAULT_NAME");
					}
					
					if (inputLine.contains("CLIENT DFS DEFAULT NAME = ")) {
						clientDFSName = inputLine.substring(inputLine.indexOf("= ") + 2);
						TestSession.logger.info("Client DFS Name response is: " + clientDFSName);
					}
					
					if (!clientDFSName.equals("")) {
						out.println("DFS_COPY_LOCAL /homes/hadoopqa/hadooptest.conf " + clientDFSName + "/user/hadoopqa/hadooptest.conf");
						
						try {
							Util.sleep(15);
						}
						catch (InterruptedException ie) {}
						
						out.println("DFS_COPY " + clientDFSName + " /user/hadoopqa/hadooptest.conf " + TestSession.cluster.getConf().get("fs.defaultFS") + " /user/hadoopqa/hadooptest.conf.test.1");
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
	
	public void getClientDFSName() {
		out.println("DFS_GET_DEFAULT_NAME");
	}
}

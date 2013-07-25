package hadooptest.cluster;

import java.io.BufferedReader;

import java.lang.RuntimeException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;

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

		String fromServer;

		try {
			while ((fromServer = in.readLine()) != null) {
				TestSession.logger.info("Server: " + fromServer);
				if (fromServer.equals("Bye."))
					break;
				else if (fromServer.equals("RETURN_VERSION")) {
					out.println("CLIENT HADOOP VERSION = " + TestSession.cluster.getVersion());
				}
				else if (fromServer.equals("CLUSTER_STOP")) {
					// Stop the cluster here
					out.println("I got the request to stop the cluster.");
				}
				else if (fromServer.contains("DFS_GET_DEFAULT_NAME")) {
	        		out.println("CLIENT DFS DEFAULT NAME = " + TestSession.cluster.getConf().get("fs.defaultFS"));
	        	}
				else if (fromServer.contains("DFS_COPY_LOCAL ")) {
					try {
						DFS dfs = new DFS();
						
						String[] fromServerSplit = fromServer.split(" ");
						String src = fromServerSplit[1];
						String dest = fromServerSplit[2];
						
						dfs.copyLocalToHdfs(src, dest);
						dfs.printFsLs(dest, true);
						out.println("DFS_COPY_LOCAL_RESULT = true" );
					}
					catch (Exception e) {
						out.println("DFS_COPY_LOCAL_RESULT = EXCEPTION: " + e.getMessage());
					}
				}
				else if (fromServer.contains("DFS_COPY ")) {
	        		try {
	        			DFS dfs = new DFS();
	        			
	        			String[] fromServerSplit = fromServer.split(" ");
	        			String srcClusterBasePath = fromServerSplit[1];
	        			String srcPath = fromServerSplit[2];
	        			String destClusterBasePath = fromServerSplit[3];
	        			String destPath = fromServerSplit[4];
	        		
	        			out.println("DFS_COPY_RESULT = " + dfs.copyDfsToDfs(srcClusterBasePath, srcPath, destClusterBasePath, destPath));
	        		}
	        		catch (Exception e) {
	        			out.println("DFS_COPY_RESULT = EXCEPTION: " + e.getMessage());
	        		}
	        	}

				if (!runClient)
					break;
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

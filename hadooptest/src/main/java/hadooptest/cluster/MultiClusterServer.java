package hadooptest.cluster;

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
	
	public MultiClusterServer(int port) {
		super("MultiClusterServer");
		
		SERVER_PORT = port;
	}
	
	public void stopServer() {
		runServer = false;
	}
	
	public void stopCluster() {
		
	}
	
	public void run() {
		ServerSocket serverSocket = null;
		Socket socket = null;

		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			socket = serverSocket.accept();
		} catch (IOException ioe) {
			TestSession.logger.error("Could not listen on port: " + SERVER_PORT, ioe);
			throw new RuntimeException(ioe);
		}

		try {
			while (runServer) {
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(
						new InputStreamReader(
								socket.getInputStream()));

				String inputLine, outputLine;
				MultiClusterProtocol mcp = new MultiClusterProtocol();
				outputLine = mcp.processInput(null);
				TestSession.logger.info(outputLine);

				while ((inputLine = in.readLine()) != null) {
					outputLine = mcp.processInput(inputLine);
					out.println(outputLine);
					if (!runServer)
						break;
				}
				out.close();
				in.close();
				socket.close();
			}
		}
		catch (IOException ioe) {
			TestSession.logger.error("Fatal IO error while listening for clients.", ioe);
			throw new RuntimeException(ioe);
		}

		try {
			serverSocket.close();
		}
		catch (IOException ioe) {
			TestSession.logger.error("Unable to close the multi cluster server socket.", ioe);
			throw new RuntimeException(ioe);
		}
	}
}

package hadooptest.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class MultiClusterClient {
	public static void main(String[] args) throws IOException {

		Socket mcSocket = null;
		PrintWriter out = null;
		BufferedReader in = null;

		String serverHostName = "localhost";
		for (String arg: args) {
			serverHostName = arg;
			break;
		}
		
		try {
			mcSocket = new Socket(serverHostName, 4444);
			out = new PrintWriter(mcSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(mcSocket.getInputStream()));
		} catch (UnknownHostException e) {
			System.err.println("Don't know about hostname");
			
			
			
			
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to hostname.");
			System.exit(1);
		}

		String fromServer;

		while ((fromServer = in.readLine()) != null) {
			System.out.println("Server: " + fromServer);
			if (fromServer.equals("Bye."))
				break;
			else if (fromServer.equals("CLUSTER_STOP")) {
				// Stop the cluster here
				out.println("I got the request to stop the cluster.");
			}
		}

		out.close();
		in.close();
		mcSocket.close();
	}
}

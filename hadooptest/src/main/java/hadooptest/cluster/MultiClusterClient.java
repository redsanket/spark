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

		try {
			mcSocket = new Socket("gwbl2005", 4444);
			out = new PrintWriter(mcSocket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(mcSocket.getInputStream()));
		} catch (UnknownHostException e) {
			System.err.println("Don't know about hostname");
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to hostname.");
			System.exit(1);
		}

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String fromServer;
		String fromUser;

		while ((fromServer = in.readLine()) != null) {
			System.out.println("Server: " + fromServer);
			if (fromServer.equals("Bye."))
				break;

			fromUser = stdIn.readLine();
			if (fromUser != null) {
				System.out.println("Client: " + fromUser);
				out.println(fromUser);
			}
		}

		out.close();
		in.close();
		stdIn.close();
		mcSocket.close();
	}
}

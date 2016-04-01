package hadooptest.gdm.regression.stackIntegration.lib;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import org.apache.commons.lang3.tuple.ImmutablePair;

public class SystemCommand {
	
	private static String errorMessage;
	
	/**
	 * Runs a system command locally
	 *
	 * @param  cmd  the command to run
	 * @return the exit value and output from the command result.  Returns null on an exception.
	 */
	public static ImmutablePair<Integer, String> runCommand(String cmd) {
		String res = "";
		int exitValue = 0;
		try {
			Runtime r = Runtime.getRuntime();

			// Things like pipe and redirection are performed by a shell. You will
			// need to execute the commands within a shell like bash, csh, ksh, etc
			String[] cmdInShell = new String[] {
					"/bin/bash", "-c", cmd
			};

			ProcessBuilder builder = new ProcessBuilder(cmdInShell);
			builder.redirectErrorStream(true);
			Process p = builder.start();

			p.waitFor();
			exitValue = p.exitValue();
			BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = "";

			while ((line = b.readLine()) != null) {
				res += line + "\n";
			}
		} catch (InterruptedException e) {
			setErrorMessage(e.getMessage());
			return null;
		} catch (IOException e) {
			setErrorMessage(e.getMessage());
			return null;
		}
		return new ImmutablePair<Integer, String>(exitValue, res);
	}

	public static String getErrorMessage() {
		return errorMessage;
	}

	public static  void setErrorMessage(String errorMessage) {
		errorMessage = errorMessage;
	}

}

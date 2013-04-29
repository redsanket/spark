/*
 * YAHOO
 */

package hadooptest.cluster.hadoop.standalone;

import java.io.IOException;

import hadooptest.cluster.Executor;

/**
 * A class which represents an Executor for a standalone cluster.
 * 
 * Handles all system calls for standalone clusters.
 * 
 * This class is not finished and should not be used.
 */
public class StandaloneExecutor extends Executor {

	/**
	 * Run a local system command.
	 * 
	 * @param commandArray The system command to run.
	 * @param username the user to run the command as.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray, String username) 
			throws InterruptedException, IOException, Exception {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for Standalone we can just pass this through right to runProcBuilder
		// for now.
		return runProcBuilder(commandArray);			
	}

	/**
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.  Additionally, specify a username to run the
	 * command as, so the Kerberos security settings configuration can occur.
	 * 
	 * @param commandArray the string array containing the command to be executed.
	 * @param username the user to run the command as.
	 * 
	 * @return Process the process handle for the system command.
	 */
	public Process runHadoopProcBuilderGetProc(String[] commandArray, String username) 
			throws InterruptedException, IOException, Exception {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for Standalone we can just pass this through right to 
		// runProcBuilderGetProc for now.
		return runProcBuilderGetProc(commandArray);
	}

}

/*
 * YAHOO
 */

package hadooptest.cluster.pseudodistributed;

import hadooptest.cluster.Executor;

/**
 * A class which represents a fully distributed hadoop command
 */
public class PseudoDistributedExecutor extends Executor {

	/**
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray, String username) {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for PseudoDistributed we can just pass this through right to runProcBuilder
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
	public Process runHadoopProcBuilderGetProc(String[] commandArray, String username) {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for PseudoDistributed we can just pass this through right to 
		// runProcBuilderGetProc for now.
		return runProcBuilderGetProc(commandArray);
	}

}

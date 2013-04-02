/*
 * YAHOO
 */

package hadooptest.cluster.pseudodistributed;

import java.io.IOException;

import hadooptest.cluster.Executor;

/**
 * A class which represents an Executor for a pseudodistributed cluster.
 * 
 * Handles all system calls for pseudodistributed clusters.
 */
public class PseudoDistributedExecutor extends Executor {

	/**
	 * Returns the output of a system command, when given the command and a user to
	 * run the command as.  For the pseudo distributed cluster type, this simply
	 * runs the process as a process builder with no security.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param username the system username to run the command under.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray, String username) 
			throws Exception {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for PseudoDistributed we can just pass this through right to runProcBuilder
		// for now.
		return runProcBuilder(commandArray);			
	}

	/**
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.  Additionally, specify a username to run the
	 * command as, so the Kerberos security settings configuration can occur.  For the
	 * pseudo distributed cluster type, this simply runs the process as a process builder
	 * with no security.
	 * 
	 * @param commandArray the string array containing the command to be executed.
	 * @param username the user to run the command as.
	 * 
	 * @return Process the process handle for the system command.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	public Process runHadoopProcBuilderGetProc(String[] commandArray, String username) 
			throws IOException {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for PseudoDistributed we can just pass this through right to 
		// runProcBuilderGetProc for now.
		return runProcBuilderGetProc(commandArray);
	}

}

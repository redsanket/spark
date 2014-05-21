/*
 * YAHOO
 */

package hadooptest.cluster.storm;

import java.io.IOException;
import java.util.Map;

import hadooptest.cluster.Executor;

/**
 * A class which represents an Executor for a storm cluster.
 * 
 * Handles all system calls for fully distributed clusters.
 */
public class StormExecutor extends Executor {

	/**
	 * Run a local system command.
	 * 
	 * @param commandArray The system command to run.
	 * @param username the user to run the command as.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray, String username) 
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
	public Process runProcBuilderSecurityGetProc(String[] commandArray, String username) 
			throws InterruptedException, IOException, Exception {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for Standalone we can just pass this through right to 
		// runProcBuilderGetProc for now.
		return runProcBuilderGetProc(commandArray);
	}
	
	/**
	* Returns the Process handle to a system command that is run, when a command, user
	* name, and environment to run the command is specified.
	*
	* @param commandArray the command to run.  Each member of the string array should
	*                                              be an item in the command string that is otherwise
	*                                              surrounded by whitespace.
	* @param username the system username to run the command under.
	* @param env the environment variables to run the command with.
	* @return String[] the output of running the system command.
	*
	* @throws Exception if there is a fatal error running the process.
	*/
	public Process runProcBuilderSecurityGetProcWithEnv(String[] commandArray,
			String username, Map<String, String> env) throws Exception {
		return runProcBuilderGetProc(commandArray, env);
	}

}

/*
 * YAHOO
 */

package hadooptest.cluster;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import hadooptest.TestSessionCore;
import hadooptest.automation.constants.HadooptestConstants;

/**
 * The base class for running system processes from the framework.
 * 
 * Subclasses of Executor should implement Executor for a specific cluster type.
 * Currently, this is done to handle different levels of Hadoop security for the
 * different cluster types.
 */
public abstract class Executor {

	protected String CLUSTER_NAME;

	/**
	 * The constructor relies on the following framework configuration file
	 * key-value pairs to be defined. They are initialized here.
	 * 
	 * CLUSTER_NAME - The name of the test cluster (this is typically used on
	 * fully distributed clusters only. If undefined, it will default to using
	 * no name).
	 */
	public Executor() {
		CLUSTER_NAME = TestSessionCore.conf.getProperty("CLUSTER_NAME", "");
	}

	/**
	 * Returns the output of a system command, when given the command and a user
	 * to run the command as.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param username
	 *            the system username to run the command under.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public abstract String[] runProcBuilderSecurity(String[] commandArray,
			String username) throws Exception;

	/**
	 * Run a local system command.
	 * 
	 * @param commandArray
	 *            The system command to run.
	 * @param username
	 *            the user to run the command as.
	 * @param verbose
	 *            true for on, false for off. Default value is false.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray,
			String username, boolean verbose) throws Exception {
		// The FullyDistributed package implements this to setup kerberos
		// security,
		// but for Standalone we can just pass this through right to
		// runProcBuilder
		// for now.
		return runProcBuilder(commandArray, verbose);
	}

	/**
	 * Returns the Process handle to a system command that is run, when a
	 * command and user name to run the command is specified.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param username
	 *            the system username to run the command under.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public abstract Process runProcBuilderSecurityGetProc(
			String[] commandArray, String username) throws Exception;

	/**
	 * Returns the Process handle to a system command that is run, when a
	 * command, user name, and environment variables to run the command is
	 * specified.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param username
	 *            the system username to run the command under.
	 * @param env
	 *            variables to set when running the command.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public abstract Process runProcBuilderSecurityGetProcWithEnv(
			String[] commandArray, String username, Map<String, String> env)
			throws Exception;

	/**
	 * Run a local system command using a ProcessBuilder.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilder(String[] commandArray) throws Exception {
		return runProcBuilder(commandArray, null);
	}

	/**
	 * Run a local system command using a ProcessBuilder.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param verbose
	 *            true for on, false for off. Default value is false.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilder(String[] commandArray, boolean verbose)
			throws Exception {
		return runProcBuilder(commandArray, null, verbose, false);
	}

	/**
	 * Run a system command with a ProcessBuilder, and get a Process handle in
	 * return.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * 
	 * @return Process the process handle for the system command.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public Process runProcBuilderGetProc(String[] commandArray)
			throws IOException {
		return runProcBuilderGetProc(commandArray, null);
	}

	/**
	 * Run a local system command with a ProcessBuilder, and additionally
	 * specify A set of environment variable definitions to run against the
	 * process.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param newEnv
	 *            a Map of environment variables and values to run as an
	 *            environment for the process to be run.
	 * @param verbose
	 *            true for on, false for off. Default value is false.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilder(String[] commandArray,
			Map<String, String> newEnv, boolean verbose) throws Exception {
		return runProcBuilder(commandArray, newEnv, verbose, false);
	}

	/**
	 * Run a local system command with a ProcessBuilder, and additionally
	 * specify A set of environment variable definitions to run against the
	 * process.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param newEnv
	 *            a Map of environment variables and values to run as an
	 *            environment for the process to be run.
	 * @param verbose
	 *            true for on, false for off. Default value is false.
	 * @param expect
	 *            failure - Default value is false.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilder(String[] commandArray,
			Map<String, String> newEnv, boolean verbose, boolean expectFailure)
			throws Exception {
		Process proc = null;
		int rc = 0;
		String output = null;
		String error = null;
		TestSessionCore.logger.trace(Arrays.toString(commandArray));
		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);

			if (verbose) {
				TestSessionCore.logger.debug("ProcessBuilder cmd='"
						+ pb.command() + "'");
				TestSessionCore.logger.info("cmd='"
						+ StringUtils.join(commandArray, " ") + "'");
			}

			Map<String, String> env = pb.environment();
			if (newEnv != null) {
				TestSessionCore.logger.trace("New Env = " + newEnv.toString());
				env.putAll(newEnv);
			}

			proc = pb.start();
			output = loadStream(proc.getInputStream());
			error = loadStream(proc.getErrorStream());

			rc = proc.waitFor();
			/*
			 * If the return code is non-zero, or standard error is not empty,
			 * then display logging.
			 */
			if (!expectFailure) {
				if ((rc != 0) || (((error != null) && !error.isEmpty()))) {
					if (rc != 0) {
						TestSessionCore.logger.warn("Process ended with rc='"
								+ rc + "'");
					}
					/*
					 * Print the command executed here only if verbose is false
					 * because we don't want to print this out twice.
					 */
					if (!verbose) {
						TestSessionCore.logger.debug("ProcessBuilder cmd='"
								+ pb.command() + "'");
						TestSessionCore.logger.info("cmd='"
								+ StringUtils.join(commandArray, " ") + "'");
					}
					if ((output != null) && !output.isEmpty()) {
						TestSessionCore.logger.warn("Captured stdout = '"
								+ output.trim() + "'");
					}
					if ((error != null) && !error.isEmpty()) {
						TestSessionCore.logger.warn("Captured stderr = '"
								+ error.trim() + "'");
					}
				}
			}
			TestSessionCore.logger.trace("Process Exit Code: '" + rc + "'");
			TestSessionCore.logger.trace("Process Stdout: '" + output + "'");
			TestSessionCore.logger.trace("Process Stderr: '" + error + "'");
		} finally {
			if (proc != null) {
				proc.destroy();
			}
		}

		return new String[] { Integer.toString(rc), output, error };
	}

	/**
	 * Run a local system command with a ProcessBuilder, and additionally
	 * specify A set of environment variable definitions to run against the
	 * process.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param newEnv
	 *            a Map of environment variables and values to run as an
	 *            environment for the process to be run.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilder(String[] commandArray,
			Map<String, String> newEnv) throws Exception {
		return runProcBuilder(commandArray, newEnv, true, false);
	}

	/**
	 * Run a system command with a ProcessBuilder, and get a Process handle in
	 * return. Additionally, specify environment variables to be processed along
	 * with the system command.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param newEnv
	 *            a Map of environment variables and values to run as an
	 *            environment for the process to be run.
	 * 
	 * @return Process the process handle for the system command.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public Process runProcBuilderGetProc(String[] commandArray,
			Map<String, String> newEnv) throws IOException {
		TestSessionCore.logger.trace(Arrays.toString(commandArray));
		TestSessionCore.logger.info("cmd='"
				+ StringUtils.join(commandArray, " ") + "'");
		Process proc = null;

		ProcessBuilder pb = new ProcessBuilder(commandArray);
		pb.redirectErrorStream(true);

		Map<String, String> env = pb.environment();
		if (newEnv != null) {
			TestSessionCore.logger.trace("New Env = " + newEnv.toString());
			env.putAll(newEnv);
		}
		proc = pb.start();

		return proc;
	}

	/**
	 * Run a local system command.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray)
			throws Exception {
		boolean verbose = true;
		return runProcBuilderSecurity(commandArray, verbose);
	}

	/**
	 * Run a local system command.
	 * 
	 * @param commandArray
	 *            the command to run. Each member of the string array should be
	 *            an item in the command string that is otherwise surrounded by
	 *            whitespace.
	 * @param verbose
	 *            true for on, false for off. Default value is false.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray,
			boolean verbose) throws Exception {
		return runProcBuilderSecurity(commandArray,
				System.getProperty("user.name"), verbose);
	}

	/**
	 * Run a local system command using runtime exec.
	 * 
	 * @param command
	 *            The system command to run.
	 * 
	 * @throws Exception
	 *             if the process can not be run.
	 */
	public static String runProc(String command) throws Exception {
		Process proc = null;
		TestSessionCore.logger.info(command);
		String output = null;
		try {
			proc = Runtime.getRuntime().exec(command);
			output = loadStream(proc.getInputStream());
		} finally {
			if (proc != null) {
				proc.destroy();
			}
		}
		return output;
	}

	/**
	 * Loads the provided inputstream to a BufferedReader and appends the output
	 * to the TestSessionCore logger.
	 * 
	 * @param is
	 *            the InputStream to process
	 * @return String the string output of the BufferedReader
	 * 
	 * @throws Exception
	 *             if the stream can not be read.
	 */
	protected static String loadStream(InputStream is) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null) {
			TestSessionCore.logger.trace(line);
			sb.append(line).append("\n");
		}
		return sb.toString();
	}

	/**
	 * Determines whether a username may be a headless user. If the username
	 * starts with "hadoop" it is assumed to be a headless user.
	 * 
	 * @param username
	 *            the username to process
	 * @return boolean whether the username is a headless username or not
	 */
	protected boolean isHeadless(String username) {
		if (username.startsWith("hadoop")
				|| username.equals(HadooptestConstants.UserNames.DFSLOAD)
				|| username.equals(HadooptestConstants.UserNames.HDFSQA)
				|| username.equals(HadooptestConstants.UserNames.MAPREDQA)) {
			return true;
		} else {
			return false;
		}

	}
}

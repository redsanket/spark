/*
 * YAHOO
 */

package hadooptest.cluster;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import hadooptest.TestSession;

/**
 * The base class for running system processes from the framework.
 * 
 * Subclasses of Executor should implement Executor for a specific
 * cluster type.  Currently, this is done to handle different levels
 * of Hadoop security for the different cluster types.
 */
public abstract class Executor {
	
	protected String CLUSTER_NAME;
	
	/**
	 * The constructor relies on the following framework configuration
	 * file key-value pairs to be defined.  They are initialized here.
	 * 
	 * CLUSTER_NAME - The name of the test cluster (this is typically
	 * used on fully distributed clusters only.  If undefined, it will
	 * default to using no name).
	 */
	public Executor() {
		CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME", "");
	}
	
	/**
	 * Returns the output of a system command, when given the command and a user to
	 * run the command as.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param username the system username to run the command under.
	 * @return String[] the output of running the system command.
	 */
	public abstract String[] runHadoopProcBuilder(String[] commandArray, String username);
	
	/**
	 * Run a local system command.
	 * 
	 * @param commandArray The system command to run.
	 * @param username the user to run the command as.
	 * @param verbose true for on, false for off. Default value is false.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray, String username, boolean verbose) {
		// The FullyDistributed package implements this to setup kerberos security,
		// but for Standalone we can just pass this through right to runProcBuilder
		// for now.
		return runProcBuilder(commandArray, verbose);
	}

	/**
	 * Returns the Process handle to a system command that is run, when a command and user
	 * name to run the command is specified.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param username the system username to run the command under.
	 * @return String[] the output of running the system command.
	 */
	public abstract Process runHadoopProcBuilderGetProc(String[] commandArray, String username);

	/**
	 * Run a local system command using a ProcessBuilder.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 */
	public String[] runProcBuilder(String[] commandArray) {
		return runProcBuilder(commandArray, null);
	}

	/**
	 * Run a local system command using a ProcessBuilder.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param verbose true for on, false for off. Default value is false.
	 */
	public String[] runProcBuilder(String[] commandArray, boolean verbose) {
		return runProcBuilder(commandArray, null, verbose);
	}

	/**
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * 
	 * @return Process the process handle for the system command.
	 */
	public Process runProcBuilderGetProc(String[] commandArray) {
		return runProcBuilderGetProc(commandArray, null);
	}
	
	/**
	 * Run a local system command with a ProcessBuilder, and additionally specify
	 * A set of environment variable definitions to run against the process.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param newEnv a Map of environment variables and values to run as an environment
	 * 						for the process to be run.
	 * @param verbose true for on, false for off. Default value is false.
	 */
	public String[] runProcBuilder(String[] commandArray, Map<String, String> newEnv, boolean verbose) {
		Process proc = null;
		int rc = 0;
		String output = null;
		String error = null;
		TestSession.logger.trace(Arrays.toString(commandArray));
		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);
			
			if (verbose) {
				TestSession.logger.debug("ProcessBuilder cmd='" + pb.command() + "'");
				TestSession.logger.info("cmd='" + StringUtils.join(commandArray, " ") + "'");
			}
			
			Map<String, String> env = pb.environment();
			if (newEnv != null) {
				env.putAll(newEnv);
			}
			
	        proc = pb.start();
	        output = loadStream(proc.getInputStream());
	        error = loadStream(proc.getErrorStream());
	        
	        rc = proc.waitFor();
	        if ((rc != 0) && (verbose)) {
	        	TestSession.logger.info("Process ended with rc='" + rc + "'");
	        	if ((output != null) && !output.isEmpty()) {
	        		TestSession.logger.info("Captured stdout = '" + output.trim() + "'");
	        	}
	        	if ((error != null) && !error.isEmpty()) {
	        		TestSession.logger.info("Captured stderr = '" + error.trim() + "'");
	        	}
	        }
	        TestSession.logger.trace("Process Exit Code: '" + rc + "'");
	        TestSession.logger.trace("Process Stdout: '" + output + "'");
	        TestSession.logger.trace("Process Stderr: '" + error + "'");
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		
		return new String[] { Integer.toString(rc), output, error};
	}
	
	/**
	 * Run a local system command with a ProcessBuilder, and additionally specify
	 * A set of environment variable definitions to run against the process.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param newEnv a Map of environment variables and values to run as an environment
	 * 						for the process to be run.
	 */
	public String[] runProcBuilder(String[] commandArray, Map<String, String> newEnv) {
		return runProcBuilder(commandArray, newEnv, true);
	}
	
	/**
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.  Additionally, specify environment
	 * variables to be processed along with the system command.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param newEnv a Map of environment variables and values to run as an environment
	 * 						for the process to be run.
	 * 
	 * @return Process the process handle for the system command.
	 */
	public Process runProcBuilderGetProc(String[] commandArray, Map<String, String> newEnv) {
		TestSession.logger.trace(Arrays.toString(commandArray));
		TestSession.logger.info("cmd='" + StringUtils.join(commandArray, " ") + "'");
		Process proc = null;

		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);
			pb.redirectErrorStream(true);
			
			Map<String, String> env = pb.environment();
			if (newEnv != null) {
				env.putAll(newEnv);
			}
			
	        proc = pb.start();
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		
		return proc;
	}
	
	/**
	 * Run a local system command.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray) {
		boolean verbose = true;
		return runHadoopProcBuilder(commandArray, verbose);
	}
	
	/**
	 * Run a local system command.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param verbose true for on, false for off. Default value is false.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray, boolean verbose) {
		return runHadoopProcBuilder(
				commandArray,
				System.getProperty("user.name"),
				verbose);
	}
	
	/**
	 * Run a local system command using runtime exec.
	 * 
	 * @param command The system command to run.
	 */
	public static String runProc(String command) {
		Process proc = null;
		TestSession.logger.info(command);
		String output = null;
		try {
			proc = Runtime.getRuntime().exec(command);
	        output = loadStream(proc.getInputStream());
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		return output;
	}	
	
	/**
	 * Loads the provided inputstream to a BufferedReader and appends
	 * the output to the TestSession logger.
	 * 
	 * @param is the InputStream to process
	 * @return String the string output of the BufferedReader
	 * @throws Exception
	 */
	protected static String loadStream(InputStream is) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); 
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
        	TestSession.logger.trace(line);
            sb.append(line).append("\n");
        }
        return sb.toString();
    }
	
	/**
	 * Determines whether a username may be a headless user.  If the username
	 * starts with "hadoop" it is assumed to be a headless user.
	 * 
	 * @param username the username to process
	 * @return boolean whether the username is a headless username or not
	 */
	protected boolean isHeadless(String username) {
		return (username.startsWith("hadoop")) ? true : false;
	}
}

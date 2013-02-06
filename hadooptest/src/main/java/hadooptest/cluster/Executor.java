/*
 * YAHOO
 */

package hadooptest.cluster;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

import hadooptest.TestSession;

public abstract class Executor {
	
	protected String HADOOP_VERSION;
	protected String HADOOP_INSTALL;
	protected String CONFIG_BASE_DIR;
	protected String CLUSTER_NAME;
	
	/*
	 * Class constructor.
	 */
	public Executor() {

		HADOOP_VERSION = TestSession.conf.getProperty("HADOOP_VERSION", "");
		HADOOP_INSTALL = TestSession.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TestSession.conf.getProperty("CONFIG_BASE_DIR", "");
		CLUSTER_NAME = TestSession.conf.getProperty("CLUSTER_NAME", "");
	}
	
	public abstract String[] runHadoopProcBuilder(String[] commandArray, String username);
	
	public abstract Process runHadoopProcBuilderGetProc(String[] commandArray, String username);

	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runProcBuilder(String[] commandArray) {
		return runProcBuilder(commandArray, null);
	}

	/*
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.
	 * 
	 * @param commandArray the string array containing the command to be executed.
	 * 
	 * @return Process the process handle for the system command.
	 */
	public Process runProcBuilderGetProc(String[] commandArray) {
		return runProcBuilderGetProc(commandArray, null);
	}
	
	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runProcBuilder(String[] commandArray, Map<String, String> newEnv) {
		TestSession.logger.info(Arrays.toString(commandArray));
		Process proc = null;
		int rc = 0;
		String output = null;
		String error = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);
			
			Map<String, String> env = pb.environment();
			if (newEnv != null) {
				env.putAll(newEnv);
			}
			
	        proc = pb.start();
	        output = loadStream(proc.getInputStream());
	        error = loadStream(proc.getErrorStream());
	        
	        rc = proc.waitFor();
	        if (rc != 0) {
	        	TestSession.logger.warn("Process ended with rc=" + rc);
	        }
	        // TSM.logger.debug("Process Stdout:" + output);
	        // TSM.logger.debug("Process Stderr:" + error);
		}
		catch (Exception e) {
			if (proc != null) {
				proc.destroy();
			}
			e.printStackTrace();
		}
		
		return new String[] { Integer.toString(rc), output, error};
	}
	
	/*
	 * Run a system command with a ProcessBuilder, and get a 
	 * Process handle in return.  Additionally, specify environment
	 * variables to be processed along with the system command.
	 * 
	 * @param commandArray the string array containing the command to be executed.
	 * @param newEnv the key value pair Map representing environment variables.
	 * 
	 * @return Process the process handle for the system command.
	 */
	public Process runProcBuilderGetProc(String[] commandArray, Map<String, String> newEnv) {
		TestSession.logger.info(Arrays.toString(commandArray));
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
	
	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runHadoopProcBuilder(String[] commandArray) {
		return runHadoopProcBuilder(
				commandArray,
				System.getProperty("user.name"));
	}
	
	/*
	 * Run a local system command.
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
	

	protected boolean isHeadless(String username) {
		return (username.startsWith("hadoop")) ? true : false;
	}
}

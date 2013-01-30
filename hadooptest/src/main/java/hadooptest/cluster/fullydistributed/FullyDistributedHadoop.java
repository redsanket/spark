/*
 * YAHOO
 */

package hadooptest.cluster.fullydistributed;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hadooptest.TestSession;

/*
 * A class which represents a fully distributed hadoop command
 */
public class FullyDistributedHadoop {
	
	private String HADOOP_VERSION;
	private String HADOOP_INSTALL;
	private String CONFIG_BASE_DIR;
	private String CLUSTER_NAME;
	
	private static TestSession TSM;
	
	/*
	 * Class constructor.
	 */
	public FullyDistributedHadoop(TestSession testSession) {
		
		TSM = testSession;

		HADOOP_VERSION = TSM.conf.getProperty("HADOOP_VERSION", "");
		HADOOP_INSTALL = TSM.conf.getProperty("HADOOP_INSTALL", "");
		CONFIG_BASE_DIR = TSM.conf.getProperty("CONFIG_BASE_DIR", "");
		CLUSTER_NAME = TSM.conf.getProperty("CLUSTER_NAME", "");
	}
	
	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String runProc(String command) {
		Process proc = null;
		TSM.logger.info(command);
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

	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runProcBuilder(String[] commandArray) {
		return runProcBuilder(commandArray, null);
	}

	/*
	 * Run a local system command.
	 * 
	 * @param command The system command to run.
	 */
	public String[] runProcBuilder(String[] commandArray, Map<String, String> newEnv) {
		TSM.logger.info(Arrays.toString(commandArray));
		Process proc = null;
		int rc = 0;
		String output = null;
		String error = null;
		try {
			ProcessBuilder pb = new ProcessBuilder(commandArray);

			// List<String> strList  = pb.command();
			// String[] strArray = strList.toArray(new String[0]);		
			// TSM.logger.info("PB command: " + Arrays.toString(strArray));
			
			Map<String, String> env = pb.environment();
			if (newEnv != null) {
				env.putAll(newEnv);
			}
			
	        proc = pb.start();
	        output = loadStream(proc.getInputStream());
	        error = loadStream(proc.getErrorStream());
	        
	        rc = proc.waitFor();
	        TSM.logger.debug("Process ended with rc=" + rc);
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
	public String[] runHadoopProcBuilder(String[] commandArray, String username) {
		if (this.isHeadless(username)) {
			Map<String, String> newEnv = new HashMap<String, String>();
			newEnv.put("KRB5CCNAME", this.obtainKerberosCache(username));
			return runProcBuilder(commandArray, newEnv);
		}
		else {
			return runProcBuilder(commandArray);			
		}
	}

	private static String loadStream(InputStream is) throws Exception {
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); 
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
 			TSM.logger.debug(line);
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

	
	private boolean isHeadless(String username) {
		return (username.startsWith("hadoop")) ? true : false;
	}
		
	private String obtainKerberosCache(String user) {
		TSM.logger.info("Setup Kerberos for user '"+user+"':");
		
		String realUser = System.getProperty("user.name");	
		String ticketDir = "/tmp/"+realUser+"/"+CLUSTER_NAME+"/kerberosTickets";		
		File file = new File(ticketDir);
		file.mkdirs();
		
		String keytabFileDir = (user.equals("hadoopqa")) ?
	             "/homes/"+user : "/homes/hdfsqa/etc/keytabs";				
		String keytabFile = user+".dev.headless.keytab";
		String kinitUser = (user.equals("hdfs")) ? 
				user+"/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM" : user;
		String cacheName = ticketDir+"/"+user+".kerberos.ticket";
		
		// e.g. kinit -c /tmp/hadoopqe/kerberosTickets/hadoop1.kerberos.ticket
		// -k -t /homes/hdfsqa/etc/keytabs/hadoop1.dev.headless.keytab hadoop1
	    String[] cmd =
	    		{ "/usr/kerberos/bin/kinit", "-c", cacheName, "-k","-t",
	    		keytabFileDir+"/"+keytabFile, kinitUser};
	    runProcBuilder(cmd);
	    return cacheName;
	}

	
}

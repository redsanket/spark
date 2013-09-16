/*
 * YAHOO
 */

package hadooptest.cluster.hadoop.fullydistributed;

import hadooptest.TestSession;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import coretest.cluster.Executor;

/**
 * A class which represents an Executor for a fully distributed cluster.
 * 
 * Handles all system calls for fully distributed clusters.
 */
public class FullyDistributedExecutor extends Executor {

	/**
	 * Returns the output of a system command, when given the command and a user to
	 * run the command as.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param username the system username to run the command under.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray, String username) 
			throws Exception {
		boolean verbose = true;
		return runProcBuilderSecurity(commandArray, username, verbose);
	}

	/**
	 * Returns the output of a system command, when given the command and a user to
	 * run the command as.
	 * 
	 * @param commandArray the command to run.  Each member of the string array should
	 * 						be an item in the command string that is otherwise
	 * 						surrounded by whitespace.
	 * @param username the system username to run the command under.
	 * @param verbose true for on, false for off. Default value is false.
	 * @return String[] the output of running the system command.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	public String[] runProcBuilderSecurity(String[] commandArray, String username, boolean verbose) 
			throws Exception {
		if (this.isHeadless(username)) {
			Map<String, String> newEnv = new HashMap<String, String>();
			newEnv.put("KRB5CCNAME", this.obtainKerberosCache(username, verbose));
			return runProcBuilder(commandArray, newEnv, verbose);
		}
		else {
			return runProcBuilder(commandArray, verbose);			
		}
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
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	public Process runProcBuilderSecurityGetProc(String[] commandArray, String username) 
			throws Exception {
		if (this.isHeadless(username)) {
			Map<String, String> newEnv = new HashMap<String, String>();
			newEnv.put("KRB5CCNAME", this.obtainKerberosCache(username));
			return runProcBuilderGetProc(commandArray, newEnv);
		}
		else {
			return runProcBuilderGetProc(commandArray);			
		}
	}

	/**
	 * Setup Kerberos authentication for a given user.
	 * 
	 * @param user the user to authenticate
	 * @param verbose true for on, false for off. Default value is false.
	 * @return String the Kerberos cache.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	private String obtainKerberosCache(String user, boolean verbose) 
			throws Exception {
		if (verbose) {
			TestSession.logger.info("Setup Kerberos for user '"+user+"':");
		}
		
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
        Map<String, String> newEnv = new HashMap<String, String>();
        newEnv.put("PATH", System.getenv("PATH") + 
                ":/usr/kerberos/bin/:/usr/local/bin:/usr/bin");
	    String[] cmd =
	    		{ "kinit", "-c", cacheName, "-k","-t",
	    		keytabFileDir+"/"+keytabFile, kinitUser};
        runProcBuilder(cmd, newEnv, verbose);
        TestSession.logger.debug("kerberos cachename KRB5CCNAME=" + cacheName);
	    return cacheName;
	}

	/**
	 * Setup Kerberos authentication for a given user.
	 * 
	 * @param user the user to authenticate
	 * @return String the Kerberos cache.
	 * 
	 * @throws Exception if there is a fatal error running the process.
	 */
	private String obtainKerberosCache(String user) 
			throws Exception {
		return obtainKerberosCache(user, true);
	}
}

/*
 * YAHOO
 */

package hadooptest.cluster.fullydistributed;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import hadooptest.TestSession;
import hadooptest.cluster.Executor;

/**
 * A class which represents a fully distributed hadoop command
 */
public class FullyDistributedExecutor extends Executor {

	/**
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
		if (this.isHeadless(username)) {
			Map<String, String> newEnv = new HashMap<String, String>();
			newEnv.put("KRB5CCNAME", this.obtainKerberosCache(username));
			return runProcBuilderGetProc(commandArray, newEnv);
		}
		else {
			return runProcBuilderGetProc(commandArray);			
		}
	}

	private String obtainKerberosCache(String user) {
		TestSession.logger.info("Setup Kerberos for user '"+user+"':");
		
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

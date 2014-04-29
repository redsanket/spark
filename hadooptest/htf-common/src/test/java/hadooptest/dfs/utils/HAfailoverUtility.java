package hadooptest.dfs.utils;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.HadoopCluster;
import hadooptest.cluster.hadoop.HadoopCluster.Action;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedCluster;
import hadooptest.workflow.hadoop.job.GenericJob;
import hadooptest.workflow.hadoop.job.JobState;
import hadooptest.node.hadoop.HadoopNode;
import hadooptest.automation.constants.HadooptestConstants;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Enumeration;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl; // H2.x, this test is 2.x only
import org.apache.hadoop.mapred.TaskReport;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class HAfailoverUtility {
				
	String adminHost = HadoopCluster.ADMIN;

	// failover an HA cluster, takes the cluster name and HA service (ha1 or ha2)
	// to failover to. Returns the currently active service (ha1 or ha2) or null
	// if failover attempt failed and could not fall back.
	public String failover(String cluster, String new_ha_service) throws Exception {
		String result = null;
		
		// get the RM host
		String rmHost = TestSession.cluster.getNodeNames(HadoopCluster.RESOURCE_MANAGER)[0];
        TestSession.logger.debug("rmHost is: "+ rmHost);
        // get ha1 namenode host
		String nnHost1 = TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
        TestSession.logger.debug("nnHost1 is: "+ nnHost1);
        // get ha2 namenode host
		String nnHost2 = TestSession.cluster.getNodeNames(HadoopCluster.SECONDARY_NAMENODE)[0];
        TestSession.logger.debug("nnHost2 is: "+ nnHost2);  
        
        // Check service states before trying to failover
        // get HA ServiceState on ha1
        String current_service_ha1 = getServiceState(cluster, nnHost1, "ha1");
        TestSession.logger.debug("Before failover attempt, current_service_ha1 is: "+current_service_ha1);
        // get HA ServiceState on ha2
        String current_service_ha2 = getServiceState(cluster, nnHost2, "ha2");
        TestSession.logger.debug("Before failover attempt, current_service_ha2 is: "+current_service_ha2);
        
        // this block defines the overall steps needed to support a failover, 
        // mainly toggling the network alias interfaces in the right order
        // before and after the actual failover attempt. 
        //
        // If the user wants to failover to ha1, we do this:
        //		take ha2 eth0:0 down
        //		make haadmin failover call
        //		bring ha1 eth0:0 up
        // If the user wants to failover to ha2;
        //		take ha1 eth0:0 down
        //		make haadmin failover call
        //		bring ha2 eth0:0 up
        if (new_ha_service.equals("ha1")){
        	// ifdown ha2
        	if (setNamenodeIfStatus(cluster, nnHost2, "down").equals("down")) {
        		TestSession.logger.info("Successfully ifdown interface eth0:0 on "+nnHost2);
        	} else {
        		TestSession.logger.error("Failed to ifdown interface eth0:0 on "+nnHost2);
        		//xx return "Failed to ifdown";
        	}
        	
        	// failover to ha1
        	if (attemptFailover(cluster, nnHost1, "ha1")) {
        		TestSession.logger.info("Successfully failed over to ha1");
        	} else {
        		TestSession.logger.error("Failed to fail over to ha1!");
        		//return "failed to fail over";
        	}
        		    	
        	// ifup ha1
        	if (setNamenodeIfStatus(cluster, nnHost1, "up").equals("up")) {
        		TestSession.logger.info("Successfully ifup interface eth0:0 on "+nnHost1);
        	} else {
        		TestSession.logger.error("Failed to ifup interface eth0:0 on "+nnHost1);
        		//xx  return "Failed to ifup";
        	}
        } else if (new_ha_service.equals("ha2")) {
        	// ifdown ha1
        	if (setNamenodeIfStatus(cluster, nnHost1, "down").equals("down")) {
        		TestSession.logger.info("Successfully ifdown interface eth0:0 on "+nnHost1);
        	} else {
        		TestSession.logger.error("Failed to ifdown interface eth0:0 on "+nnHost1);
        		//xx  return "Failed to ifdown";
        	}
        	// failover to ha2
        	if (attemptFailover(cluster, nnHost2, "ha2")) {
        		TestSession.logger.info("Successfully failed over to ha2");
        	} else {
        		TestSession.logger.error("Failed to fail over to ha2!");
        		//return "failed to fail over";
        	}
        	
        	// ifup ha2
        	if (setNamenodeIfStatus(cluster, nnHost2, "up").equals("up")) {
        		TestSession.logger.info("Successfully ifup interface eth0:0 on "+nnHost2);
        	} else {
        		TestSession.logger.error("Failed to ifup interface eth0:0 on "+nnHost2);
        		//xx  return "Failed to ifup";
        	}
        }

        
        // Check service states after trying to failover
        // get HA ServiceState on ha1
        current_service_ha1 = getServiceState(cluster, nnHost1, "ha1");
        TestSession.logger.debug("After failover attempt, current_service_ha1 is: "+current_service_ha1);
        // get HA ServiceState on ha2
        current_service_ha2 = getServiceState(cluster, nnHost2, "ha2");
        TestSession.logger.debug("After failover attempt, current_service_ha2 is: "+current_service_ha2);
        
        
        // verify state and return it
        if (new_ha_service.equals("ha1") && getServiceState(cluster, nnHost1, "ha1").contains("active")) {
        	TestSession.logger.info("ha1 service is ACTIVE");
        	return "ha1";
        } else if (new_ha_service.equals("ha2") && getServiceState(cluster, nnHost2, "ha2").contains("active")) {
           	TestSession.logger.info("ha2 service is ACTIVE");
        	return "ha2";
        } else {
        	return "ERROR";
        }
	}
	
	
	// utility method to get the namenode's ifconfig status, up or down,
	// this looks for the eth0:0 HA alias interface to either be there
	// or not, returns "up" if it is, "down" otherwise
	public String getNamenodeIfconfigStatus (String cluster, String nnHost) throws Exception {
		String status = "down";
		String pattern = "eth0:0";
		
		// build up the pdsh command to run from admin box to the NN
		String[] pdshCmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost,
    		"/sbin/ifconfig " };
		String output[] = TestSession.exec.runProcBuilder(pdshCmd);
		
		// send it
		String responseLine;
		Process p;
		try {
			// debugging
            for (String s: pdshCmd) {
            	TestSession.logger.debug("getNamenodeIfconfig PDSH_COMMAND being sent: "+s+" END");
            }// end
            
            p = Runtime.getRuntime().exec(pdshCmd);
            p.waitFor();

            BufferedReader r = new BufferedReader(new InputStreamReader(
                    p.getInputStream()));
            while ((responseLine = r.readLine()) != null) {
                    TestSession.logger.debug("RESPONSE_BEGIN:" + responseLine + "RESPONSE_END\n");
                    if (responseLine.contains(pattern)) {
                          status = "up";
                             break;
                    }
            }
		} catch (IOException e) {
            throw new RuntimeException(e);
		} catch (InterruptedException e) {
            e.printStackTrace();
		}
    
		return status;
		
	}

	
	// utility method to get the ha1 and ha2 service states, active or standby
	public String getNamenodeHAserviceState (String cluster, String nnHost, String service) throws Exception {
		String state = "unknown";
		//String pattern = "eth0:0";
		String hdfsqaKinit = "/usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM";
		
		
		// build up the remote node's command
		String[] cmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost, "\"", hdfsqaKinit, ";", 
				"/home/gs/gridre/yroot."+cluster+"/share/hadoop/bin/hdfs",
				"--config  /home/gs/gridre/yroot."+cluster+"/conf/hadoop/",
				"haadmin -getServiceState", service, "\""};
		String output[] = TestSession.exec.runProcBuilder(cmd);
		
		// send it
		String responseLine;
		Process p;
		try {
			// debugging
            for (String s: cmd) {
            	TestSession.logger.debug("getNamenodeIfconfig COMMAND being sent: "+s+" END");
            }// end
            p = Runtime.getRuntime().exec(cmd);
            p.waitFor();

            BufferedReader r = new BufferedReader(new InputStreamReader(
                    p.getInputStream()));
            while ((responseLine = r.readLine()) != null) {
                    TestSession.logger.debug("RESPONSE_BEGIN:" + responseLine + "RESPONSE_END\n");
                    if (responseLine.contains("standby"))
                    	state = "standby";
                    else if (responseLine.contains("active"))
                        state = "active";
                    else
                    	TestSession.logger.error("Unknown HA service state was received, " +
                    			"should be up or down, got:" + responseLine);
            }
		} catch (IOException e) {
            throw new RuntimeException(e);
		} catch (InterruptedException e) {
            e.printStackTrace();
		}
		   
		return state;
		
	}
	
	
	// utility method to set the namenode's HA net interface up or down,
	// using the eth0:0 HA alias. The relies on the yinst package
	// hadoop_qe_runasroot which allows root priv commands to be run 
	// through yinst, the ifup/ifdown commands are root only.
	// Bloody annoying, and slow, but we can't directly exec as root, so 
	// no other option for now
	public String setNamenodeIfStatus (String cluster, String nnHost, String state) throws Exception {
		String alias_net_interface = "eth0:0";
		
		String command;		
		if (state.equals("up")) {
			command = "ifup ";
		} else if (state.equals("down")) {
			command = "ifdown ";
		} else {
			TestSession.logger.error("Unknown network interface state, should be either up or down");
			command = "unknown";
		}
		
		// the yinst commands to clear any non-root user, send:
		//   if cmd (ifup/ifdown) 
		//   set args (the net interface to work on)
		//   exec the ifup/ifdown via yinst root auth
		//String yinst_unset = "yinst unset -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.user";
		//String yinst_cmd_to_send = "yinst set -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.command="+command;
		//String yinst_args_to_use = "yinst set -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.cmdoptsNargs="+alias_net_interface;
		//String yinst_exec = "yinst start -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot";		
		
		String[] yinst_cmd_set = { 
				"yinst unset -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.user",
				"yinst set -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.command="+command,
				"yinst set -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot.cmdoptsNargs="+alias_net_interface,
				"yinst start -root /home/gs/gridre/yroot."+cluster+" hadoop_qe_runasroot"
		};
	
		for (String s: yinst_cmd_set) {
			// build up each command to run from admin box to the NN
			String[] cmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost, s };
			String output[] = TestSession.exec.runProcBuilder(cmd);	
			
			// send it
			String responseLine;
			Process p;
			try {
				// debugging
	            for (String s2: cmd) {
	            	TestSession.logger.debug("setNamenodeIfStatusCOMMAND being sent: "+s2+" END");
	            }// end
	            
	            p = Runtime.getRuntime().exec(cmd);
	            p.waitFor();
	
	            BufferedReader r = new BufferedReader(new InputStreamReader(
	                    p.getInputStream()));
	            while ((responseLine = r.readLine()) != null) {
	                    TestSession.logger.debug("RESPONSE_BEGIN:" + responseLine + "RESPONSE_END\n");
	                    /*if (responseLine.contains(pattern)) {
	                          status = "up";
	                             break;
	                    }*/
	            }
			} catch (IOException e) {
	            throw new RuntimeException(e);
			} catch (InterruptedException e) {
	            e.printStackTrace();
			}
		}
    
		// verify state is correct
		if (state.equals("up") && getNamenodeIfconfigStatus(cluster, nnHost).equals("up")) {
			return "up";
		} else if (state.equals("down") && getNamenodeIfconfigStatus(cluster, nnHost).equals("down")) {
			return "down";
		} else {
			TestSession.logger.error("Failed to if"+state+" interface "+alias_net_interface+" on "+nnHost+"!");
			return "error";
		}
		
	}
	
	// utility method to get current service, in this case the nnHost is merely
	// where the client is run for the haadmin command, nnHost is not necessarily
	// the same as the HA service being checked (ha1 or ha2), returns service
	// state of "active" or "standby", or "unknown"
	// NOTE: this relies on the caller having  the hdfsqa credentials for the
	// haadmin command
	public String getServiceState(String cluster, String nnHost, String service) throws Exception {
		String state = "unknown";
		
		// build up pdsh 'haadmin -getServiceState' command
        String[] pdshCmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost,
        		"/home/gs/gridre/yroot."+cluster+"/share/hadoop/bin/hdfs ",
        		" --config /home/gs/gridre/yroot."+cluster+"/conf/hadoop " +
        		" haadmin -getServiceState ", service };
        String output[] = TestSession.exec.runProcBuilderSecurity(pdshCmd, "hdfsqa");
        // send it out
        String responseLine;
        Process p;
        try {

                for (String s: pdshCmd) {
                	TestSession.logger.debug("PDSH_COMMAND being sent: "+s+" END");
                }
                
                p = Runtime.getRuntime().exec(pdshCmd);
                p.waitFor();

                BufferedReader r = new BufferedReader(new InputStreamReader(
                        p.getInputStream()));
                while ((responseLine = r.readLine()) != null) {
                        TestSession.logger.debug("RESPONSE_BEGIN:" + responseLine + "RESPONSE_END\n");
                        if (responseLine.contains("active") || responseLine.contains("standby")) {
                              state = responseLine;
                        }
                }
        } catch (IOException e) {
                throw new RuntimeException(e);
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
		
		return state;
	}
		
	
    // utility method for doing the actual failover   
	protected boolean attemptFailover(String cluster, String nnHost, String new_ha_service) throws Exception {
    	
		boolean result=false;
		String hdfsqaKinit = "/usr/kerberos/bin/kinit -k -t /homes/hdfsqa/etc/keytabs/hdfs.dev.headless.keytab hdfs/dev.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM";

		// haadmin -failover now requires that the new service be different from the
		// new service, so we force the toggle. Open question; this masks negative
		// test cases that would try to send incorrect args, maybe add poly methods
		// that allow bad args?
		String old_ha_service;
		if (new_ha_service.equals("ha1")) 
			 old_ha_service = "ha2";
		else if (new_ha_service.equals("ha2")) 
			 old_ha_service = "ha1";
		else {
			old_ha_service = "none";
			TestSession.logger.error("Got an invalid service name: "+new_ha_service);
			return result;
		}
		
		
		// build up the remote node's command
		String[] cmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost, "\"", 
		//String[] cmd = {"ssh", adminHost, "pdsh", "-t", "5", "-w", nnHost, "\"", hdfsqaKinit, ";",
				"/home/gs/gridre/yroot."+cluster+"/share/hadoop/bin/hdfs",
				"--config  /home/gs/gridre/yroot."+cluster+"/conf/hadoop/",
				"haadmin -failover", old_ha_service, new_ha_service, "\""};
		String output[] = TestSession.exec.runProcBuilderSecurity(cmd, "hdfsqa");
		
        // send it out
        String responseLine;
        Process p;
        try {

                for (String s: cmd) {
                	TestSession.logger.debug("FAILOVER_COMMAND being sent: "+s+" END");
                }
                
                p = Runtime.getRuntime().exec(cmd);
                p.waitFor();

                BufferedReader r = new BufferedReader(new InputStreamReader(
                        p.getInputStream()));
                while ((responseLine = r.readLine()) != null) {
                        TestSession.logger.debug("FAILOVER_RESPONSE:" + responseLine + "RESPONSE_END\n");
                        if (responseLine.contains("successful")) {
                              result = true;
                              break;
                        }
                }
        } catch (IOException e) {
                throw new RuntimeException(e);
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
	
        
       
		return result;
    }
		
}

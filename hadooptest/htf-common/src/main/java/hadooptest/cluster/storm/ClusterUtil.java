package hadooptest.cluster.storm;

import hadooptest.TestSessionStorm;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * The current storm cluster, assumed to be installed through yinst.
 */
public class ClusterUtil {
    private String namespace = "ystorm";

    private Map<String, Map<String, String>> origConf;
    private boolean confChanged;

    public ClusterUtil(String confNS) {
    	try {
    		init(confNS);
    	}
    	catch (Exception e) {
    		TestSessionStorm.logger.info(
    				"ClusterUtil: Caught excpetion: " + e.getMessage());
    	}
    }
    
    public boolean changed() {
        return confChanged;
    }

    public void init(String confNS) 
    		throws Exception {
    	
    	TestSessionStorm.logger.info("INIT CLUSTER UTIL");
        namespace = confNS;
        origConf = getYinstConf();
        confChanged = false;
    }

    /**
     * Get the Map of yinst configuration for all nodes in a Storm cluster.
     * 
     * @return A Map containing keys of the DNS names of the cluster nodes,
     *         and elements corresponding to Maps of the yinst configuration
     *         on each node.
     *         
     * @throws Exception
     */
    public Map<String, Map<String, String>> getYinstConf() 
    		throws Exception {

        Map<String, Map<String, String>> ret = 
        		new HashMap<String, Map<String, String>>();
        
    	ArrayList<String> dnsNames = null;
    	
    	if (namespace.equals("ystorm_registry")) {
    	    dnsNames = StormDaemon.lookupIgorRoles(StormDaemon.REGISTRY, 
    	            TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	}
    	else {
    	    dnsNames = 
                StormDaemon.lookupIgorRoles(StormDaemon.ALL, 
                        TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	}

    	TestSessionStorm.logger.info(
    			"*** GETTING YINST CONFIGURATION FOR ALL STORM NODES ***");
    	
        Process p = null;
        BufferedReader reader;
        String line;
        Map<String, String> nodeYinst;
        for (String nodeDNSName: dnsNames) {
        	TestSessionStorm.logger.info(
        			"*** GETTING YINST CONFIGURATION FOR NODE:  " + 
        					nodeDNSName + " ***");
        	
        	try {
        		nodeYinst = new HashMap<String, String>();
        		
        		p = TestSessionStorm.exec.runProcBuilderGetProc(
        				new String[] {
        						"ssh", 
        						nodeDNSName, 
        						"yinst", 
        						"set", 
        						namespace } );
        		reader = new BufferedReader(new InputStreamReader(
        				p.getInputStream()));
        		line = reader.readLine();

        		while (line != null) {
                    TestSessionStorm.logger.debug(line);
                    
        			String[] parts = line.split(": ", 2);
        			if (parts.length == 2) {
        				nodeYinst.put(parts[0], parts[1]);
        				ret.put(nodeDNSName, nodeYinst);
        			}
        			
                    line = reader.readLine();
        		}
        	}
        	catch (Exception e) {
        		if (p != null) {
        			p.destroy();
        		}

        		TestSessionStorm.logger.error("Exception " + e.getMessage(), e);
        		throw e;
        	}
        }

        return ret;
    }

    /**
     * Reset the yinst configuration for every node in a Storm cluster to its
     * default configuration.
     * 
     * @throws Exception
     */
    public void resetConfigs() throws Exception {
    	TestSessionStorm.logger.info(
    			"*** RESET CONFIGS FOR ALL STORM NODES AND RESTART ***");
    	
        if (!confChanged) {
            return;
        }

        // Get the current yinst state on all nodes.
        Map<String, Map<String, String>> allConfigCurrent = getYinstConf();
        
        // For each node in the current yinst state Map...
        for (Map.Entry<String, 
        		Map<String, String>> nodewiseNameConfCurrent: 
        			allConfigCurrent.entrySet()) {
        	
        	// Get the original yinst conf for this node...
        	Map<String, String> nodewiseConfOrig = 
        			origConf.get(nodewiseNameConfCurrent.getKey());
        	// Get the current yinst conf for this node...
        	Map<String, String> nodewiseConfCurrent = 
        			nodewiseNameConfCurrent.getValue();
        	
        	TestSessionStorm.logger.info("*** CURRENT CONF FOR THIS NODE ***");
        	for (String key: nodewiseConfCurrent.keySet()) {
        		TestSessionStorm.logger.info("CURRENT KEY = " + key);
        	}
        	
        	TestSessionStorm.logger.info("*** ORIGINAL CONF FOR THIS NODE ***");
        	for (String oKey: nodewiseConfOrig.keySet()) {
        		TestSessionStorm.logger.info("ORIG KEY = " + oKey);
        	}
        	
        	// Get a hash of all of the keys in the current yinst conf for this
        	// node.
            HashSet<String> toRemove = 
            		new HashSet<String>(nodewiseConfCurrent.keySet());            
            
            // Remove all keys that are already in the original configuration.
            toRemove.removeAll(nodewiseConfOrig.keySet());
            
            TestSessionStorm.logger.info("*** TOREMOVE CONTAINS *** " + toRemove.toString());

            // If there were new keys added that we now need to remove, yinst
            // unset those keys.
            if (!toRemove.isEmpty()) {
                ArrayList<String> unsetCmd = new ArrayList<String>();
                unsetCmd.add("ssh");
                unsetCmd.add(nodewiseNameConfCurrent.getKey());
                unsetCmd.add("yinst");
                unsetCmd.add("unset");
                unsetCmd.addAll(toRemove);
                
                TestSessionStorm.logger.info("Running " + unsetCmd);
                
                String[] unsetCommand = new String[unsetCmd.size()];
                unsetCommand = unsetCmd.toArray(unsetCommand);
                
            	String[] output = 
            			TestSessionStorm.exec.runProcBuilder(unsetCommand);
            	
        		if (!output[0].equals("0")) {
        			TestSessionStorm.logger.info(
        					"Got unexpected non-zero exit code: " + output[0]);
        			TestSessionStorm.logger.info("stdout" + output[1]);
        			TestSessionStorm.logger.info("stderr" + output[2]);	
                    throw new RuntimeException(
                    		"ssh and yinst returned an error code.");		
        		}
            }
            
            TestSessionStorm.logger.info(
            		"*** RESETTING YINST CONFIGURATION FOR NODE: " + 
            				nodewiseNameConfCurrent.getKey() + " ***");
            
            // Now, yinst set the original conf back on the node.
            ArrayList<String> setCmd = new ArrayList<String>();
            setCmd.add("ssh");
            setCmd.add(nodewiseNameConfCurrent.getKey());
            setCmd.add("yinst");
            setCmd.add("set");
            
            // Add each original key-value pair to the yinst set command.
            for (Map.Entry<String,String> entry: nodewiseConfOrig.entrySet()) {
                if (entry.getValue().contains(" ")) {
                    setCmd.add(entry.getKey()+"="+"\""+entry.getValue()+"\"");
                } else {
                    setCmd.add(entry.getKey()+"="+entry.getValue());
                }
            }

            String[] setCommand = new String[setCmd.size()];
            setCommand = setCmd.toArray(setCommand);
            
        	String[] output = TestSessionStorm.exec.runProcBuilder(setCommand);
        	
    		if (!output[0].equals("0")) {
    			TestSessionStorm.logger.info(
    					"Got unexpected non-zero exit code: " + output[0]);
    			TestSessionStorm.logger.info("stdout" + output[1]);
    			TestSessionStorm.logger.info("stderr" + output[2]);	
                throw new RuntimeException(
                		"ssh and yinst returned an error code.");		
    		}
        }
        
        confChanged = false;
    }

    /**
     * Unset a yinst configuration variable on all nodes in a Storm cluster.
     * 
     * @param key The key to unset.
     * 
     * @throws Exception
     */
    public void unsetConf(String key) throws Exception {
    	
    	ArrayList<String> dnsNames = 
    			StormDaemon.lookupIgorRoles(StormDaemon.ALL, 
    					TestSessionStorm.conf.getProperty("CLUSTER_NAME"));
    	
    	TestSessionStorm.logger.info("Unsetting " + key);
    	TestSessionStorm.logger.info(
    			"*** UNSETTING CONFIGURATION FOR ALL STORM NODES:  " + 
    					key + " ***");

		confChanged = true;
		String strKey = namespace+"."+key.replace('.','_');
    	
    	for (String nodeDNSName: dnsNames) {
        	TestSessionStorm.logger.info(
        			"*** UNSETTING CONFIGURATION FOR NODE:  " + nodeDNSName + 
        				" - KEY:  " + key + " ***");
        	
        	String[] output = TestSessionStorm.exec.runProcBuilder(
        			new String[] {"ssh", nodeDNSName, "yinst", "unset", 
        					strKey } );
        	
    		if (!output[0].equals("0")) {
    			TestSessionStorm.logger.info(
    					"Got unexpected non-zero exit code: " + output[0]);
    			TestSessionStorm.logger.info("stdout" + output[1]);
    			TestSessionStorm.logger.info("stderr" + output[2]);	
                throw new RuntimeException(
                		"ssh and yinst returned an error code.");		
    		}
    	}
    }

    /**
     * Set a yinst configuration variable on all nodes in a Storm cluster.
     * 
     * @param key The key to set.
     * @param value The value of the key to set.
     * 
     * @throws Exception
     */
    public void setConf(String key, Object value) throws Exception {
    	TestSessionStorm.logger.info("Setting " + key + "=" + value);
    	TestSessionStorm.logger.info(
    			"*** SETTING CONFIGURATION FOR ALL STORM NODES:  " + 
    					key + " ***");

        confChanged = true;
        String strVal = value.toString();
        
        if (value instanceof Map) {
        	StringBuffer b = new StringBuffer();
        	
        	for (Map.Entry<?,?> entry: ((Map<?,?>)value).entrySet()) {
        		if (b.length() != 0) {
        			b.append(",");
        		}
        		
        		b.append(entry.getKey().toString());
        		b.append(",");
        		b.append(entry.getValue().toString());
        	}
        	
        	strVal = b.toString();	
        } 
        else if (value instanceof Iterable) {
        	StringBuffer b = new StringBuffer();
        	
        	for (Object o: ((Iterable<?>)value)) {
        		if (b.length() != 0) {
        			b.append(",");
        		}
        		
        		b.append(o.toString());
        	}
        	
        	strVal = b.toString();
        }

        String strKey = namespace+"."+key.replace('.','_');

    	ArrayList<String> dnsNames = 
    			StormDaemon.lookupIgorRoles(StormDaemon.ALL, 
    					TestSessionStorm.conf.getProperty("CLUSTER_NAME"));

    	for (String nodeDNSName: dnsNames) {   
        	TestSessionStorm.logger.info(
        			"*** SETTING CONFIGURATION FOR NODE:  " + nodeDNSName + 
        				" - KEY:  " + strKey + " - VALUE:  " + strVal + " ***");
        	
        	String[] output = TestSessionStorm.exec.runProcBuilder(
        			new String[] {"ssh", nodeDNSName, "yinst", "set", 
        					strKey+"="+strVal } );
        	
    		if (!output[0].equals("0")) {
    			TestSessionStorm.logger.info(
    					"Got unexpected non-zero exit code: " + output[0]);
    			TestSessionStorm.logger.info("stdout" + output[1]);
    			TestSessionStorm.logger.info("stderr" + output[2]);	
    		}
    	}
    }
}

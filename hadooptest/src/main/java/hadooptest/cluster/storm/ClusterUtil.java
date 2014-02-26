package hadooptest.cluster.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import hadooptest.ConfigProperties;

import org.apache.thrift7.TException;


/**
 * The current storm cluster, assumed to be installed through yinst.
 */
public class ClusterUtil {
    private String namespace = "ystorm";

    private Map<String,String> origConf;
    private boolean confChanged;

    public boolean changed() {
        return confChanged;
    }

    public void init(String confNS) throws Exception {
        System.err.println("INIT CLUSTER UTIL");
	namespace = confNS;
        origConf = getYinstConf();
        confChanged = false;
    }

    public ClusterUtil(String confNS) {
	try {
            init(confNS);
	} catch (Exception e) {
		System.err.println("ClusterUtil:  Caught excpetion: " + e.getMessage());
	}
    }

    public Map<String,String> getYinstConf() throws Exception {
        //TODO save stats for multiple nodes
        Map<String,String> ret = new HashMap<String,String>();
        ProcessBuilder pb = new ProcessBuilder("yinst", "set", namespace);
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);
        Process p = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(": ", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Error parsing yinst output "+line);
            }
            ret.put(parts[0], parts[1]);
        }
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        return ret;
    }

    public void resetConfigs() throws Exception {
        System.err.println("RESET CONFIGS AND RESTART");
        if (!confChanged) {
            return;
        }
        //TODO allow this to run on multiple nodes
        Map<String,String> current = getYinstConf();
        HashSet<String> toRemove = new HashSet<String>(current.keySet());
        toRemove.removeAll(origConf.keySet());

        if (!toRemove.isEmpty()) {
            List<String> command = new ArrayList<String>();
            command.add("yinst");
            command.add("unset");
            command.addAll(toRemove);
            System.err.println("Running "+command);
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.inheritIO();
            Process p = pb.start();
            if (p.waitFor() != 0) {
                throw new RuntimeException("yinst returned an error code "+p.exitValue());
            }
        }

        List<String> command = new ArrayList<String>();
        command.add("yinst");
        command.add("set");
        for (Map.Entry<String,String> entry: origConf.entrySet()) {
            command.add(entry.getKey()+"="+entry.getValue());
        }
        System.err.println("Running "+command);
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
        confChanged = false;
    }

    public void unsetConf(String key) throws Exception {
        System.err.println("Unsetting "+key);
        confChanged = true;
        String strKey = namespace+"."+key.replace('.','_');
        //TODO for all of the nodes (May want something that allows for a specific class of node to be set)
        ProcessBuilder pb = new ProcessBuilder("yinst","unset",strKey);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
    }

    public void setConf(String key, Object value) throws Exception {
        System.err.println("setting "+key+"="+value);
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
        } else if (value instanceof Iterable) {
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
        //TODO for all of the nodes (May want something that allows for a specific class of node to be set)
        ProcessBuilder pb = new ProcessBuilder("yinst","set",strKey+"="+strVal);
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("yinst returned an error code "+p.exitValue());
        }
    }
}

package hadooptest.gdm.regression.stackIntegration.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.commons.lang3.StringUtils;

import hadooptest.cluster.gdm.ConsoleHandle;

public class CheckDistedHadoopVersion {
    private String replicationHostName;
    private String nameNodeName;
    private String hadoopVersion;
    private CommonFunctions commonFunctions = null;
    private ConsoleHandle consoleHandle = null;
    private String clusterName;
    private String latestExistingHadoopVersion = null;
    private static final String GDM_HADOOP_CONFIGS = "/home/y/libexec/gdm_hadoop_configs/";
    
    public CheckDistedHadoopVersion(String clusterName , String replicationHostName) {
	this.commonFunctions = new CommonFunctions();
	this.consoleHandle = new ConsoleHandle();
	this.setClusterName(clusterName);
	this.setReplicationHostName(replicationHostName);
    }

    public String getReplicationHostName() {
	return replicationHostName;
    }
    
    public void setReplicationHostName(String replicationHostName) {
	this.replicationHostName = replicationHostName;
    }
    
    public String getNameNodeName() {
	return nameNodeName;
    }
    
    public void setNameNodeName(String nameNodeName) {
	this.nameNodeName = nameNodeName;
    }
    
    public String getHadoopVersion() {
	return hadoopVersion;
    }
    
    public void setHadoopVersion(String hadoopVersion) {
	this.hadoopVersion = hadoopVersion;
    }
    
    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
    
    public String getLatestExistingHadoopVersion() {
        return latestExistingHadoopVersion;
    }

    public void setLatestExistingHadoopVersion(String latestExistingHadoopVersion) {
        this.latestExistingHadoopVersion = latestExistingHadoopVersion;
    }

    public void fetchHostNames() throws InterruptedException, ExecutionException {
	List<Callable<String>> list = new ArrayList<Callable<String>>();
/*	Callable<String> replicationHostCallable = ()->{
	    String replHostName = this.consoleHandle.getFacetHostName("replication" , "blue" , "gq1");
	    return "replicationHostName-" + replHostName;
	};*/

	Callable<String> nameNodeCallable = ()->{
	    String command = "yinst range -ir \"(@grid_re.clusters." + this.getClusterName() + "." + "namenode" +")\"";
	    String nnHostName = this.commonFunctions.executeCommand(command);
	    return "nameNodeHostName-" +  nnHostName;
	};
	
	list.add(nameNodeCallable);
	
	ExecutorService executor = Executors.newFixedThreadPool(2);
	List<Future<String>> hostNamesList = executor.invokeAll(list);
	for ( Future<String> hostNames : hostNamesList) {
	    String hostName = hostNames.get();
	    if (hostName.startsWith("replicationHostName")) {
		String temp = hostName.substring("replicationHostName-".length() + 1, hostName.length());
		this.setReplicationHostName(temp);
	    }
	    if (hostName.startsWith("nameNodeHostName-")) {
		String temp = hostName.substring("nameNodeHostName-".length() + 1 , hostName.length());
		this.setNameNodeName(temp);
	    }
	}
	executor.shutdown();
    }
    
    public boolean checkClusterHadoopVersionAndReplDistedVersionMatches() {
	String cmd1 = "ssh " +  this.getReplicationHostName() + " \"ls -dt " + GDM_HADOOP_CONFIGS + this.getClusterName() ;
	String command = cmd1 + "/*  | head -n 1 \"";
	boolean flag = false;
	for ( int i = 1; i <=10 ; i++) {
	    try {
		System.out.println("Wait for some time so that latest hadoop jar is fetched.");
		Thread.sleep(30000);
		String latestFolderTemp = this.commonFunctions.executeCommand(command);
		List<String> latestFolderValueList = Arrays.asList(latestFolderTemp.split("\n"));
		String latestFolder = "";
		for ( String str : latestFolderValueList) {
		    if ( str.startsWith(GDM_HADOOP_CONFIGS)){
			latestFolder = str.trim();
		    }
		}
		if (StringUtils.isNotBlank(latestFolder)) {
		    String latestHadoopVersion = this.commonFunctions.executeCommand("ssh " + this.getReplicationHostName() + "  \"" + latestFolder  + "/hadoopcoretree/share/hadoop-*\"");
		    System.out.println("installed hadoop version - " + latestHadoopVersion);
		    this.setLatestExistingHadoopVersion(latestHadoopVersion);
		    if ( latestHadoopVersion.indexOf(this.getHadoopVersion()) > -1 ) {
			flag = true;
			break;
		    } else {
			continue;
		    }    
		}
		
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}
	return flag;
    }

}

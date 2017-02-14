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
    
    public CheckDistedHadoopVersion(String clusterName , String replicationHostName) throws InterruptedException, ExecutionException {
	this.commonFunctions = new CommonFunctions();
	this.consoleHandle = new ConsoleHandle();
	this.setClusterName(clusterName);
	this.setReplicationHostName(replicationHostName);
	//getNameNodeHostName();
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

    public void getNameNodeHostName() throws InterruptedException, ExecutionException {
	String command = "yinst range -ir \"(@grid_re.clusters." + this.getClusterName() + "." + "namenode" +")\"";
	String nnHostName = this.commonFunctions.executeCommand(command);
	this.setNameNodeName(nnHostName);
	System.out.println("getNameNodeHostName() - namenodeHost Name - " + this.getNameNodeName());
    }
    
    public void getClustterHadoopVersion() throws InterruptedException, ExecutionException {
	String command = "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null  " + this.getNameNodeName().trim() +  "  \"" + " hadoop version\"";
	String hadoopVersion = this.commonFunctions.executeCommand(command);
	List<String> versionList = Arrays.asList(hadoopVersion.split("\n"));
	for ( String str : versionList) {
	    if (str.startsWith("Hadoop")) {
		List<String> tempList = Arrays.asList(str.split(" "));
		if (tempList.size() > 0) {
		    this.setHadoopVersion(tempList.get(1));
		}
	    }
	}
	System.out.println("getHadoopVersion() - namenodeHost Name - " + this.getHadoopVersion());
    }
    
    
    public boolean setYinstSettingAndRestartFacet() {
	String yinstSetting = "\"yinst set ygrid_gdm_replication_server.hadoop_config_watcher_frequency_seconds=120";
	String restartCommand = "yinst restart ygrid_gdm_replication_server\"";
	this.commonFunctions.executeCommand("ssh " + this.getReplicationHostName().trim() + "   " + yinstSetting + ";" + restartCommand );
	
	try {
	    for ( int i =1 ;i <10;i++) {
		System.out.println("Please wait for some time so that replication facet comes up. " + i);
		Thread.sleep(30000);
	    }
	} catch (InterruptedException e) {
	    e.printStackTrace();
	}
	
	int i = 1;
	boolean flag = false;
	while (i <= 10) {
	    if (this.consoleHandle.isFacetRunning("replication", "blue", "gq1") == true){
		flag = true;
		break;
	    }
	}
	return flag;
    }
    
    public boolean checkClusterHadoopVersionAndReplDistedVersionMatches() throws InterruptedException, ExecutionException {
	getNameNodeHostName();
	getClustterHadoopVersion();
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
		    String latestHadoopVersion = this.commonFunctions.executeCommand("ssh " + this.getReplicationHostName() + "  \" ls -dt  " + latestFolder  + "/hadoopcoretree/share/hadoop-*\"");
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
    
    public void restartFacet() {
	
    }

}

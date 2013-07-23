package hadooptest.cluster;

import hadooptest.TestSession;

import hadooptest.cluster.hadoop.DFS;

public class MultiClusterProtocol {
	 
    public String processInput(String theInput) {
        String theOutput = null;
        
        if (theInput != null) {
        	if (theInput.equals("Hi, this is a multi cluster client.")) {
        		theOutput = "Hello there.  I am the multi cluster server.";
        	}
        	
        	if (theInput.contains("CLIENT HADOOP VERSION = ")) {
        		theOutput = "Thanks.  I recieved your Hadoop version. " +
        				"Server Hadoop version is: " + 
        				TestSession.cluster.getVersion();
        	}
        	
        	
        	//if (theInput.contains("DFS_COPY")) {
        	//	try {
        	//		DFS dfs = new DFS();
        	//	
        	//		theOutput = "DFS_COPY_RESULT = " + dfs.copyDfsToDfs(srcClusterBasePath, srcPath, destClusterBasePath, destPath);
        	//	}
        	//	catch (Exception e) {
        	//		theOutput = "DFS_COPY_RESULT = EXCEPTION: " + e.getMessage();
        	//	}
        	//}
        }
        
        return theOutput;
    }

}

package hadooptest.cluster;

import coretest.Util;
import hadooptest.TestSession;

import hadooptest.cluster.hadoop.DFS;

public class MultiClusterProtocol {
	 
	public String clientVersion = "";
	public String clientDFSName = "";
	
    public String processInput(String theInput) {
        String theOutput = null;
        
        if (theInput != null) {
        	/* Server response protocols */
        	if (theInput.equals("Hi, this is a multi cluster client.")) {
        		theOutput = "Hello there.  I am the multi cluster server.";
        	}
        	else if (theInput.contains("CLIENT HADOOP VERSION = ")) {
        		theOutput = "Thanks.  I recieved your Hadoop version. " +
        				"Server Hadoop version is: " + 
        				TestSession.cluster.getVersion();
        	}
        	else if (theInput.contains("CLIENT HADOOP VERSION = ")) {
				clientVersion = theInput;
			}
        	else if (theInput.contains("CLIENT DFS DEFAULT NAME = ")) {
				clientDFSName = theInput.substring(theInput.indexOf("= ") + 2);
				TestSession.logger.info("Client DFS Name response is: " + clientDFSName);
			}
        	else if (theInput.contains("DFS_REMOTE_LOCAL_COPY")) {
				theOutput = "DFS_COPY_LOCAL /homes/hadoopqa/hadooptest.conf " + clientDFSName + "/user/hadoopqa/hadooptest.conf";
        	}
        	else if (theInput.contains("DFS_REMOTE_DFS_COPY")) {
				theOutput = "DFS_COPY " + clientDFSName + " /user/hadoopqa/hadooptest.conf " + TestSession.cluster.getConf().get("fs.defaultFS") + " /user/hadoopqa/hadooptest.conf.test.1";
			}
        	
        	/* Client response protocols */
        	else if (theInput.equals("RETURN_VERSION")) {
				theOutput = "CLIENT HADOOP VERSION = " + TestSession.cluster.getVersion();
			}
			else if (theInput.equals("CLUSTER_STOP")) {
				// Stop the cluster here
				theOutput = "I got the request to stop the cluster.";
			}
			else if (theInput.contains("DFS_GET_DEFAULT_NAME")) {
        		theOutput = "CLIENT DFS DEFAULT NAME = " + TestSession.cluster.getConf().get("fs.defaultFS");
        	}
			else if (theInput.contains("DFS_COPY_LOCAL ")) {
				try {
					DFS dfs = new DFS();
					
					String[] fromServerSplit = theInput.split(" ");
					String src = fromServerSplit[1];
					String dest = fromServerSplit[2];
					
					dfs.copyLocalToHdfs(src, dest);
					dfs.printFsLs(dest, true);
					theOutput = "DFS_COPY_LOCAL_RESULT = true";
				}
				catch (Exception e) {
					theOutput = "DFS_COPY_LOCAL_RESULT = EXCEPTION: " + e.getMessage();
				}
			}
			else if (theInput.contains("DFS_COPY ")) {
        		try {
        			DFS dfs = new DFS();
        			
        			String[] fromServerSplit = theInput.split(" ");
        			String srcClusterBasePath = fromServerSplit[1];
        			String srcPath = fromServerSplit[2];
        			String destClusterBasePath = fromServerSplit[3];
        			String destPath = fromServerSplit[4];
        		
        			theOutput = "DFS_COPY_RESULT = " + dfs.copyDfsToDfs(srcClusterBasePath, srcPath, destClusterBasePath, destPath);
        		}
        		catch (Exception e) {
        			theOutput = "DFS_COPY_RESULT = EXCEPTION: " + e.getMessage();
        		}
        	}
        }
        
        return theOutput;
    }

}

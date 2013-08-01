package hadooptest.cluster;

import java.io.IOException;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;

public class MultiClusterProtocol {
	 
	public String clientVersion = "";
	public String clientDFSName = "";
	public boolean fileExists = false;
	
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
				theOutput = "Thanks, I got your Hadoop version number.";
			}
        	else if (theInput.contains("CLIENT DFS DEFAULT NAME = ")) {
				clientDFSName = theInput.substring(theInput.indexOf("= ") + 2);
				TestSession.logger.info("Client DFS Name response is: " + clientDFSName);
				theOutput = "Thanks, I got your DFS Name.";
			}
        	else if (theInput.contains("DFS_REMOTE_LOCAL_COPY")) {
        		String[] fromInputSplit = theInput.split(" ");
        		String src = fromInputSplit[1];
        		String dest = fromInputSplit[2];
        		
				theOutput = "DFS_COPY_LOCAL " + src + " " + dest;
        	}
        	else if (theInput.contains("DFS_REMOTE_DFS_LS_FILE_EXISTS")) {
        		fileExists = false;
        		String [] fromInputSplit = theInput.split(" ");
        		String path = fromInputSplit[1];
        		
        		theOutput = "DFS_LS_FILE_EXISTS " + path;
        	}
        	else if (theInput.contains("DFS_LS_FILE_EXISTS_CLIENT_RESPONSE")) {
        		String [] fromInputSplit = theInput.split(" ");
        		if (fromInputSplit[2].contains("true")) {
        			fileExists = true;
        		}
        		else {
        			fileExists = false;
        		}
        	}
        	else if (theInput.contains("DFS_REMOTE_DFS_COPY")) {
        		String[] fromInputSplit = theInput.split(" ");
        		String srcDfs = fromInputSplit[1];
        		String srcFile = fromInputSplit[2];
        		String destDfs = fromInputSplit[3];
        		String destFile = fromInputSplit[4];
        		
				theOutput = "DFS_COPY " + srcDfs + " " + srcFile + " " + destDfs + " " + destFile;
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
			else if (theInput.contains("DFS_LS_FILE_EXISTS")) {
				String[] fromServerSplit = theInput.split(" ");
				String path = fromServerSplit[1];
				
				DFS dfs = new DFS();
				
				try {
					boolean response = dfs.fileExists(path);
					
					theOutput = "DFS_LS_FILE_EXISTS_CLIENT_RESPONSE = " + response;
				}
				catch (IOException ioe) {
					theOutput = "DFS_LS_FILE_EXISTS_CLIENT_RESPONSE = false " + 
								"- EXCEPTION: " + ioe.getMessage();
				}
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

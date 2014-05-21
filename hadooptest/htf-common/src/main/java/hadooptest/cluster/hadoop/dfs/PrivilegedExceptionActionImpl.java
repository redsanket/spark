package hadooptest.cluster.hadoop.dfs;

import hadooptest.TestSession;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

public class PrivilegedExceptionActionImpl implements PrivilegedExceptionAction<String> {
	public static String ACTION_COPY_FROM_LOCAL = "copyFromLocal";
	public static String ACTION_APPEND_TO_FILE = "appendToFile";
	public static String ACTION_RM = "rm";
	public static String ACTION_RMDIR = "rmdir";
	public static String ACTION_DUMP_STUFF = "dumpStuff";
	public static String ACTION_MOVE_FROM_LOCAL = "moveFromLocal";
	public static String ACTION_MOVE_WITHIN_DFS = "mv";
	public static String ACTION_MOVE_TO_LOCAL = "moveToLocal";
	public static String ACTION_CHECKSUM = "chucksum";
	public static String ACTION_FILE_EXISTS = "fileExists";
	
	private UserGroupInformation ugi;
	private String action;
	private Configuration configuration;
	private String oneFile;
	private String otherFile;
	
	private static String DFS_SUPPORT_APPEND = "dfs.support.append";
	
	public PrivilegedExceptionActionImpl(UserGroupInformation ugi, String action,
			Configuration configuration, String firstFile, String secondFile)
					throws IOException {
		this.ugi = ugi;
		this.action = action;
		this.configuration = configuration;
		this.oneFile = firstFile;
		this.otherFile = secondFile;
	}

	String retStr = null;

	public String getReturnString() {
		return retStr;
	}

	public void setReturnString(String returnString) {
		this.retStr = returnString;
	}

	public String run() throws IOException {
	    String returnString = null;

	    TestSession.logger.info("Doing action[" + action + "] as [" + ugi.getUserName()
	            + "] with oneFile[" + oneFile + "] & otherFile["
	            + otherFile + "]");
	    FileSystem aRemoteFS = FileSystem.get(configuration);
	    if (action.equals(ACTION_COPY_FROM_LOCAL)) {
	        try {
	            aRemoteFS.copyFromLocalFile(false, true, new Path(oneFile),
	                    new Path(otherFile));
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	    } else if (action.equals(ACTION_APPEND_TO_FILE)) {
	        configuration.set(DFS_SUPPORT_APPEND, "TRUE");
	        aRemoteFS = FileSystem.get(configuration);
	        FSDataOutputStream fsout;
	        try {
	            fsout = aRemoteFS.append(new Path(oneFile));
	            // create a byte array of 10000 bytes, to append
	            int len = 10000;
	            byte[] data = new byte[len];
	            for (int i = 0; i < len; i++) {
	                data[i] = new Integer(i).byteValue();
	            }
	            fsout.write(data);
	            fsout.close();
	        } catch (AccessControlException acx) {
	            throw acx;
	        }
	    } else if (action.equals(ACTION_RM)) {
	        try {
	            aRemoteFS.delete(new Path(oneFile), false);
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	    } else if (action.equals(ACTION_RMDIR)) {
	        try {
	            // Do a recursive delete (should delete the dir name
	            // as well)
	            aRemoteFS.delete(new Path(oneFile), true);
	        } catch (IOException e) {
	            throw new RuntimeException(e);
	        }
	    } else if (action.equals(ACTION_DUMP_STUFF)) {
	        try {
	            System.out
	            .println("[file in question <-------------------------------------------------------------------->"
	                    + oneFile);
	            TestSession.logger.info("Dumping UGI details ");
	            TestSession.logger.info("UGI Current User (Static)"
	                    + UserGroupInformation.getCurrentUser()
	                    .getUserName());
	            TestSession.logger.info("UGI Login User (Static)"
	                    + UserGroupInformation.getLoginUser().getUserName());
	            TestSession.logger.info("UGI Short User Name " + ugi.getShortUserName());
	            TestSession.logger.info("UGI User Name " + ugi.getUserName());
	            for (String aGrpName : ugi.getGroupNames()) {
	                TestSession.logger.info(" Grp:" + aGrpName);
	            }
	            TestSession.logger.info("UGI Real User " + ugi.getRealUser());
	            System.out
	            .println("=======================================================================");
	            TestSession.logger.info("Canonical Service name:"
	                    + aRemoteFS.getCanonicalServiceName());
	            //          logger.info("Default Block Size:"
	            //                  + aRemoteFS.getDefaultBlockSize());
	            TestSession.logger.info("Default Block Size Path:"
	                    + aRemoteFS.getDefaultBlockSize(new Path(oneFile)));
	            //          logger.info("Default Replication:"
	            //                  + aRemoteFS.getDefaultReplication());
	            TestSession.logger.info("Default Replication Path:"
	                    + aRemoteFS
	                    .getDefaultReplication(new Path(oneFile)));
	            TestSession.logger.info("Used:" + aRemoteFS.getUsed());

	            ContentSummary cs = aRemoteFS.getContentSummary(new Path(
	                    oneFile));
	            TestSession.logger.info("Content Summary:" + cs.toString());
	            for (String renewer : new String[] { "hadoopqa", "dfsload" }) {
	                Token<?> aToken = aRemoteFS
	                        .getDelegationToken("hadoopqa");
	                TestSession.logger.info("Token Kind:" + aToken.getKind());
	                TestSession.logger.info("Token Service:" + aToken.getService());
	            }
	            FileStatus fileStatus = aRemoteFS.getFileStatus(new Path(
	                    oneFile));
	            TestSession.logger.info("Dumping file Status:");
	            TestSession.logger.info(" Access Time:" + fileStatus.getAccessTime());
	            TestSession.logger.info(" Block Size:" + fileStatus.getBlockSize());
	            TestSession.logger.info(" Group:" + fileStatus.getGroup());
	            TestSession.logger.info(" Length:" + fileStatus.getLen());
	            TestSession.logger.info(" Modification time:"
	                    + fileStatus.getModificationTime());
	            TestSession.logger.info(" Owner:" + fileStatus.getOwner());
	            TestSession.logger.info(" Replication:" + fileStatus.getReplication());
	            TestSession.logger.info(" Path:" + fileStatus.getPath().toString());
	            TestSession.logger.info(" Permissions:"
	                    + fileStatus.getPermission().toString());
	            // logger.info(" Symlink:" +
	            // fileStatus.getSymlink().toString();
	            BlockLocation[] blockLocs = aRemoteFS
	                    .getFileBlockLocations(fileStatus, 0, 50);
	            System.out.println("Dumping file Block locations_____");
	            for (BlockLocation blockLok : blockLocs) {
	                TestSession.logger.info(" Block Length:" + blockLok.getLength());
	                TestSession.logger.info(" Block Offset:" + blockLok.getOffset());
	                String[] hosts = blockLok.getHosts();
	                for (String aHost : hosts) {
	                    TestSession.logger.info(" Host:" + aHost);
	                }

	                String[] names = blockLok.getNames();
	                for (String aName : names) {
	                    TestSession.logger.info(" Name:" + aName);
	                }
	                TestSession.logger.info("Block Offset:"
	                        + blockLok.getTopologyPaths());
	                String[] topPaths = blockLok.getTopologyPaths();
	                for (String aTopPath : topPaths) {
	                    TestSession.logger.info(" Topology Path:" + aTopPath);
	                }
	            }
	            TestSession.logger.info("Home dir:" + aRemoteFS.getHomeDirectory());
	            TestSession.logger.info("URI:" + aRemoteFS.getUri());
	        } catch (IOException e) {
	            throw (e);
	        }
	    } else if (action.equals(ACTION_MOVE_FROM_LOCAL)) {
	        aRemoteFS.moveFromLocalFile(new Path(oneFile), new Path(
	                otherFile));
	    } else if (action.equals(ACTION_MOVE_WITHIN_DFS)) {
	        aRemoteFS.rename(new Path(oneFile), new Path(otherFile));
	    } else if (action.equals(ACTION_MOVE_TO_LOCAL)) {
	        aRemoteFS.moveToLocalFile(new Path(oneFile),
	                new Path(otherFile));
	    } else if (action.equals(ACTION_CHECKSUM)) {
	        FileChecksum aChecksum = aRemoteFS.getFileChecksum(new Path(
	                oneFile));
	        returnString = Hex.encodeHexString(aChecksum.getBytes());
	        this.retStr = returnString;
	        TestSession.logger.info("Checksum string[" + returnString + "]");
	    } else if (action.equals(ACTION_FILE_EXISTS)) {
	        Boolean fileExists = aRemoteFS.exists(new Path(oneFile));				
	        setReturnString(fileExists.toString());
	    }
		return returnString;
	}
}
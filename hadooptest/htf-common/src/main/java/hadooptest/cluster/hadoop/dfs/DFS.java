package hadooptest.cluster.hadoop.dfs;

import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.HadoopCluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A class which handles common test funcions of a DFS.
 */
public class DFS {
	
	/**
	 * Class constructor.
	 */
	public DFS() {
		
	}
	
	/**
	 * Create a local binary file to use for test purposes.
	 * 
	 * @param aFileName The full path and name of the file.
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void createLocalFile(String aFileName) 
			throws FileNotFoundException, IOException {
		
		TestSession.logger.info("!!!!!!! Creating local file:" + aFileName);
		File attemptedFile = new File(aFileName);

		if (attemptedFile.exists()) {
			attemptedFile.delete();
		}
		// create a file on the local fs
		if (!attemptedFile.getParentFile().exists()) {
			attemptedFile.getParentFile().mkdirs();
		}

		FileOutputStream fout;

		fout = new FileOutputStream(attemptedFile);
		// create a byte array of 350MB (367001600) bytes
		int len = 36700;
		byte[] data = new byte[len];
		for (int i = 0; i < len; i++) {
			data[i] = new Integer(i).byteValue();
		}
		fout.write(data);
		fout.close();
	}
	
	/**
	 * Check if a file exists on a remote DFS.
	 * 
	 * @param destFile The path to the file to check
	 * @param nameNode The namenode address of the target DFS, in a format like:
	 *                 http://gsbl90639.blue.ygrid.yahoo.com:50070
	 * @param user The user to perform the operation as.
	 * @param userKeytab The keytab file of the user to perform the operation as
	 * @param schema The schema to perform the operation as (hdfs or webhdfs)
	 * 
	 * @return boolean true if the file was found on the remote DFS.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean fileExistsOnRemoteDFS(String destFile, 
			String nameNode, String user, String userKeytab, String schema) 
					throws IOException, InterruptedException {
		
		Configuration aConf = DFS.getConfForRemoteFS(nameNode, user, schema);
		UserGroupInformation ugi = DFS.getUgiForUser(user, userKeytab, aConf);

		DoAs doAs;
		// Check if the file was created
		doAs = new DoAs(ugi, PrivilegedExceptionActionImpl.ACTION_FILE_EXISTS, 
				aConf, destFile, null);
		Boolean fileExists = Boolean.valueOf(doAs.doAction());
		
		if (fileExists) {
			TestSession.logger.info("File exists on DFS: " + destFile);
		}
		else {
			TestSession.logger.info("File does not exist on DFS: " + destFile);
		}
		
		return fileExists;
	}
	
	/**
	 * Copy a local file to a remote DFS.
	 * 
	 * @param localFile The path to the local system file to copy.
	 * @param destFile The destination of the file copy.
	 * @param nameNode The namenode address of the target DFS, in a format like:
	 *                 http://gsbl90639.blue.ygrid.yahoo.com:50070
	 * @param user The user to perform the operation as.
	 * @param userKeytab The keytab file of the user to perform the operation as
	 * @param schema The schema to perform the operation as (hdfs or webhdfs)
	 * 
	 * @return boolean true if the file was successfully copied.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean copyFileToRemoteDFS(String localFile, String destFile, 
			String nameNode, String user, String userKeytab, String schema) 
					throws IOException, InterruptedException {
		
		Configuration aConf = DFS.getConfForRemoteFS(nameNode, user, schema);
		UserGroupInformation ugi = DFS.getUgiForUser(user, userKeytab, aConf);

		DoAs doAs;
		// First copy the file to the remote DFS'es
		doAs = new DoAs(ugi, 
				PrivilegedExceptionActionImpl.ACTION_COPY_FROM_LOCAL, aConf,
				localFile, destFile);
		doAs.doAction();

		// Check if the file was created
		Boolean fileExists = fileExistsOnRemoteDFS(destFile, nameNode, user, 
				userKeytab, schema);
		
		if (fileExists) {
			TestSession.logger.info("File was created on DFS: " + destFile);
		}
		else {
			TestSession.logger.info("File was not created on DFS: " + destFile);
		}
		
		return fileExists;
	}
	
	/**
	 * Delete a file from a remote DFS.
	 * 
	 * @param destFile The destination of the file delete.
	 * @param nameNode The namenode address of the target DFS, in a format like:
	 *                 http://gsbl90639.blue.ygrid.yahoo.com:50070
	 * @param user The user to perform the operation as.
	 * @param userKeytab The keytab file of the user to perform the operation as
	 * @param schema The schema to perform the operation as (hdfs or webhdfs)
	 * 
	 * @return boolean true if the file was successfully deleted.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean deleteFileFromRemoteDFS(String destFile, 
			String nameNode, String user, String userKeytab, String schema) 
					throws IOException, InterruptedException {
		
		Configuration aConf = DFS.getConfForRemoteFS(nameNode, user, schema);
		UserGroupInformation ugi = DFS.getUgiForUser(user, userKeytab, aConf);

		DoAs doAs;
		// First delete the file on the remote DFS
		doAs = new DoAs(ugi, PrivilegedExceptionActionImpl.ACTION_RM, aConf,
				destFile, null);
		doAs.doAction();

		// Check if the file was deleted
		Boolean fileExists = fileExistsOnRemoteDFS(destFile, nameNode, user, 
				userKeytab, schema);
		
		if (!fileExists) {
			TestSession.logger.info("File was deleted on DFS: " + destFile);
		}
		else {
			TestSession.logger.info("File was not deleted on DFS: " + destFile);
		}
		
		return !fileExists;
	}
	
	/**
	 * Get the UGI for a given keytab user.
	 * 
	 * @param keytabUser The user to get the UGI for.
	 * @param keytabDir The path to the user's keytab file.
	 * @param aConf The Hadoop Configuration for the DFS you're connecting to.
	 * 
	 * @return UserGroupInformation
	 * 
	 * @throws IOException
	 */
	public static UserGroupInformation getUgiForUser(String keytabUser, 
			String keytabDir, Configuration aConf) 
					throws IOException {
		
		TestSession.logger.info("Set keytab user=" + keytabUser);
		TestSession.logger.info("Set keytab dir=" + keytabDir);
		UserGroupInformation ugi;

		UserGroupInformation.setConfiguration(aConf);
		ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
				keytabUser, keytabDir);
		TestSession.logger.info("UGI=" + ugi);
		TestSession.logger.info("credentials:" + ugi.getCredentials());
		TestSession.logger.info("group names" + ugi.getGroupNames());
		TestSession.logger.info("real user:" + ugi.getRealUser());
		TestSession.logger.info("short user name:" + ugi.getShortUserName());
		TestSession.logger.info("token identifiers:" + ugi.getTokenIdentifiers());
		TestSession.logger.info("tokens:" + ugi.getTokens());
		TestSession.logger.info("username:" + ugi.getUserName());
		TestSession.logger.info("current user:" + UserGroupInformation.getCurrentUser());
		TestSession.logger.info("login user:" + UserGroupInformation.getLoginUser());

		return ugi;
	}
	
	/**
	 * Get a basic Hadoop Configuration to use for connecting to a secure 
	 * remote DFS.
	 * 
	 * @param nameNode The namenode address of the target DFS, in a format like:
	 *                 http://gsbl90639.blue.ygrid.yahoo.com:50070 
	 * @param aUser The user to connect as.
	 * @param schema The schema to connect as (hdfs or webhdfs)
	 * 
	 * @return Configuration a Hadoop Configuration object representing the 
	 *         basic configuration necessary to connect to a secure remote DFS.
	 */
	public static Configuration getConfForRemoteFS(String nameNode, 
			String aUser, String schema) {
		
		Configuration conf = new Configuration(false);

		if (schema.equals(HadooptestConstants.Schema.HDFS)) {
			// HDFS
			String namenodeWithChangedSchema = nameNode.replace(
					HadooptestConstants.Schema.HTTP,
					HadooptestConstants.Schema.HDFS);
			String namenodeWithChangedSchemaAndPort = namenodeWithChangedSchema
					.replace("50070", HadooptestConstants.Ports.HDFS);
			TestSession.logger.info("For HDFS set the namenode to:["
					+ namenodeWithChangedSchemaAndPort + "]");
			conf.set("fs.defaultFS", namenodeWithChangedSchemaAndPort);
		} else {
			// WEBHDFS
			String namenodeWithChangedSchema = nameNode.replace(
					HadooptestConstants.Schema.HTTP,
					HadooptestConstants.Schema.WEBHDFS);
			String namenodeWithChangedSchemaAndNoPort = namenodeWithChangedSchema
					.replace(":50070", "");
			TestSession.logger.info("For WEBHDFS set the namenode to:["
					+ namenodeWithChangedSchemaAndNoPort + "]");
			conf.set("fs.defaultFS", namenodeWithChangedSchemaAndNoPort);
		}
		if (aUser.equals(HadooptestConstants.UserNames.HADOOPQA)) {
			conf.set("hadoop.job.ugi", HadooptestConstants.UserNames.HADOOPQA);
		} else {
			conf.set("hadoop.job.ugi", HadooptestConstants.UserNames.DFSLOAD);
		}
		
		conf.set("hadoop.security.authentication", "kerberos");
		conf.set("hadoop.security.authorization", "true");
		conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("fs.webhdfs.impl", "org.apache.hadoop.hdfs.web.WebHdfsFileSystem");
		conf.set("dfs.block.access.token.enable", "true");
		conf.set("dfs.web.authentication.kerberos.principal", "HTTP/_HOST@DEV.YGRID.YAHOO.COM");
		conf.set("dfs.web.authentication.kerberos.keytab", "/etc/grid-keytabs/gsbl90639.dev.service.keytab");
		conf.set("dfs.namenode.keytab.file", "/etc/grid-keytabs/gsbl90639.dev.service.keytab");
		conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DEV.YGRID.YAHOO.COM");
		conf.set("dfs.namenode.kerberos.https.principal", "host/gsbl90639.blue.ygrid.yahoo.com@DEV.YGRID.YAHOO.COM");
		
		TestSession.logger.info(conf);
		return conf;
	}
	
	/**
	 * Performs a filesystem ls given a path, with no extra ls args.
	 * 
	 * @param path The filesystem path.
	 * 
	 * @throws Exception if the fs ls fails.
	 */
	public void fsls(String path) throws Exception {
		fsls(path, null);
	}
	
    /**
     * Form a base URL for an HDFS cluster.
     * 
     * @return String the base URL for the HDFS cluster.
     * 
     * @throws Exception if we can not get the cluster namenode.
     */
    public String getBaseUrl() throws Exception {
        return "hdfs://" +
                TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0];
    }
    
	/**
	 * Performs a filesystem ls given a path and any extra arguments
	 * to run on the fs ls.
	 * 
	 * @param path The filesystem path.
	 * @param args Extra arguments to attach to the fs ls.
	 * 
	 * @throws Exception if the method can't get the FS shell.
	 */
	public void fsls(String path, String[] args) throws Exception {
	    TestSession.logger.debug("Show HDFS path: '" + path + "':");
	    FsShell fsShell = TestSession.cluster.getFsShell();
	    String URL = path.startsWith("hdfs://") ? path : getBaseUrl() + path;
	 
	    String[] cmd;
	    if (args == null) {	        
            cmd = new String[] {"-ls", URL};
	    } else {
	        ArrayList<String> list = new ArrayList<String>();
	        list.add("-ls");
            list.addAll(Arrays.asList(args));
            list.add(URL);
            cmd = (String[]) list.toArray(new String[0]);
        }
        TestSession.logger.info(
                TestSession.cluster.getConf().getHadoopProp("HDFS_BIN") +
                    " dfs " + StringUtils.join(cmd, " "));
        fsShell.run(cmd);
    }
	
    /**
     * Performs a filesystem ls given a path and any extra arguments
     * to run on the fs ls.
     * 
     * @param path The filesystem path.
     * @param args Extra arguments to attach to the fs ls.
     * 
     * @throws Exception if the method can't get the FS shell.
     */
    public void fsdu(String path, String[] args) throws Exception {
        TestSession.logger.debug("Show HDFS disk uage for path: '" +
                path + "':");
        FsShell fsShell = TestSession.cluster.getFsShell();
        String URL = path.startsWith("hdfs://") ? path : getBaseUrl() + path;
     
        String[] cmd;
        if (args == null) {         
            cmd = new String[] {"-du", URL};
        } else {
            ArrayList<String> list = new ArrayList<String>();
            list.add("-du");
            list.addAll(Arrays.asList(args));
            list.add(URL);
            cmd = (String[]) list.toArray(new String[0]);
        }
        TestSession.logger.info(
                TestSession.cluster.getConf().getHadoopProp("HDFS_BIN") +
                    " dfs " + StringUtils.join(cmd, " "));
        fsShell.run(cmd);
    }

    /**
	 * Creates a new directory in the DFS.  If the directory already exists, it 
	 * will delete the existing directory before creating the new one.
	 * 
	 * The path format will be of the form "/user/[USERNAME]/[path].
	 * 
	 * @param path The new DFS path.
	 * 
	 * @throws Exception if we can not make the new directory.
	 */
	public void mkdir(String path) throws Exception {
		FileSystem fs = TestSession.cluster.getFS();
		FsShell fsShell = TestSession.cluster.getFsShell();		
		String dir = getBaseUrl() + path;
		if (fs.exists(new Path(dir))) {
			TestSession.logger.info("Delete existing directory: " + dir);
			fsShell.run(new String[] {"-rm", "-r", dir});			
		}
		TestSession.logger.info("Create new directory: " + dir);
		fsShell.run(new String[] {"-mkdir", "-p", dir});
	}
	
	/**
	 * Put a file from the local filesystem to the hdfs filesystem, given
	 * a source file and a target file.  Uses hadoop dfs command line
	 * interface.
	 * 
	 * @param source the source file on the local filesystem.
	 * @param target the target for the file on the hdfs filesystem.  Expects a
	 *               correctly formed HDFS path.
	 * 
	 * @throws Exception if unable to get the fsShell, unable to get the 
	 *                   cluster filesystem, unable to get the hdfs base URL,
	 *                   the filesystem path does not exist, the new target
	 *                   directory cannot be made, or the dfs put has a fatal
	 *                   failure.
	 */
	public void putFileLocalToHdfs(String source, String target) 
			throws Exception {
		
		TestSession.logger.debug("target=" + target);
		String targetDir = target.substring(0, target.lastIndexOf("/"));	
		TestSession.logger.debug("target path=" + targetDir);

		FsShell fsShell = TestSession.cluster.getFsShell();
		FileSystem fs = TestSession.cluster.getFS();

		if (!fs.exists(new Path(targetDir))) {
			fsShell.run(new String[] {"-mkdir", "-p", targetDir});
		}
		TestSession.logger.debug("dfs -put " + source + " " + target);
		fsShell.run(new String[] {"-put", source, target});
	}
	
	/**
	 * Copies a file from a local system filepath to a HDFS destination, using
	 * the Hadoop API.
	 * 
	 * @param src The local filepath and filename.
	 * @param dest The HDFS destination path.
	 * 
	 * @throws IOException if the copy does not succeed.
	 */
	public void copyLocalToHdfs(String src, String dest) throws IOException {
		TestSession.cluster.getFS().copyFromLocalFile(
				new Path(src), new Path(dest));
	}
	
	/**
	 * Copies a file from an DFS in one cluster to a DFS in another cluster.
	 * The clusters must currently use the same gateway.  This can also be used
	 * to copy/move data within a single DFS.  Uses the Hadoop API.
	 * 
	 * This method will delete any prior instance of the destination file
	 * before it attempts the copy.
	 * 
	 * @param srcClusterBasePath The base URI for your source DFS.
	 * @param srcPath The relative path in your source DFS to copy from.
	 * @param destClusterBasePath The base URI for your destination DFS.
	 * @param destPath The relative path in your destination DFS to copy to.
	 * 
	 * @return boolean whether the copy was successful or not.
	 * 
	 * @throws Exception if the copy fails to find the source or destination
	 * 						DFS paths.
	 */
	public boolean copyDfsToDfs(String srcClusterBasePath,
			String srcPath, 
			String destClusterBasePath,
			String destPath) throws Exception {
		
		String srcURL = srcClusterBasePath + "/" + srcPath;
		TestSession.logger.info("SRC_PATH = " + srcClusterBasePath);
		
		String destURL = destClusterBasePath + "/" + destPath;
		TestSession.logger.info("DEST_PATH = " + destClusterBasePath);
		
		Configuration srcClusterConf = new Configuration();
		srcClusterConf.set("fs.default.name", srcClusterBasePath);
		
		Configuration destClusterConf = new Configuration();
		destClusterConf.set("fs.default.name", destClusterBasePath);

		FileSystem srcFS = FileSystem.get(URI.create(srcURL), srcClusterConf);
		TestSession.logger.info("SRC_FILESYSTEM = " + srcFS.toString());
		
		FileSystem destFS = FileSystem.get(URI.create(destURL), 
				destClusterConf);
		TestSession.logger.info("DEST_FILESYSTEM = " + destFS.toString());

		if (this.fileExists(destFS, destURL)) {
			// remove any instance of the target file.
			TestSession.logger.info(
					"Deleting a prior instance file in target location....");
			this.delete(destFS, destURL, true);
		}
		else {
			TestSession.logger.info(
					"Target file doesn't exist yet, no need to clean up.");
		}
		
		TestSession.logger.info("Copying data...");
		return FileUtil.copy(srcFS, new Path(srcURL), destFS, 
				new Path(destURL), 
				false, (Configuration)(TestSession.cluster.getConf()));
	}
	
	/**
	 * Delete a path from the primary cluster DFS using the Hadoop API.
	 * 
	 * @param path The path to delete.
	 * @param recursive Whether the delete should be recursive or not.
	 * 
	 * @return boolean Whether the delete was successful or not.
	 * 
	 * @throws IOException if the target path cannot be formed.
	 */
	public boolean delete(String path, boolean recursive) throws IOException {
		TestSession.logger.info("Deleting: " + path);
		TestSession.logger.info("Recursive delete is: "  + recursive);
		return TestSession.cluster.getFS().delete(new Path(path), recursive);
	}
	
	/**
	 * Delete a path from an accessible DFS on any cluster using the Hadoop API.
	 * 
	 * @param fs The target FileSystem.
	 * @param path The path to delete.
	 * @param recursive Whether the delete should be recursive or not.
	 * 
	 * @return boolean Whether the delete was successful or not.
	 * 
	 * @throws IOException if the target path cannot be formed or the target
	 * 						FileSystem cannot be reached.
	 */
	public boolean delete(FileSystem fs, String path, boolean recursive) 
			throws IOException {
		TestSession.logger.info("Deleting: " + path);
		TestSession.logger.info("Recursive delete is: "  + recursive);
		return fs.delete(new Path(path), recursive);
	}
	
	/**
	 * Gets an iterator of files in a DFS path through the Hadoop API, for the
	 * primary cluster filesystem.
	 * 
	 * @param basePath The base directory path to perform the ls.
	 * @param recursive Whether the ls should be recursive or not.
	 * 
	 * @return RemoteIterator<LocatedFileStatus> an iterator of file statuses.
	 * 
	 * @throws Exception if there is a problem performing the ls.
	 */
	public RemoteIterator<LocatedFileStatus> getFsLs(String basePath, 
			boolean recursive) throws Exception {
		return this.getFsLs(TestSession.cluster.getFS(), basePath, recursive);
	}
	
	/**
	 * Gets an iterator of files in a DFS path through the Hadoop API, for a 
	 * remote DFS.
	 * 
	 * @param fs The filesystem to perform the ls on.
	 * @param basePath The base directory path to perform the ls.
	 * @param recursive Whether the ls should be recursive or not.
	 * 
	 * @return RemoteIterator<LocatedFileStatus> an iterator of file statuses.
	 * 
	 * @throws Exception if there is a problem performing the ls.
	 */
	public RemoteIterator<LocatedFileStatus> getFsLs(FileSystem fs, 
			String basePath, boolean recursive) throws IOException {
		return fs.listFiles(new Path(basePath), recursive);
	}
	
	/**
	 * Print the list of all files in the primary cluster DFS at a given path.
	 * 
	 * @param basePath The base directory path to perform the ls.
	 * @param recursive Whether the ls should be recursive or not.
	 * 
	 * @throws Exception if there is a problem performing the ls.
	 */
	public void printFsLs(String basePath, boolean recursive) throws Exception {
		this.printFsLs(TestSession.cluster.getFS(), basePath, recursive);
	}
	
	/**
	 * Print the list of all files in a remote cluster DFS at a given path.
	 * 
	 * @param fs the FileSystem to perform the ls on.
	 * @param basePath The base directory path to perform the ls.
	 * @param recursive Whether the ls should be recursive or not.
	 * 
	 * @throws Exception if there is a problem performing the ls.
	 */
	public void printFsLs(FileSystem fs, String basePath, boolean recursive) 
			throws Exception {
		RemoteIterator<LocatedFileStatus> iter = 
				this.getFsLs(fs, basePath, recursive);

		while(iter.hasNext()) {
			TestSession.logger.info(
					((LocatedFileStatus)(iter.next())).getPath().toString());
		}
	}
	
	/**
	 * Check whether a path exists in the DFS on the primary cluster.
	 * 
	 * @param path The path to verify.
	 * 
	 * @return Whether the path exists or not.
	 * 
	 * @throws IOException if there is a problem setting the path.
	 */
	public boolean fileExists(String path) throws IOException {
		return this.fileExists(TestSession.cluster.getFS(), path);
	}
	
	/**
	 * Check whether a path exists in the DFS on a remote file system.
	 * 
	 * @param fs The filesystem which should contain the path.
	 * @param path The path to verify.
	 * 
	 * @return Whether the path exists or not.
	 * 
	 * @throws IOException if there is a problem setting the path.
	 */
	public boolean fileExists(FileSystem fs, String path) throws IOException {
		return fs.exists(new Path(path));
	}
	
	/**
	 * Gets a FileSystem reference via a string URI representing the file
	 * system in question.  The filesystem string should look similar to:
	 * "hdfs://hostname:port".
	 * 
	 * @param filesystem a string representing the filesystem base URI.
	 * 
	 * @return a FileSystem representing the passed-in URI.
	 * 
	 * @throws IOException if the FileSystem cannot be found.
	 */
	public FileSystem getFileSystemFromURI(String filesystem) 
			throws IOException {
		Configuration destClusterConf = new Configuration();
		destClusterConf.set("fs.default.name", filesystem);
		return FileSystem.get(URI.create(filesystem), destClusterConf);
	}
	
	/**
	 * Returns the default DFS HTTP port.
	 * 
	 * @return String the default DFS HTTP port.
	 */
	public String getHTTPDefaultPort() {
		return TestSession.cluster.getConf().get("dfs.http.address");
	}
	
	/**
	 * Verifies whether webhdfs is enabled on a cluster DFS.
	 * 
	 * @return whether webhdfs is enabled on the cluster DFS.
	 */
	public boolean isWebhdfsEnabled() {
		String isEnabled = TestSession.cluster.getConf().get(
				"dfs.webhdfs.enabled");
		
		if (isEnabled.equals("true")) {
			return true;
		}
		else {
			return false;
		}
	}
	
	/**
	 * Gets the configuration value for 
	 * dfs.web.authentication.kerberos.principal
	 * 
	 * @return the configuration value for 
	 * 				dfs.web.authentication.kerberos.principal
	 */
	public String getWebhdfsKerberosAuthPrincipal() {
		return TestSession.cluster.getConf().get(
				"dfs.web.authentication.kerberos.principal");
	}
	
	/**
	 * Gets the configuration value for dfs.web.authentication.kerberos.keytab
	 * 
	 * @return the configuration value for 
	 * 			dfs.web.authentication.kerberos.keytab
	 */
	public String getWebhdfsKerberosAuthKeytab() {
		return TestSession.cluster.getConf().get(
				"dfs.web.authentication.kerberos.keytab");
	}

}

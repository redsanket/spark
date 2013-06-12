package hadooptest.cluster.hadoop;

import hadooptest.TestSession;

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
		String URL = getBaseUrl() + path;
 
		String[] cmd;
		if (args == null) {
			cmd = new String[] {"-ls", URL};
		} else {
			ArrayList list = new ArrayList();
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
	 * @throws IOException if the copy fails to find the source or destination
	 * 						DFS paths.
	 */
	public boolean copyDfsToDfs(String srcClusterBasePath,
			String srcPath, 
			String destClusterBasePath,
			String destPath) throws IOException {
		
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
		
		FileSystem destFS = FileSystem.get(URI.create(destURL), destClusterConf);
		TestSession.logger.info("DEST_FILESYSTEM = " + destFS.toString());

		// remove any instance of the target file.
		TestSession.logger.info("Deleting any prior instance file in target location....");
		this.deleteFromFS(destFS, destURL, true);
		
		TestSession.logger.info("Copying data...");
		return FileUtil.copy(srcFS, new Path(srcURL), destFS, 
				new Path(destURL), false, (Configuration)(TestSession.cluster.getConf()));
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
	public boolean deleteFromFS(FileSystem fs, String path, boolean recursive) 
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
		return this.getFsLsRemote(TestSession.cluster.getFS(), basePath, recursive);
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
	public RemoteIterator<LocatedFileStatus> getFsLsRemote(FileSystem fs, String basePath, boolean recursive) throws IOException {
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
		this.printFsLsRemote(TestSession.cluster.getFS(), basePath, recursive);
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
	public void printFsLsRemote(FileSystem fs, String basePath, boolean recursive) throws Exception {
		RemoteIterator<LocatedFileStatus> iter = this.getFsLsRemote(fs, basePath, recursive);

		while(iter.hasNext()) {
			TestSession.logger.info(
					((LocatedFileStatus)(iter.next())).getPath().toString());
		}
	}
}

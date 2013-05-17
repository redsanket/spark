package hadooptest.cluster.hadoop;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;

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
	 * a source file and a target file.
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
	
}

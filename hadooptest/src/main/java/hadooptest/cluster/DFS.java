package hadooptest.cluster;

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
		TestSession.logger.info(TestSession.cluster.getConf().getHadoopProp("HDFS_BIN") +
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
		return "hdfs://" + TestSession.cluster.getNodes("namenode")[0];
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
		String dir = getBaseUrl() + "/user/" + System.getProperty("user.name") + "/" + path;
		if (fs.exists(new Path(dir))) {
			TestSession.logger.info("Delete existing directory: " + dir);
			fsShell.run(new String[] {"-rm", "-r", dir});			
		}
		TestSession.logger.info("Create new directory: " + dir);
		fsShell.run(new String[] {"-mkdir", "-p", dir});
	}
	
}

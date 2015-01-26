package hadooptest.gdm.regression.stress;

import static org.junit.Assert.assertTrue;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * Class that deletes instance(s) and instance file(s).
 *
 */
public class CleanSourceClusterDataAction implements PrivilegedExceptionAction<String> {

	private Configuration configuration;
	private String basePath ;
	
	CleanSourceClusterDataAction(Configuration conf , String path) {
		this.configuration = conf;
		this.basePath = path;
	}
	
	
	/*
	 * this override method deletes the given path.
	 * 
	 */
	@Override
	public String run() throws Exception {
		String result = "failed";
		FileSystem remoteFS = FileSystem.get(this.configuration);
		Path path = new Path(this.basePath.trim());

		// check whether remote path exists on the grid
		boolean flag = remoteFS.exists(path);
		if (flag == true) {
			boolean deletedSuccessfull = remoteFS.delete(path, true);
			assertTrue("Failed to deleted " + path.toString() , deletedSuccessfull == true);
			if (deletedSuccessfull) {
				result = "passed";
			}
		}
		return result;
	}
}

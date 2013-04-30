package hadooptest.hadoop.regression;

import static org.junit.Assert.*;

import hadooptest.TestSession;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFS extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	public String getHdfsBaseUrl() throws Exception {
		return "hdfs://" + TestSession.cluster.getNodes("namenode")[0];
	}
	
	public void fsls(String path) throws Exception {
		fsls(path, null);
	}

	public void fsls(String path, String[] args) throws Exception {
		TestSession.logger.debug("Show HDFS path: '" + path + "':");
		FsShell fsShell = TestSession.cluster.getFsShell();
		String URL = getHdfsBaseUrl() + path;
 
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

	/*
	* A test for running a pipes wordcount job
	*/
	@Test
	public void runTestFS() throws Exception {
		TestSession.logger.info("Run FS Test");

		FileSystem fs = TestSession.cluster.getFS();
		String testDir = getHdfsBaseUrl() + "/";
		if (fs.exists(new Path(testDir))) {
			TestSession.logger.info("Found test directory: " + testDir);
		}
		assertTrue("Root directory '/' does not exist. ", fs.exists(new Path(testDir)));
		fsls("/", new String[] {"-d"});
	}
}

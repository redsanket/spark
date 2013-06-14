package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.*;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;
import hadooptest.cluster.hadoop.HadoopCluster;

import java.io.File;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestEndToEndPipes extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
	}

	public void showHdfsDir() throws Exception {
        DFS dfs = new DFS();
		dfs.fsls("/user/" + System.getProperty("user.name") + "/pipes", new String[] {"-d"});
		dfs.fsls("/user/" + System.getProperty("user.name") + "/pipes", new String[] {"-R"});
	}	

	public void setupHdfsDir() throws Exception {
		FileSystem fs = TestSession.cluster.getFS();
		FsShell fsShell = TestSession.cluster.getFsShell();		
        DFS dfs = new DFS();
		String testDir =
		        dfs.getBaseUrl() + "/user/" + System.getProperty("user.name") +
		        "/pipes";
		if (fs.exists(new Path(testDir))) {
			TestSession.logger.info("Delete existing test directory: " + testDir);
			fsShell.run(new String[] {"-rm", "-r", testDir});			
		}
		TestSession.logger.info("Create new test directory: " + testDir);
		fsShell.run(new String[] {"-mkdir", "-p", testDir});
	}
	
	
	/*
	 *  Check for the destination directory and create it if
     * is not present because 'dfs put' used to do that 
     */
	private void putLocalToHdfs(String source, String target) throws Exception {
	    TestSession.logger.debug("target=" + target);
	    String targetDir = target.substring(0, target.lastIndexOf("/"));	    
	    TestSession.logger.debug("target path=" + targetDir);

	    FsShell fsShell = TestSession.cluster.getFsShell();
		FileSystem fs = TestSession.cluster.getFS();

		String URL =
		        "hdfs://" +
		        TestSession.cluster.getNodeNames(HadoopCluster.NAMENODE)[0] +
		        "/";
		String homeDir = URL + "user/" + System.getProperty("user.name");
		String testDir = homeDir + "/" + targetDir;
		if (!fs.exists(new Path(testDir))) {
			fsShell.run(new String[] {"-mkdir", "-p", testDir});
		}
		TestSession.logger.debug("dfs -put " + source + " " + target);
		fsShell.run(new String[] {"-put", source, target});
	}
	
	private String getResourceFullPath(String relativePath) throws Exception {
        URL url = this.getClass().getClassLoader().getResource(relativePath);
        String fullPath = url.getPath();
        TestSession.logger.debug("Resource URL path=" + fullPath);
        return fullPath;
	}
	
	/*
	 * A test for running a pipes wordcount job
	 */
	@Test
	public void runPipesTest() throws Exception {
		TestSession.logger.info("Run EndToEnd Pipes Test");
		
		TestSession.logger.trace(TestSession.cluster.getConf().toString("resources"));
		TestSession.logger.trace(TestSession.cluster.getConf().toString("props"));

		setupHdfsDir();
		putLocalToHdfs(getResourceFullPath("resources/hadoop/data/pipes/c++-examples/Linux-i386-32/bin/"), "pipes/");
		putLocalToHdfs(getResourceFullPath("resources/hadoop/data/pipes/input.txt"), "pipes/input.txt");
		showHdfsDir();

		String[] jobCmd = {
				TestSession.cluster.getConf().getHadoopProp("MAPRED_BIN"),
				"--config", TestSession.cluster.getConf().getHadoopConfDir(),
				"pipes", 
				"-conf", getResourceFullPath("resources/hadoop/data/pipes/word.xml"),	
				"-input", "pipes/input.txt",
				"-output", "pipes/outputDir",	 
				"-jobconf", "mapred.job.name=End2EndPipesTest",
				"-jobconf", "mapreduce.job.acl-view-job=*"
		 };
		String[] jobOutput = TestSession.exec.runProcBuilderSecurity(jobCmd);
		if (!jobOutput[0].equals("0")) {
			TestSession.logger.info("Got unexpected non-zero exit code: " + jobOutput[0]);
			TestSession.logger.info("stdout" + jobOutput[1]);
			TestSession.logger.info("stderr" + jobOutput[2]);			
		}

		String[] catCmd = {
				TestSession.cluster.getConf().getHadoopProp("HDFS_BIN"),
				"--config", TestSession.cluster.getConf().getHadoopConfDir(),
				"dfs", "-cat", "pipes/outputDir/*"				
		};
		String[] catOutput = TestSession.exec.runProcBuilderSecurity(catCmd);
		if (!catOutput[0].equals("0")) {
			TestSession.logger.info("Got unexpected non-zero exit code: " + catOutput[0]);
			TestSession.logger.info("stdout" + catOutput[1]);
			TestSession.logger.info("stderr" + catOutput[2]);			
		}
		
		String expectedOutputStr = FileUtils.readFileToString(
						new File(getResourceFullPath("" +
								"resources/hadoop/data/pipes/expectedOutput")));
		String actualOutputStr = catOutput[1];
		TestSession.logger.debug("expected output str = \n'" + expectedOutputStr + "'");
		TestSession.logger.debug("actual output str = \n'" + actualOutputStr + "'");
		assertEquals("Actual output is different than expected ouput.",
		        expectedOutputStr, actualOutputStr);
	}

}

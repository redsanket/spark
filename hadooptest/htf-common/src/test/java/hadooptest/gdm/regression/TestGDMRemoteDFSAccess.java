package hadooptest.gdm.regression;

import static org.junit.Assert.assertTrue;
import hadooptest.SerialTests;
import hadooptest.TestSession;
import hadooptest.automation.constants.HadooptestConstants;
import hadooptest.cluster.hadoop.dfs.DFS;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

@Category(SerialTests.class)
public class TestGDMRemoteDFSAccess extends TestSession {

	// Namenode address
	private static String nameNode;

	private String aHadoopqaFileName = "/tmp/"
			+ HadooptestConstants.UserNames.HADOOPQA + "Dir/"
			+ HadooptestConstants.UserNames.HADOOPQA + "File";

	private String aDfsloadFileName = "/tmp/"
			+ HadooptestConstants.UserNames.DFSLOAD + "Dir/"
			+ HadooptestConstants.UserNames.DFSLOAD + "File";
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
		
		nameNode = TestSession.conf.getProperty("GDM_REMOTE_DFS_NAMENODE");
	}
	
	// A test that copies a local system file to a remote DFS as hadoopqa, 
	// over webhdfs.
	@Test
	public void testDFSCopyLocalFileHadoopqaWebhdfs() throws Exception {
		// Create a local file
		DFS.createLocalFile(aHadoopqaFileName);

		assertTrue("File " + aHadoopqaFileName + " was not created.", 
				DFS.copyFileToRemoteDFS(
						aHadoopqaFileName, 
						aHadoopqaFileName, 
						nameNode, 
						HadooptestConstants.UserNames.HADOOPQA, 
						HadooptestConstants.Location.Keytab.HADOOPQA, 
						HadooptestConstants.Schema.WEBHDFS));
	}

	// A test that deletes a file on a remote DFS as hadoopqa, over webhdfs.
	@Test
	public void testDFSDeleteFileHadoopqaWebhdfs() throws Exception {		
		assertTrue("File " + aHadoopqaFileName + " was not deleted.", 
				DFS.deleteFileFromRemoteDFS(
						aHadoopqaFileName, 
						nameNode, 
						HadooptestConstants.UserNames.HADOOPQA, 
						HadooptestConstants.Location.Keytab.HADOOPQA, 
						HadooptestConstants.Schema.WEBHDFS));
	}

	// A test that copies a local system file to a remote DFS as hadoopqa, 
	// over hdfs.
	// IGNORE THIS TEST FOR NOW AS HDFS ACTIONS WILL NOT WORK OVER HDFS SCHEMA 
	// FROM A HADOOP 2 CLUSTER TO A HADOOP 0.23 TARGET CLUSTER.  SINCE ALL GDM 
	// TARGET CLUSTERS ARE CURRENTLY HADOOP 0.23, WE CAN NOT RUN THIS TEST
	// FOR GDM.  ADDITIONALLY, GDM DOES NOT CURRENTLY SHIP HADOOP 2 SYSTEM
	// DEPENDENCIES WITH GDM, SO WE CAN NOT COMPILE HTF AGAINST HADOOP 2
	// SYSTEM DEPENDENCIES TO GET THIS TO WORK FROM A GDM NODE.  WHEN GDM
	// GIVES HADOOP 2 COMPATIBILITY, WE CAN UN-IGNORE THIS TEST.
	@Ignore
	@Test
	public void testDFSCopyLocalFileHadoopqaHdfs() throws Exception {
		// Create a local file
		DFS.createLocalFile(aHadoopqaFileName);
		
		assertTrue("File " + aHadoopqaFileName + " was not created.", 
				DFS.copyFileToRemoteDFS(
						aHadoopqaFileName, 
						aHadoopqaFileName, 
						nameNode, 
						HadooptestConstants.UserNames.HADOOPQA, 
						HadooptestConstants.Location.Keytab.HADOOPQA, 
						HadooptestConstants.Schema.HDFS));
	}

	// A test that deletes a file on a remote DFS as hadoopqa, over hdfs.
	// IGNORE THIS TEST FOR NOW AS HDFS ACTIONS WILL NOT WORK OVER HDFS SCHEMA 
	// FROM A HADOOP 2 CLUSTER TO A HADOOP 0.23 TARGET CLUSTER.  SINCE ALL GDM 
	// TARGET CLUSTERS ARE CURRENTLY HADOOP 0.23, WE CAN NOT RUN THIS TEST
	// FOR GDM.  ADDITIONALLY, GDM DOES NOT CURRENTLY SHIP HADOOP 2 SYSTEM
	// DEPENDENCIES WITH GDM, SO WE CAN NOT COMPILE HTF AGAINST HADOOP 2
	// SYSTEM DEPENDENCIES TO GET THIS TO WORK FROM A GDM NODE.  WHEN GDM
	// GIVES HADOOP 2 COMPATIBILITY, WE CAN UN-IGNORE THIS TEST.
	@Ignore
	@Test
	public void testDFSDeleteFileHadoopqaHdfs() throws Exception {		
		assertTrue("File " + aHadoopqaFileName + " was not deleted.", 
				DFS.deleteFileFromRemoteDFS(
						aHadoopqaFileName, 
						nameNode, 
						HadooptestConstants.UserNames.HADOOPQA, 
						HadooptestConstants.Location.Keytab.HADOOPQA, 
						HadooptestConstants.Schema.HDFS));
	}

	// A test that copies a local system file to a remote DFS as dfsload, 
	// over webhdfs.
	@Test
	public void testDFSCopyLocalFileDfsloadWebhdfs() throws Exception {
		// Create a local file
		DFS.createLocalFile(aDfsloadFileName);		

		assertTrue("File " + aDfsloadFileName + " was not created.", 
				DFS.copyFileToRemoteDFS(
						aDfsloadFileName, 
						aDfsloadFileName, 
						nameNode, 
						HadooptestConstants.UserNames.DFSLOAD, 
						HadooptestConstants.Location.Keytab.DFSLOAD, 
						HadooptestConstants.Schema.WEBHDFS));
	}

	// A test that deletes a file on a remote DFS as dfsload, over webhdfs.
	@Test
	public void testDFSDeleteFileDfsloadWebhdfs() throws Exception {		
		assertTrue("File " + aDfsloadFileName + " was not deleted.", 
				DFS.deleteFileFromRemoteDFS(
						aDfsloadFileName, 
						nameNode, 
						HadooptestConstants.UserNames.DFSLOAD, 
						HadooptestConstants.Location.Keytab.DFSLOAD, 
						HadooptestConstants.Schema.WEBHDFS));
	}

	// A test that copies a local system file to a remote DFS as dfsload, 
	// over hdfs.
	// IGNORE THIS TEST FOR NOW AS HDFS ACTIONS WILL NOT WORK OVER HDFS SCHEMA 
	// FROM A HADOOP 2 CLUSTER TO A HADOOP 0.23 TARGET CLUSTER.  SINCE ALL GDM 
	// TARGET CLUSTERS ARE CURRENTLY HADOOP 0.23, WE CAN NOT RUN THIS TEST
	// FOR GDM.  ADDITIONALLY, GDM DOES NOT CURRENTLY SHIP HADOOP 2 SYSTEM
	// DEPENDENCIES WITH GDM, SO WE CAN NOT COMPILE HTF AGAINST HADOOP 2
	// SYSTEM DEPENDENCIES TO GET THIS TO WORK FROM A GDM NODE.  WHEN GDM
	// GIVES HADOOP 2 COMPATIBILITY, WE CAN UN-IGNORE THIS TEST.
	@Ignore
	@Test
	public void testDFSCopyLocalFileDfsloadHdfs() throws Exception {
		// Create a local file
		DFS.createLocalFile(aDfsloadFileName);

		assertTrue("File " + aDfsloadFileName + " was not created.", 
				DFS.copyFileToRemoteDFS(
						aDfsloadFileName, 
						aDfsloadFileName, 
						nameNode, 
						HadooptestConstants.UserNames.DFSLOAD, 
						HadooptestConstants.Location.Keytab.DFSLOAD, 
						HadooptestConstants.Schema.HDFS));
	}

	// A test that deletes a file on a remote DFS as dfsload, over hdfs.
	// IGNORE THIS TEST FOR NOW AS HDFS ACTIONS WILL NOT WORK OVER HDFS SCHEMA 
	// FROM A HADOOP 2 CLUSTER TO A HADOOP 0.23 TARGET CLUSTER.  SINCE ALL GDM 
	// TARGET CLUSTERS ARE CURRENTLY HADOOP 0.23, WE CAN NOT RUN THIS TEST
	// FOR GDM.  ADDITIONALLY, GDM DOES NOT CURRENTLY SHIP HADOOP 2 SYSTEM
	// DEPENDENCIES WITH GDM, SO WE CAN NOT COMPILE HTF AGAINST HADOOP 2
	// SYSTEM DEPENDENCIES TO GET THIS TO WORK FROM A GDM NODE.  WHEN GDM
	// GIVES HADOOP 2 COMPATIBILITY, WE CAN UN-IGNORE THIS TEST.
	@Ignore
	@Test
	public void testDFSDeleteFileDfsloadHdfs() throws Exception {		
		assertTrue("File " + aDfsloadFileName + " was not deleted.", 
				DFS.deleteFileFromRemoteDFS(
						aDfsloadFileName, 
						nameNode, 
						HadooptestConstants.UserNames.DFSLOAD, 
						HadooptestConstants.Location.Keytab.DFSLOAD, 
						HadooptestConstants.Schema.HDFS));
	}
}

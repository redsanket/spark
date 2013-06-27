
package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.assertTrue;
import hadooptest.TestSession;
import hadooptest.cluster.hadoop.DFS;

import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;

@Category(SerialTests.class)
public class TestMultiClusterCopy extends TestSession {

	private static DFS dfs;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		
		dfs = new DFS();
		
		logger.info("HTTP DEFAULT PORT: " + dfs.getHTTPDefaultPort());
		logger.info("WEBHDFS ENABLED: " + dfs.isWebhdfsEnabled());
		logger.info("WEBHDFS KERBEROS AUTH PRINCIPAL: " + dfs.getWebhdfsKerberosAuthPrincipal());
		logger.info("WEBHDFS KERBEROS AUTH KEYTAB: " + dfs.getWebhdfsKerberosAuthKeytab());
		
		logger.info("COPYING FROM LOCAL FILE");
		dfs.copyLocalToHdfs("/homes/hadoopqa/hadooptest.conf", dfs.getBaseUrl() + "/user/hadoopqa/" + "hadooptest.conf");
		logger.info("FINISHED COPYING FROM LOCAL FILE");
	}

	@Test
	public void copyWithinClusterHDFS() throws Exception {
		dfs.copyDfsToDfs(getFSDefaultName(), "/user/hadoopqa/hadooptest.conf", getFSDefaultName(), "/user/hadoopqa/hadooptest.conf.2");
		
		FileSystem destFS = dfs.getFileSystemFromURI(getFSDefaultName());
		
		dfs.printFsLs(destFS, getFSDefaultName() + "/user/hadoopqa/", true);
		
		assertTrue("File was not successfully copied to remote DFS.", dfs.fileExists(destFS, getFSDefaultName() + "/user/hadoopqa/hadooptest.conf.2"));
	}
	
	@Test
	public void copyBetweenClustersSameGatewayHDFS() throws Exception {
		String strDestFS = "hdfs://gsbl90768.blue.ygrid.yahoo.com:8020";
		
		dfs.copyDfsToDfs(getFSDefaultName(), "/user/hadoopqa/hadooptest.conf", strDestFS, "/user/hadoopqa/hadooptest.conf.2");
		
		FileSystem destFS = dfs.getFileSystemFromURI(strDestFS);
		
		dfs.printFsLs(destFS, strDestFS + "/user/hadoopqa/", true);
		
		assertTrue("File was not successfully copied to remote DFS.", dfs.fileExists(destFS, strDestFS + "/user/hadoopqa/hadooptest.conf.2"));
	}
	 
	@Test
	public void copyBetweenClustersSameGatewayWEBHDFS() throws Exception {
		String strSrcFS = "webhdfs://gsbl90760.blue.ygrid.yahoo.com:1006";
		String strDestFS = "hdfs://gsbl90768.blue.ygrid.yahoo.com:8020";
		
		dfs.copyDfsToDfs(strSrcFS, "/user/hadoopqa/hadooptest.conf", strDestFS, "/user/hadoopqa/hadooptest.conf.2");
		
		FileSystem destFS = dfs.getFileSystemFromURI(strDestFS);
		
		dfs.printFsLs(destFS, strDestFS + "/user/hadoopqa/", true);
		
		assertTrue("File was not successfully copied to remote DFS.", dfs.fileExists(destFS, strDestFS + "/user/hadoopqa/hadooptest.conf.2"));
	}
	
	@Test
	public void copyBetweenClustersSameGatewayHFTP() throws Exception {
		String strSrcFS = "hftp://gsbl90760";
		String strDestFS = "hdfs://gsbl90768.blue.ygrid.yahoo.com:8020";
		
		dfs.copyDfsToDfs(strSrcFS, "/user/hadoopqa/hadooptest.conf", strDestFS, "/user/hadoopqa/hadooptest.conf.2");
		
		FileSystem destFS = dfs.getFileSystemFromURI(strDestFS);
		
		dfs.printFsLs(destFS, strDestFS + "/user/hadoopqa/", true);
		
		assertTrue("File was not successfully copied to remote DFS.", dfs.fileExists(destFS, strDestFS + "/user/hadoopqa/hadooptest.conf.2"));
	}

	private String getFSDefaultName() throws Exception {
		String fsDefaultName = null;
		
		fsDefaultName = cluster.getConf().get("fs.defaultFS");
		
		return fsDefaultName;
	}
	
}

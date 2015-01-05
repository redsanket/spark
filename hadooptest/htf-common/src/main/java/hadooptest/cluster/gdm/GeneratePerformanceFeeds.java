package hadooptest.cluster.gdm;

import hadooptest.TestSession;
import hadooptest.cluster.hadoop.fullydistributed.FullyDistributedExecutor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;


/**
 * 
 * Class that helps in producing performance dataset feed
 *
 */
public class GeneratePerformanceFeeds {
	String feedName;
	String suffix;
	String dataPath;
	final static Charset ENCODING = StandardCharsets.UTF_8;

	public GeneratePerformanceFeeds(String feedName , String suffix) {
		this.feedName  = feedName;
		this.suffix = suffix;
	}

	public void generateFeed(String instanceId) throws Exception {
		createInstanceId(instanceId.trim());
	}

	/**
	 * Create a feed for the specified instance id
	 * @param instanceId
	 * @throws Exception
	 */
	public void createInstanceId(String instanceId) throws Exception {
		String tmpFeedPath = "/tmp/" + this.feedName + "/" + instanceId + "/DONE";
		TestSession.logger.info("tmpFeedPath  = " + tmpFeedPath);
		String sourcePath=null;

		TestSession.logger.info("instanceId  = " +  instanceId);

		if(instanceId.equals("2013010518")) {
			sourcePath = "/grid/0/yroot/var/yroots/fdiserver/tmp/GDMDeploy." + this.suffix.trim() + "/dataset_scripts/ss_search_click_hourly/dataset_0";
		} else if (instanceId.equals("2013010519")) {
			sourcePath = "/grid/0/yroot/var/yroots/fdiserver/tmp/GDMDeploy." + this.suffix.trim() + "/dataset_scripts/ss_search_click_hourly/dataset_2";
		}  else if (instanceId.equals("2013010520")) {
			sourcePath = "/grid/0/yroot/var/yroots/fdiserver/tmp/GDMDeploy." + this.suffix.trim() + "/dataset_scripts/ss_search_click_hourly/dataset_24";
		} else {
			sourcePath = "/grid/0/yroot/var/yroots/fdiserver/tmp/GDMDeploy." + this.suffix.trim() + "/dataset_scripts/ss_search_click_hourly/dataset_2";
		}
		
		TestSession.logger.info("Source Path = " + sourcePath );

		File tmpFeedDir = new File(tmpFeedPath);
		FileUtils.deleteDirectory(tmpFeedDir);
		FileUtils.forceMkdir(tmpFeedDir);

		List<String> rowCountFileContent = new ArrayList<String>();
		FullyDistributedExecutor obj = new FullyDistributedExecutor();

		String []gzFiles = null;

		// copy .gz files
		{
			File f = new File(sourcePath);
			gzFiles = f.list();
			for (String fileNames : gzFiles) {
				TestSession.logger.info("fileNames  = "   + fileNames);
				if ( !fileNames.startsWith("."))  {
					File gzFileName = new File(sourcePath + "/" +fileNames.trim());
					String []s  = obj.runProcBuilder(new String [] {"wc", "-l", gzFileName.toString().trim() });
					for (String s1 : s) {

						if (s1.length() > 1) {
							TestSession.logger.info("s = " + s1);
							s1 = s1.replaceFirst("/grid/0/yroot/var/yroots/fdiserver", "");
							rowCountFileContent.add(s1);
						}
					}
					FileUtils.copyFileToDirectory(gzFileName, tmpFeedDir , true);
				}
			}
		}

		// create ROWCOUNT file
		{
			File rowCountFile = new File(tmpFeedPath + "/" + "ROWCOUNT." + this.feedName + "." + instanceId);
			for (String rowContent : rowCountFileContent) {
				FileUtils.writeStringToFile(rowCountFile, rowContent , true);
			}
		}

		// create STATUS file
		{
			File dstStatusFile = new File(tmpFeedPath + "/"+ "STATUS." + this.feedName + "." + instanceId);
			String statusData = "/tmp/FDIROOT/" + this.feedName + "/" + instanceId + "/DONE/DONE." + this.feedName + "." + instanceId;
			FileUtils.writeStringToFile(dstStatusFile, statusData);
		}

		// create Done file  
		{
			File dstDoneFile = new File(tmpFeedPath +  "/" + "DONE." + this.feedName + "." + instanceId);  

			for (String fileName : gzFiles) {
				TestSession.logger.info("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ fileName   " + fileName);
				String content = "/tmp/FDIROOT/" + this.feedName + "/" + instanceId + "/DONE/" + fileName + "\n";
				TestSession.logger.info("content " + content);
				FileUtils.writeStringToFile(dstDoneFile, content , true);
			}

		}

		// create HEADER file
		{
			File headerFile = new File(tmpFeedPath + "/"+ "HEADER." + this.feedName + "." + instanceId);
			String schemaFilePath =  "/grid/0/yroot/var/yroots/fdiserver/tmp/GDMDeploy." + this.suffix.trim() + "/dataset_scripts/ss_search_click_hourly/schema.dat";
			List<String> schemaFileContent =  readSchemaFile (schemaFilePath);
			writeSchemaFileContent(schemaFileContent , headerFile.toString());
		}

		{
			File finalDir = new File("/grid/0/yroot/var/yroots/fdiserver/tmp/FDIROOT/" + this.feedName + "/" + instanceId);
			FileUtils.forceMkdir(finalDir);
			FileUtils.copyDirectoryToDirectory(tmpFeedDir, finalDir);
		}

	}	 

	/**
	 * Read the specified file content and store them as List<String>
	 * @param aFileName
	 * @return
	 * @throws IOException
	 */
	private List<String> readSchemaFile(String aFileName) throws IOException {
		Path path = Paths.get(aFileName);
		return Files.readAllLines(path, ENCODING);
	}

	/**
	 * Write the content of List<String> to the specified file.
	 * @param aLines
	 * @param aFileName
	 * @throws IOException
	 */
	private void writeSchemaFileContent(List<String> aLines, String aFileName) throws IOException {
		Path path = Paths.get(aFileName);
		Files.write(path, aLines, ENCODING);
	}

}

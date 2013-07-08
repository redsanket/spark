package hadooptest.hadoop.regression.yarn;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import hadooptest.TestSession;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWordCountAPI_Durability extends TestSession {
	
	/****************************************************************
	 *  Please set up input and output directory and file name here *
	 ****************************************************************/
	// NOTE: the file should appear in you home directory
	private static String localFile = "wc_input_new.txt";
	// NOTE: this is a directory and will appear in your home directory in the HDFS
	private static String outputFile = "wc_output_new";
	
	/****************************************************************
	 *          Please give the string for the input file           *
	 ****************************************************************/
	
	private static String input_string = "Hello world, and run Durability Test";
	
	/****************************************************************
	 *  Configure the total file number that you want to generate   *
	 *                       in the HDFS                            *
	 ****************************************************************/
	private static int TotalFileNum = 20;
		
	/****************************************************************
	 *                  Configure the total runtime                 *
	 ****************************************************************/
	
	// location information 
	private static Path inpath = null;
	private static String outputDir = null;
	private static String localDir = null;
	
	int jobNum = 1000;
	
	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
		setupTestDir();
	}
	
public static void setupTestDir() throws Exception {
		
	    FileSystem myFs = TestSession.cluster.getFS();
		
		// show the input and output path
	    // the localDir might be different, check if "/user/", "//Users", or "/home/"
		localDir = "/home/" + System.getProperty("user.name") + "/";
		TestSession.logger.info("Target local Directory is: "+ localDir + "\n" + "Target File Name is: " + localFile);
		
		outputDir = "/user/" + TestSession.conf.getProperty("USER") + "/"; 
		TestSession.logger.info("Target HDFS Directory is: "+ outputDir + "\n" + "Target File Name is: " + outputFile);
		
		inpath = new Path(outputDir+"/"+"wc_input_foo");
		Path infile = null;
		
		// create local input file
		File inputFile = new File(localDir + localFile);
		try{
			if(inputFile.delete()){
				TestSession.logger.info("Input file already exists from previous test, delete it!");
			} else {
				TestSession.logger.info("Input path clear, creating new input file!");
			}
			
			FileUtils.writeStringToFile(new File(localDir + localFile), input_string);
		
		} catch (Exception e) {
			TestSession.logger.error(e);
		}
		
		// Check the valid of the input directory in HDFS
		// check if path exists and if so remove it 
	    try {
	       if ( myFs.isDirectory(inpath) ) {
	         myFs.delete(inpath, true);
	         TestSession.logger.info("INFO: deleted input path: " + inpath );
	       }
	    }
	    catch (Exception e) {
	        System.err.println("FAIL: can not remove the input path, can't run wordcount jobs. Exception is: " + e);
	    }
	    // make the input directory
	    try {
	    	 if ( myFs.mkdirs(inpath) ) {
	    		 TestSession.logger.info("INFO: created input path: " + inpath );
	      }
	    }
	    catch (Exception e) {
	         System.err.println("FAIL: can not create the input path, can't run wordcount jobs. Exception is: " + e);
	    }
	    
	    // Read the local input file
        String s = new Scanner(new File(localDir+localFile) ).useDelimiter("\\A").next();
        TestSession.logger.info("Input string is: "+s);  
		
		
		for(int fileNum = 0; fileNum < TotalFileNum; fileNum ++)
		{
			try {
				 infile = new Path(inpath.toString() + "/" + Integer.toString(fileNum) + ".txt" );
		         FSDataOutputStream dostream = FileSystem.create(myFs, infile, new FsPermission("644")); 
		          
		         // generate a set of different input files
		         for(int i= 0; i < 25*fileNum; i++)
		         		dostream.writeChars(s);
		          	
		         dostream.flush();
		         dostream.close();
		    } catch (IOException ioe) {
		        	System.err.println("FAIL: can't create input file for wordcount: " + ioe);
		    }
		}
		// Delete the file, if it exists in the same directory
		TestSession.cluster.getFS().delete(new Path(outputDir+outputFile), true);
	}

	/*
	 * A test for running a Wordcount job
	 * 
	 * Equivalent to JobSummaryInfo10 in the original shell script YARN regression suite.
	 */
	@Test
	public void runWordCountTest() {
		try {
			String[] args = {Integer.toString(jobNum), "hdfs://gsbl90628.blue.ygrid.yahoo.com:8020/user/hadoopqa/wc_input_foo/1.txt", outputDir + outputFile, "wc_foo"};
//			String[] args = {"-in", outputDir + localFile, "-out", outputDir + outputFile, "-name", "HEHE"};
			Configuration conf = TestSession.cluster.getConf();

			int rc;
			TestSession.cluster.setSecurityAPI("keytab-hadoopqa", "user-hadoopqa");
			rc = ToolRunner.run(conf, new WordCountAPIJob(), args);
			if (rc != 0) {
				TestSession.logger.error("Job failed!!!");
			}
		}
		catch (Exception e) {
			TestSession.logger.error("Exception failure.", e);
			fail();
		}
	}
}

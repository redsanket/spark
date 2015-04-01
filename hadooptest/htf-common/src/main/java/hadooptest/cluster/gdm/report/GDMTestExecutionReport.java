package hadooptest.cluster.gdm.report;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.runner.Description;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import static org.junit.Assert.assertTrue;

/**
 * Class that generates report dynamically while the test is executing.
 * 
 */
public class GDMTestExecutionReport extends RunListener {

	private String filePath;
	private String resultFolderPath;
	private String reportFilePath;
	private String testResult;
	private String failureExeceptionString;
	private String stackTrace;
	private static int slNo = 1;
	private static String methodName;
	private static int count;

	GDMTestExecutionReport() throws IOException {
		this.filePath = new File("").getAbsoluteFile() + File.separator;
		this.resultFolderPath = filePath + "resources" + File.separator + "gdm" + File.separator + "results";
		this.reportFilePath = resultFolderPath + File.separator + "Result.html";
	}

	@Override
	public void testRunStarted(Description description) throws Exception {
		if (this.slNo == 1) {
			startGDMTestReport();
		}
	}

	@Override
	public void testRunFinished(Result result) throws Exception {
		if (this.slNo >=  result.getRunCount() ) {
			stopGDMTestReport();
		}
	}

	@Override
	public void testStarted(Description description) throws Exception {
		this.testResult = "passed";
		System.out.println("*************************************************Started: " + description.getMethodName());
	}

	@Override
	public void testFinished(Description description) throws Exception {
		System.out.println("*************************************************Finished: " + description.getMethodName());
		StringBuilder rowTag = new StringBuilder();
		String className = description.getClassName();
		String mName = description.getMethodName();
		if ((this.slNo % 2) == 1) {
			rowTag.append("<tr class=\"pure-table-odd\">");
		} else {
			rowTag.append("<tr>");
		}

		// used static count since  testStarted and testFinished is invoked multiple times and getMethodName() is also invoked so many times. 
		if (count == 0) {
			this.methodName = mName;
			if (this.testResult.equals("failed")) {
				rowTag.append("<td> <font size=\"2\">").append(className).append("</font> </td>").append("<td> <font size=\"2\">").append(this.methodName).append("</font> </td>").
				append("<td> <font color=\"red\" size=\"2\">").append("Failed").append(" </font> </td>").append("<td>  <font size=\"2\">").append(this.failureExeceptionString).append("</font> </td>").append("<td>  <font size=\"2\">").append(this.stackTrace).append("</font> </td>").append("</tr> \n");
			} else if (this.testResult.equals("igorned")) {
				rowTag.append("<td> <font size=\"2\">").append(className).append("</font> </td>").append("<td> <font size=\"2\">").append(this.methodName).append("</font> </td>").
				append("<td> <font color=\"red\">  size=\"2\">").append("Igorned").append(" </font> </td>").append("<td>  <font size=\"2\">").append("igorned").append(" </font> </td>").append("<td>").append("</td>").append("</tr> \n");
			} else if (this.testResult.equals("passed")) {
				rowTag.append("<td> <font size=\"2\">").append(className).append("</font> </td>").append("<td> <font size=\"2\">").append(this.methodName).append("</font> </td>").
				append("<td> <font color=\"green\" size=\"2\" >").append("Passed").append(" </font> </td>").append("<td>").append("</td>").append("<td>").append("</td>").append("</tr> \n");
			}
			FileUtils.writeStringToFile(new File(this.reportFilePath), rowTag.toString() , "UTF8" ,  true);
			this.slNo ++;
			rowTag = null;
			this.testResult = "";
			count++;
		} else if (count == 1) {
			count = 0;
		}
	}

	@Override
	public void testFailure(Failure failure) throws Exception {
		this.testResult = "failed";
		this.failureExeceptionString = failure.getException().toString();
		this.stackTrace = failure.getTrace();
		System.out.println("Failed: " + failure.getDescription().getMethodName());
	}

	@Override
	public void testAssumptionFailure(Failure failure) {
		this.testResult = "fail";
		System.out.println("Failed: " + failure.getDescription().getMethodName());
	}

	@Override
	public void testIgnored(Description description) throws Exception {
		this.testResult = "igorned";
		System.out.println("Ignored: " + description.getMethodName());
	}

	/*
	 * Close the html report file
	 */
	private void stopGDMTestReport() throws Exception {
		String htmlCloseTag = "</tbody></table></center><br><br></body></html>";
		try {
			FileUtils.writeStringToFile(new File(this.reportFilePath), htmlCloseTag , "UTF8" ,  true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Create a result folder , Result.html file and add the html tag when test execution starts 
	 */
	private void startGDMTestReport() {
		File resultFolder = new File(this.resultFolderPath);
		if ( !resultFolder.exists() ) {
			boolean isDirCreated = resultFolder.mkdir();
			assertTrue("Failed : To create " + resultFolderPath  , isDirCreated == true);
			System.out.println("isDirCreated = "  + isDirCreated  +  "  path = " + resultFolderPath);
			File reportFile = new File(reportFilePath);
			try {
				boolean  isReportFileCreated = reportFile.createNewFile();
				assertTrue("Failed : To create " + reportFilePath , isReportFileCreated);
				String htmlTag = "<html><head> <link rel=\"stylesheet\" href=\"http://yui.yahooapis.com/pure/0.6.0/pure-min.css\"> <meta http-equiv=\"refresh\" content=\"10\" > </head> " +
						"<body><center><h2> GDM Test Result </h2>" + "<table class=\"pure-table pure-table-bordered\">" +
						"<tr>  <th>TestCase Name </th> <th>Method Name </th> <th>Test Result </th> <th>Failure reason </th> <th> Comments </th> </tr> </thead> <tbody> \n" ;
				FileUtils.writeStringToFile(new File(this.reportFilePath), htmlTag , "UTF8" );
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}

package hadooptest.gdm.regression.integration.report;


import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import hadooptest.TestSession;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 *  Fetch last 3 hr or 3 iteration results from database, create a html report & attach the report for the mail and send to hadoop-hit@yahoo-inc.com  
 */
public class SendIntegrationReportMail {

	private static final String TO = "hadoop-hit@yahoo-inc.com";
	private static final String FROM = "hadoopqa@yahoo-inc.com";
	private static final String SMTP = "mtarelay.ops.yahoo.net";
	
	public static void main(String... args) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException, MessagingException {
		SendIntegrationReportMail obj = new SendIntegrationReportMail();
		obj.sendMail();
	}

	public String createHtmlPage() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		String report = this.getRecords();
		if (report.equalsIgnoreCase("failed")) {
			report  = "<h3>  Failed to fetch the result from db </h3";
		}
		StringBuilder htmlBuilder = new StringBuilder("<html>");
		htmlBuilder.append("<head>").
		append("</head>").append("<body>").append("<center>").append("<h3>").append("Test Report for ").append(this.getTodaysDate()).append("</h3").append("</center>").
		append(report).
		append("</body>").append("</html>");
		return htmlBuilder.toString();
	}

	public void sendMail() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, MessagingException {
		String host = "localhost";

		//Get the session object
		Properties properties = System.getProperties();
		properties.setProperty("mail.smtp.host", SMTP );
		Session session = Session.getDefaultInstance(properties);

		MimeMultipart multipart = new MimeMultipart("related");
		BodyPart messageBodyPart = new MimeBodyPart();
		String htmlText = this.createHtmlPage();

		System.out.println("htmlText  = " + htmlText);
		messageBodyPart.setContent(htmlText, "text/html");
		multipart.addBodyPart(messageBodyPart);

		// second part (the image)
		messageBodyPart = new MimeBodyPart();
		DataSource failIcon = new FileDataSource(this.getImagePath("cross.png"));
		DataSource passIcon = new FileDataSource(this.getImagePath("check.png"));

		messageBodyPart.setDataHandler(new DataHandler(failIcon));
		messageBodyPart.setDataHandler(new DataHandler(passIcon));
		messageBodyPart.addHeader("Content-ID", "<image>");
		messageBodyPart.addHeader("Content-ID", "<image1>");

		// add image to the multipart
		multipart.addBodyPart(messageBodyPart);

		//compose the message
		MimeMessage message = new MimeMessage(session);
		message.setFrom(new InternetAddress(FROM));
		message.addRecipient(Message.RecipientType.TO, new InternetAddress(TO));
		message.setSubject("Integration test result for " + this.getTodaysDate());
		message.setContent(multipart);

		// Send message
		Transport.send(message);
		TestSession.logger.info("Send mail successfully....");
	}

	public String getImagePath(String imageName) {
		File file = new File(new File("").getAbsolutePath() + File.separator + "resources/gdm/images"  + File.separator + imageName);
		System.out.println("image path = " + file.toString());
		if (file.exists()) {
			return file.toString();
		}
		return null;
	}

	public Connection getConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		final String DRIVER = "com.mysql.jdbc.Driver";
		final String DB_NAME = "integration_test";
		Class.forName(DRIVER).newInstance();
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" + DB_NAME, "root", "");
		if (con != null) {
			return con;
		} else {
			TestSession.logger.info("Failed to open  the connection.");
			return con;
		}
	}

	public String getRecords() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
		StringBuilder tableBuilder = null;
		Connection con = this.getConnection();
		String QUERY = "SELECT distinct * FROM integration_test where dataSetName like \"%" + this.getTodaysDate() + "%\"  order by dataSetName desc limit 3";
		System.out.println("QUERY  - " + QUERY);
		if (con != null) {
			Statement stmt = con.createStatement();
			ResultSet resultSet = stmt.executeQuery(QUERY);
			if (resultSet != null) {
				tableBuilder = new StringBuilder("<table border=\"1\"> ");
				tableBuilder.append("<thead>")
				.append("<tr>")
				.append("<th>").append("DataSetName").append("</th>")
				.append("<th>").append("HadoopVersion").append("</th>")
				.append("<th>").append("GDM").append("</th>")
				.append("<th>").append("PIG").append("</th>")
				.append("<th>").append("Tez").append("</th>")
				.append("<th>").append("Hive").append("</th>")
				.append("<th>").append("HCatalog").append("</th>")
				.append("<th>").append("HBase").append("</th>")
				//.append("<th>").append("Oozie").append("</th>")
				.append("</tr>")
				.append("</thead>").append("<tbody>");
				while (resultSet.next()) {
					int rowNo = resultSet.getRow();
					System.out.println("rowNo = " + rowNo);
					StringBuffer dataSetName = this.getResult(resultSet.getString("dataSetName"), 0);
					StringBuffer hadoopVersion = this.getResult(resultSet.getString("hadoopVersion"), 0);
					StringBuffer dataAvailable = this.getResult(resultSet.getString("dataAvailable"), 0);

					// gdm result
					StringBuffer gdmVersion = this.getResult(resultSet.getString("gdmVersion"), 1);
					if ((dataAvailable.indexOf("AVAILABLE") > -1) || (dataAvailable.indexOf("MISSED_SLA") > -1)) {
						dataAvailable = dataAvailable.delete(0, dataAvailable.length());
						dataAvailable.append("pass").append("~").append(gdmVersion);
					} else {
						dataAvailable.append("fail").append("~").append(gdmVersion);
					}

					// tez result
					String tez = resultSet.getString("tez");
					String tezVersion = resultSet.getString("tezVersion");
					StringBuffer pigVersion = this.getResult(resultSet.getString("pigVersion"), 1);
					StringBuffer pigResult = new StringBuffer();
					StringBuffer tezResult = this.getResult(tez, 0);
					if (tezResult.indexOf("PASS") > -1) {
						tezResult.append("~").append(tezVersion);
						pigResult.append("PASS").append("~").append(pigVersion);
					} else if ((tezResult.indexOf("UNKNOWN") > -1) || (tezResult.indexOf("FAIL") > -1)) {
						tezResult.append("~").append(tezVersion).append("~").append(this.getResult(tez, 1));
						pigResult.append("FAIL").append("~").append(pigVersion);
					}

					// hive result
					StringBuffer hiveTableDeleted = this.getResult(resultSet.getString("hiveTableDeleted"), 0);
					StringBuffer hiveTableCreate = this.getResult(resultSet.getString("hiveTableCreate"), 0);
					StringBuffer hiveLoadData = this.getResult(resultSet.getString("hiveLoadData"), 0);
					StringBuffer hiveVersion = this.getResult(resultSet.getString("hiveVersion"), 1);
					StringBuffer hiveResult = new StringBuffer();
					if ((hiveTableDeleted.indexOf("UNKNOWN") > -1) || (hiveTableDeleted.indexOf("FAIL") > -1) ||
							(hiveTableCreate.indexOf("UNKNOWN") > -1) || (hiveTableCreate.indexOf("FAIL") > -1) ||
							(hiveLoadData.indexOf("UNKNOWN") > -1) || (hiveLoadData.indexOf("FAIL") > -1)) {
						hiveResult.append("FAIL").append("~").append(hiveVersion);
					} else {
						hiveResult.append("PASS").append("~").append(hiveVersion);
					}

					// hcat result
					StringBuffer hcat = this.getResult(resultSet.getString("hcat"), 0);
					StringBuffer hcatVersion = this.getResult(resultSet.getString("hcatVersion"), 1);
					StringBuffer hcatResult = new StringBuffer();
					if ((hcat.indexOf("UNKNOWN") > -1) || (hcat.indexOf("FAIL") > -1)) {
						hcatResult.append("FAIL").append("~").append(hcatVersion);
					} else {
						hcatResult.append("PASS").append("~").append(hcatVersion);
					}

					// hbase result
					StringBuffer hbaseDeleteTable = this.getResult(resultSet.getString("hbaseDeleteTable"), 0);
					StringBuffer hbaseCreateTable = this.getResult(resultSet.getString("hbaseCreateTable"), 0);
					StringBuffer hbaseInsert = this.getResult(resultSet.getString("hbaseInsert"), 0);
					StringBuffer hbaseScan = this.getResult(resultSet.getString("hbaseScan"), 0);
					StringBuffer hbaseVersion = this.getResult(resultSet.getString("hbaseVersion"), 1);
					StringBuffer hbaseResult = new StringBuffer();
					if ((hbaseDeleteTable.indexOf("UNKNOWN") > -1) || (hbaseDeleteTable.indexOf("FAIL") > -1) ||
							(hbaseCreateTable.indexOf("UNKNOWN") > -1) || (hbaseCreateTable.indexOf("FAIL") > -1) ||
							(hbaseInsert.indexOf("UNKNOWN") > -1) || (hbaseInsert.indexOf("FAIL") > -1) ||
							(hbaseScan.indexOf("UNKNOWN") > -1) || (hbaseScan.indexOf("FAIL") > -1)) {
						hbaseResult.append("FAIL").append("~").append(hiveVersion);
					} else {
						hbaseResult.append("PASS").append("~").append(hiveVersion);
					}

					// oozie result
					// purposefully commented, due to oozie environment issue.
				/*	StringBuffer oozieJobStarted = this.getResult(resultSet.getString("oozieJobStarted"), 0);
					StringBuffer cleanUpOutput = this.getResult(resultSet.getString("cleanUpOutput"), 0);
					StringBuffer pigRawProcessor = this.getResult(resultSet.getString("pigRawProcessor"), 0);
					StringBuffer checkInput = this.getResult(resultSet.getString("checkInput"), 0);
					StringBuffer hiveStorage = this.getResult(resultSet.getString("hiveStorage"), 1);
					StringBuffer hiveVerify = this.getResult(resultSet.getString("hiveVerify"), 1);
					StringBuffer oozieJobCompleted = this.getResult(resultSet.getString("oozieJobCompleted"), 1);
					StringBuffer oozieVersion = this.getResult(resultSet.getString("oozieVersion"), 1);
					StringBuffer oozieResult = new StringBuffer();
					if ((oozieJobStarted.indexOf("UNKNOWN") > -1) || (oozieJobStarted.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ||
							(cleanUpOutput.indexOf("UNKNOWN") > -1) || (cleanUpOutput.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ||
							(pigRawProcessor.indexOf("UNKNOWN") > -1) || (pigRawProcessor.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ||
							(checkInput.indexOf("UNKNOWN") > -1) || (checkInput.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ||
							(hiveStorage.indexOf("UNKNOWN") > -1) || (hiveStorage.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ) {
						(hiveVerify.indexOf("UNKNOWN") > -1) || (hiveVerify.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1) ||
						(oozieJobCompleted.indexOf("UNKNOWN") > -1) || (oozieJobCompleted.indexOf("FAIL") > -1) || (oozieJobStarted.indexOf("ERROR") > -1)) {
							oozieResult.append("FAIL").append("~").append(oozieVersion);
						} else {
							oozieResult.append("PASS").append("~").append(oozieVersion);
						}*/

						TestSession.logger.info("dataSetName  - " + dataSetName + " dataAvailable = " + dataAvailable + " hadoopVersion " + hadoopVersion + "   pig " + pigResult.toString() + " tez = " + tezResult + " hiveResult = " + hiveResult
								+ "  hcatResult = " + hcatResult + "   hbaseResult = " + hbaseResult /* + "   oozieResult  - " + oozieResult */);
						tableBuilder.append(this.constructTableRow(dataSetName.toString(), hadoopVersion.toString(), dataAvailable.toString(), pigResult.toString(), tezResult.toString(), hiveResult.toString(),
								hcatResult.toString(), hbaseResult.toString() /*, oozieResult.toString() */ ));
					}
					tableBuilder.append("</tbody>").append("</table>");
				} else {
					TestSession.logger.info("Failed to execute " + QUERY + " query.");
					return "failed";
				}
			} else {
				TestSession.logger.info("Failed to create the connection to the database.");
				return "failed";
			}
			TestSession.logger.info("table - " + tableBuilder.toString());
			return tableBuilder.toString();
		}

		public String constructTableRow(String... values) {
			StringBuilder rowTagBuilder = new StringBuilder("<tr>");
			for (String value : values) {
				TestSession.logger.info("value = " + value);
				if (value.indexOf("~") > -1) {
					List<String> resultList = Arrays.asList(value.split("~"));
					if (resultList.get(0).trim().equalsIgnoreCase("fail") || resultList.get(1).trim().equalsIgnoreCase("UNKNOWN")) {
						rowTagBuilder.append("<td>").append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />").append(resultList.get(1)).append("</td>");
					} else if (resultList.get(0).trim().equalsIgnoreCase("pass")) {
						rowTagBuilder.append("<td>").append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />").append(resultList.get(1)).append("</td>");
					}
				} else {
					rowTagBuilder.append("<td>").append(value).append("</td>");
				}
			}
			rowTagBuilder.append("</tr>");
			return rowTagBuilder.toString();
		}

		public StringBuffer getResult(String resultValue, int position) {
			TestSession.logger.info("resultValue  = " + resultValue);
			int indexOf = resultValue.indexOf("~");
			if (indexOf == -1) {
				return new StringBuffer(resultValue);
			} else {
				List<String> resultList = Arrays.asList(resultValue.split("~"));
				return new StringBuffer(resultList.get(position));
			}
		}
		
		public String getTodaysDate() {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
			simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			Calendar calendar = Calendar.getInstance();
			String todaySDate = simpleDateFormat.format(calendar.getTime());
			return todaySDate.trim();
		}
	}

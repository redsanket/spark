// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.stackIntegration.lib;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import hadooptest.TestSession;
import hadooptest.cluster.gdm.GdmUtils;
import hadooptest.gdm.regression.stackIntegration.db.AggIntResult;
import hadooptest.gdm.regression.stackIntegration.db.DataBaseOperations;


public class SendIntegrationResultMail {
    
    private AggIntResult aggIntResultObject;
    private DataBaseOperations dbOperations ;
    private static final String TO = "hadoop-hit@yahoo-inc.com";
    private static final String FROM = "hadoopqa@yahoo-inc.com";
    private static final String SMTP = "mtarelay.ops.yahoo.net";

    public SendIntegrationResultMail() {
        this.aggIntResultObject = new AggIntResult();
    }
    
    public String getImagePath(String imageName) {
        File file = new File(new File("").getAbsolutePath() + File.separator + "resources/gdm/images"  + File.separator + imageName);
        TestSession.logger.info("image path = " + file.toString());
        if (file.exists()) {
            return file.toString();
        }
        return null;
    }

    public void sendMail()  throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, MessagingException{
        String host = "localhost";

        //Get the session object
        Properties properties = System.getProperties();
        properties.setProperty("mail.smtp.host", SMTP );
        Session session = Session.getDefaultInstance(properties);

        MimeMultipart multipart = new MimeMultipart("related");
        BodyPart messageBodyPart = new MimeBodyPart();
        String htmlText = this.createHtmlPage();

        TestSession.logger.info("htmlText  = " + htmlText);
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
        String pipeLineName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName");
        message.setSubject(pipeLineName + " Integration test result for " + this.getTodaysDate());
        message.setContent(multipart);

        // Send message
        Transport.send(message);
        TestSession.logger.info("Send mail successfully....");
    }

    public String createHtmlPage() {
    	String pipeLineName = GdmUtils.getConfiguration("testconfig.TestWatchForDataDrop.pipeLineName");
    	TestSession.logger.info("SendIntegrationResultMail - pipeLineName  = " + pipeLineName);
        java.util.List<String> versionList = this.aggIntResultObject.getToDaysResult();
        StringBuilder builder = new StringBuilder();
        if (versionList != null) {
            for (String version : versionList) {
                String SELECT_QUERY = "select * from integrationFinalResult  where " + pipeLineName +"Version like  "  + "\"" + version +  "\"";
                try {
                    builder.append(this.getRecords(SELECT_QUERY));
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        StringBuilder htmlBuilder = new StringBuilder("<html>");
        htmlBuilder.append("<head>").
        append("</head>").append("<body>").append("<center>").append("<h3>").append("Test Report for ").append(this.getTodaysDate()).append("</h3").append("</center>").
        append(builder.toString()).
        append("</body>").append("</html>");
        return htmlBuilder.toString();
    }

    public String getTodaysDate() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Calendar calendar = Calendar.getInstance();
        String todaySDate = simpleDateFormat.format(calendar.getTime());
        return todaySDate.trim();
    }

    public String getRecords(String query) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
        StringBuilder tableBuilder = null;
        this.dbOperations = new DataBaseOperations();
        Connection con = this.dbOperations.getConnection();
        TestSession.logger.info("QUERY  - " + query);
        if (con != null) {
            Statement stmt = con.createStatement();
            ResultSet resultSet = stmt.executeQuery(query);
            if (resultSet != null) {
                tableBuilder = new StringBuilder("<table border=\"1\"> ");
                tableBuilder.append("<thead>")
                .append("<tr>")
                .append("<th>").append("DataSetName").append("</th>")
                .append("<th>").append("Date").append("</th>")
                .append("<th>").append("HadoopVersion").append("</th>")
                .append("<th>").append("GDM").append("</th>")
                .append("<th>").append("Tez").append("</th>")
                .append("<th>").append("Hive").append("</th>")
                .append("<th>").append("HCatalog").append("</th>")
                .append("<th>").append("HBase").append("</th>")
                .append("<th>").append("Oozie").append("</th>")
                .append("<th>").append("Comments").append("</th>")
                .append("</tr>")
                .append("</thead>").append("<tbody>");
                while (resultSet.next()) {

                    tableBuilder.append("<tr>");

                    String dataSetName = resultSet.getString("dataSetName");
                    tableBuilder.append("<td>").append(dataSetName).append("</td>");

                    String date = resultSet.getString("date");
                    tableBuilder.append("<td>").append(date).append("</td>");

                    String hadoopVersion = resultSet.getString("hadoopVersion");
                    if (!(hadoopVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(hadoopVersion);  
                    } else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String hadoopResult = resultSet.getString("hadoopResult");
                    if (hadoopResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (hadoopResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");

                    String gdmVersion = resultSet.getString("gdmVersion");
                    if (!(gdmVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(gdmVersion); 
                    } else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String gdmResult = resultSet.getString("gdmResult");
                    if (gdmResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (gdmResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");

                    String tezVersion = resultSet.getString("tezVersion");
                    if (!(tezVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(tezVersion); 
                    } else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String tezResult = resultSet.getString("tezResult");
                    if (tezResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (tezResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");


                    String hiveVersion = resultSet.getString("hiveVersion");
                    if (!(hiveVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(hiveVersion);
                    } else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String hiveResult = resultSet.getString("hiveResult");
                    if (hiveResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (hiveResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");


                    String hcatVersion = resultSet.getString("hcatVersion");
                    if (!(hcatVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(hcatVersion);
                    }else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String hcatResult = resultSet.getString("hcatResult");
                    if (hcatResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (hcatResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");

                    String hbaseVersion = resultSet.getString("hbaseVersion");
                    if (!(hbaseVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(hbaseVersion);
                    }else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String hbaseResult = resultSet.getString("hbaseResult"); 
                    if (hbaseResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (hbaseResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");

                    String oozieVersion = resultSet.getString("oozieVersion"); 
                    if (!(oozieVersion.indexOf("UNKNOWN") > -1)) {
                        tableBuilder.append("<td>").append(oozieVersion);
                    } else {
                        tableBuilder.append("<td>").append("-");
                    }
                    
                    String oozieResult = resultSet.getString("oozieResult");
                    if (oozieResult.indexOf("PASS") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"cid:image\" title=\"Cluster is active\" />");
                    } if (oozieResult.indexOf("FAIL") > -1) {
                        tableBuilder.append("<span>\t<img style=\"float: center; margin: 0px 0px 10px 10px;\" src=\"https://cdn2.iconfinder.com/data/icons/oxygen/16x16/actions/no.png\" width=\"20\" title=\"Cluster is active\" />");
                    }
                    tableBuilder.append("</td>");

                    String comments = resultSet.getString("comments");

                    tableBuilder.append("<td>").append(comments).append("</td>");
                    tableBuilder.append("</tr>");
                }
                tableBuilder.append("</tbody>").append("</table>");
            } else {
                TestSession.logger.info("Failed to execute " + query + " query.");
                return "failed";
            }
        } else {
            TestSession.logger.info("Failed to create the connection to the database.");
            return "failed";
        }
        TestSession.logger.info("table - " + tableBuilder.toString());
        return tableBuilder.toString();
    }

}

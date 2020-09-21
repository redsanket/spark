package com.yahoo.apollo.bpm_client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.net.SocketException;
import javax.net.ssl.SSLException;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.StringJoiner;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.nio.charset.StandardCharsets;
// logger
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
// Config
import org.apache.hadoop.conf.Configuration;
// HDFS client
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.io.Text;
// FTPSClient
import org.apache.commons.net.ftp.FTPSClient;
import org.apache.commons.net.ftp.FTPClient;

/**
 * Worker for pulling files from the BPM FTP to HDFS.
 * Searches the apollo.bpm_downloader.ftp.dir directory on the FTP server for the following files:
 *   Bing_Clicks_report_$DATE.csv
 *   Clicks_report_$DATE.csv
 *   YahooGemini_Clicks_report_$DATE.csv
 * $DATE is formatted like {weekday}_{dayOfMonth}_{MonthName}_{Year}
 * Example:
 *   Bing_Clicks_report_Sunday_20_May_2018.csv
 *   Clicks_report_Sunday_20_May_2018.csv
 *   YahooGemini_Clicks_report_Sunday_20_May_2018.csv
 *
 * Required configuration:
 *   apollo.bpm_downloader.batchdate
 *   apollo.bpm_downloader.hdfsOutputDir
 *   apollo.bpm_downloader.ftp.host
 *   apollo.bpm_downloader.ftp.user
 *   apollo.bpm_downloader.ftp.dir
 *   apollo.bpm_downloader.ykeykey.clientSecretName
 *
 * Optional configuration:
 *   apollo.bpm_downloader.waitSec
 *   apollo.bpm_downloader.timeoutSec
 **/
public class BpmDownloader {
  public static final Log LOG = LogFactory.getLog(BpmDownloader.class);
  private static final String OOZIE_CONFIG_PROPERTY = "oozie.action.conf.xml";
  // Date formats
  private static final String YMD_DATE_FORMAT = "yyyyMMdd";
  private static final String FILE_DATE_FORMAT = "EEEE_d_MMMM_yyyy";
  // all settings for this class must start with this string
  private static final String CONFIG_NAMESPACE = "apollo.bpm_downloader.";

  // Job config
  private Configuration conf;
  // FTP settings
  private String ftpHost;
  private String ftpUsername;
  private String ftpDir;
  // YKeyKey settings
  private String ykeykeySecretName;
  // Map of file name to flag identifying whether the file has been downloaded
  private Map<String, Boolean> targetFiles;
  // Number of seconds to wait for data to appear before failing
  private int timeoutSec;
  // Number of seconds to wait between checks for data
  private int waitSec;
  // Output location on HDFS
  private String hdfsOutputDir;
  private boolean deleteLocalFiles = true;
  private boolean overwriteHdfs = true;

  /**
   * Constructor
   **/
  protected BpmDownloader(Configuration conf) throws IOException {
    this.conf = conf;
    // FTP settings
    ftpHost = getRequiredConf(conf, CONFIG_NAMESPACE+"ftp.host"); // dropbox.yahoo.com
    ftpUsername = getRequiredConf(conf, CONFIG_NAMESPACE+"ftp.user"); // bpmapo
    ftpDir = getRequiredConf(conf, CONFIG_NAMESPACE+"ftp.dir"); // feedback
    // YKeyKey settings
    ykeykeySecretName = getRequiredConf(conf, CONFIG_NAMESPACE+"ykeykey.clientSecretName"); // apollo_dev.bpm.password
    // Query settings
    String targetDateStr = getRequiredConf(conf, CONFIG_NAMESPACE+"batchdate"); // 20180531
    // HDFS output settings
    hdfsOutputDir = getRequiredConf(conf, CONFIG_NAMESPACE+"hdfsOutputDir");
    // Timeout settings. Not required.
    waitSec = conf.getInt(CONFIG_NAMESPACE+"waitSec", 300); // default to 5 min
    timeoutSec = conf.getInt(CONFIG_NAMESPACE+"timeoutSec", 900); // default to 15 min
    // Sanity check timeout settings
    if (waitSec < 10) {
      throw new IOException("Invalid wait time '"+waitSec+"' specified. Must be >= 10.");
    }
    if (timeoutSec < 1) {
      throw new IOException("Invalid timeout '"+timeoutSec+"' specified. Must be >= 1.");
    }

    // Populate targetFiles with the list of files for the current batchdate
    // Example:
    //   Bing_Clicks_report_Sunday_20_May_2018.csv
    //   Clicks_report_Sunday_20_May_2018.csv
    //   YahooGemini_Clicks_report_Sunday_20_May_2018.csv
    try {
      SimpleDateFormat ymdFormat = new SimpleDateFormat(YMD_DATE_FORMAT);
      Date targetDate = ymdFormat.parse(targetDateStr);
      SimpleDateFormat fileFormat = new SimpleDateFormat(FILE_DATE_FORMAT);
      String fileDateStr = fileFormat.format(targetDate);

      targetFiles = new HashMap<>();
      targetFiles.put("Clicks_report_"+fileDateStr+".csv", Boolean.FALSE);
      targetFiles.put("Bing_Clicks_report_"+fileDateStr+".csv", Boolean.FALSE);
      targetFiles.put("YahooGemini_Clicks_report_"+fileDateStr+".csv", Boolean.FALSE);
    } catch (ParseException e) {
      throw new IOException("Failed to reformat date:"+e.getMessage(), e);
    }
  }

  /**
   * Gets a required setting from the input Configuration.
   * @param jobConf
   * @param key
   * @throws IOException if the setting is not defined
   **/
  protected String getRequiredConf(Configuration jobConf, String key) throws IOException {
    if (jobConf == null) {
      throw new IOException("Null Configuration passed to BpmDownloader");
    }
    String value = jobConf.get(key);
    if (value == null) {
      throw new IOException("Missing required value '"+key+"' in job configuration");
    }
    return value;
  }

  /**
   * Run the BPM downloader.
   * Connects to the FTP server and downloads the files specified by the input configuration.
   *
   * @throws IOException for SSH or FTP errors or if the required files are not found before the timeout.
   *         InterruptedException for errors in Thread.sleep()
   **/
  protected void run() throws IOException, InterruptedException {
    FTPSClient ftp = null;
    try {
      // Look for target files until they appear or we reach the timeout
      long endTimeMillis = System.currentTimeMillis() + (timeoutSec * 1000);
      while (missingFileCount() > 0) {
        // Open the FTP connection
        ftp = getFTPSClient(ftpHost, ftpUsername, getSecret(ykeykeySecretName), ftpDir);

        // Check for each file that hasn't been downloaded yet
        for(String fileName : targetFiles.keySet()) {
          if (targetFiles.get(fileName)) {
            LOG.info("Already found '"+fileName+"'. Skipping");
          } else {
            LOG.info("ls "+fileName);
            String[] foundFile = ftp.listNames(fileName);
            LOG.info("FTP reply: '"+ftp.getReplyString()+"'");

            if (foundFile != null && foundFile.length > 0) {
              // Found a file. Download it and mark it as complete.
              if (getFile(ftp, fileName)) {
                targetFiles.put(fileName, Boolean.TRUE);
              }
            }
          }
        }
        // Close the FTP connection to avoid keeping it open for hours
        ftp.disconnect();
        LOG.info("Closing FTP connection");

        // If we are still missing files, either throw an exception to stop
        // or sleep before checking again.
        if (missingFileCount() > 0) {
          if(System.currentTimeMillis() > endTimeMillis) {
            StringJoiner missingFiles = new StringJoiner(",");
            for(Map.Entry<String, Boolean> entry : targetFiles.entrySet()) {
              if (entry.getValue() == Boolean.FALSE) {
                missingFiles.add(entry.getKey());
              }
            }
            throw new IOException("Failed to find file(s): "+missingFiles.toString()
                +" after "+timeoutSec+" seconds");
          }

          LOG.warn("Still waiting for "+missingFileCount()+" files. Sleeping for "
              +waitSec+" seconds...");
          Thread.sleep(waitSec*1000);
        }
      }

      LOG.info("Successfully downloaded all files");

      // Upload files to HDFS
      FileSystem fs = getFileSystem(conf);
      Path outputDir = new Path(hdfsOutputDir);
      fs.mkdirs(outputDir);
      for(String file : targetFiles.keySet()) {
        fs.copyFromLocalFile(deleteLocalFiles, overwriteHdfs, new Path(file), outputDir);
        LOG.info("Successfully uploaded '"+file+"' to HDFS dir '" +hdfsOutputDir+"'");
      }

      LOG.info("FTP download completed successfully");
    } finally {
      if (ftp != null && ftp.isConnected()) {
        ftp.disconnect();
        LOG.info("FTP channel disconnected");
      }
    }
  }

  /**
   * Downloads the specified file via the FTP channel.
   * Returns true if the file was downloaded, false otherwise.
   *
   * @param ftp FTPClient
   * @param fileName
   *
   * @throws IOException if fileName matches multiple files.
   **/
  private boolean getFile(FTPClient ftp, String fileName) throws IOException {
    LOG.info("Downloading file: '"+fileName+"'");
    OutputStream output;
    try {
      output = new FileOutputStream(fileName);
      boolean downloadSucceeded = ftp.retrieveFile(fileName, output);
      output.close();

      if (downloadSucceeded) {
        return true;
      } else {
        LOG.warn("Download failed. FTP Reply: '"+ftp.getReplyString()+"'");
        return false;
      }
    } catch (Exception e) {
      throw new IOException("FTP Error: "+e.getMessage(), e);
    }
  }

  /**
   * Returns the number of target files that have not been downloaded.
   **/
  private int missingFileCount() {
    int result = 0;
    for(Map.Entry<String, Boolean> entry : targetFiles.entrySet()) {
      if (entry.getValue() == Boolean.FALSE) {
        result++;
      }
    }
    return result;
  }

  /**
   * Gets a secret from yKeyKey via Athenz.
   *
   * @param secretName - yKeyKey key name
   *
   * @throws IOException if there's an error retrieving the secret
   **/
  protected String getSecret(String secretName) throws IOException {
    String secret;
    try {
      byte[] secretBytes = UserGroupInformation.getCurrentUser().getCredentials().getSecretKey(new Text(secretName));
      secret = new String(secretBytes, StandardCharsets.UTF_8);

      LOG.info("Got secret '"+secret.substring(0,3)+"...'");
    } catch (IOException e) {
      throw new IOException("Failed to get credential from ykeykey: '"+e.getMessage()+"'", e);
    }
    return secret;
  }

  /**
   * Creates a new FTPSClient for the input URL.
   *
   * @param url for FTPS server
   * @param username for FTPS login
   * @param password for FTPS login
   * @param dir working dir to set for FTPS session. May be null
   *
   * @throws IOException if there is an error creating the client
   **/
  protected FTPSClient getFTPSClient(String url, String username, String password, String dir)
      throws IOException {
    try {
      FTPSClient ftp = new FTPSClient(false);     // Use 'implicit' mode
      LOG.info("Opening FTP connection to "+url);
      ftp.connect(url);
      ftp.execPBSZ(0);             // Protection Buffer Size (PBSZ) must be 0 for TLS
      ftp.execPROT("P");           // Use 'private' protection level
      ftp.enterLocalPassiveMode(); // Use passive mode to avoid firewall issues

      // Log some details of the connection
      LOG.info("enabled cipher suites: "+String.join(" ", ftp.getEnabledCipherSuites()));
      LOG.info("enabled protocols: "+String.join(" ", ftp.getEnabledProtocols()));

      // Log in
      LOG.info("Logging in to "+url+" as "+username);
      if (!ftp.login(username, password)) {
        throw new IOException("Failed to login as '"+username+"': "+ftp.getReplyString());
      }
      LOG.info("FTP reply: '"+ftp.getReplyString()+"'");

      // move to target dir
      if (dir != null) {
        LOG.info("cd "+dir);
        if (!ftp.changeWorkingDirectory(dir)) {
          throw new IOException("Failed to cd to '"+dir+"': "+ftp.getReplyString());
        }
        LOG.info("FTP reply: '"+ftp.getReplyString()+"'");
      }

      return ftp;
    }
    catch (SocketException se) {
      throw new IOException("Failed to open FTP connection to "+url+": "+se.getMessage(), se);
    }
    catch (SSLException ssle) {
      throw new IOException("Failed to set up FTPS connection to "+url+": "+ssle.getMessage(), ssle);
    }
  }

  /**
   * Gets the FileSystem for the input Configuration. 
   * Allows unit test to inject a mock Athens client.
   * @param jobConf
   **/
  protected FileSystem getFileSystem(Configuration jobConf) throws IOException {
    return FileSystem.get(jobConf);
  }

  /**
   * Entry point for oozie java action.
   */
  public static void main(String args[]) throws Exception {
    Configuration conf = new Configuration();
    conf.addResource(new Path("file:///", System.getProperty(OOZIE_CONFIG_PROPERTY)));

    BpmDownloader downloader = new BpmDownloader(conf);
    downloader.run();
  }

}
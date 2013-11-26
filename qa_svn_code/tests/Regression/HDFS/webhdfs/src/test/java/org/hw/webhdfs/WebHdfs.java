package org.hw.webhdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hw.hadoop.HadoopKeys;
import org.hw.hadoop.HadoopUtils;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.XPathUtils;
import org.json.JSONException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import static org.junit.Assert.*;


public class WebHdfs {
  
  //instantiate the logger
  protected static final Logger LOG = Logger.getLogger(WebHdfs.class);
  
  //filesystem related objects
  protected static FileSystem localFS;
  protected static DistributedFileSystem dfs;
  
  protected static Configuration localConf = new Configuration();
  protected static Configuration hadoopConf = new Configuration();
  protected static URI HDFS_URI;
  
  //variables that are specific to webhdfs api's
  protected static String URL;
  protected static String HOST;
  protected static String HDFS_URL;
  protected static final String SCHEME=WebHdfsKeys.SCHEME;
  protected static int PORT;
  protected static final String PATH=WebHdfsKeys.PATH;
  protected static final String USER=WebHdfsKeys.USER;
  protected static final String USER2=WebHdfsKeys.USER2;
  protected static final String HDFS_DATA_DIR=WebHdfsKeys.HDFS_DATA_DIR;
  protected static final String LOCAL_DATA_DIR=WebHdfsKeys.LOCAL_DATA_DIR;
  protected static final String LOCAL_LARGE_FILE=LOCAL_DATA_DIR+"/large_movie_file.MOV";
  protected static final String HDFS_LARGE_FILE=HDFS_DATA_DIR+"/large_movie_file.MOV";
  protected static String DToken;
  protected static String superUserDToken;
  protected static List<NameValuePair> QPARAMS,SUPER_USER_QPARAMS;
  protected static Boolean IS_SECURE=false;
  private static String[] dfs_http_address;
  protected static String VERSION = System.getenv("VERSION").trim();
  
  //Default Setup method that each class can use
  @BeforeClass
  public static void setUp() throws Exception {
    //configure log4j
    PropertyConfigurator.configure(WebHdfsKeys.LOG4J_FILE);
    LOG.info("Haddop Version :" + VERSION);
    //create a large file on the local fs
    File large_file=new File(LOCAL_LARGE_FILE);
    //if the file does not exist create it
    if (! large_file.exists()) {
      FileOutputStream fout= new FileOutputStream(large_file);
      //create a byte array of 350MB (367001600) bytes
      int len=367001600;
      byte[] data = new byte[len];
      for (int i=0;i < len ;i++) {
        data[i] = new Integer(i).byteValue();
      }
      fout.write(data);
      fout.close();
    }
    
    //get the host and port from the hadoop configs
    UserGroupInformation ugi = null;
    //determine security of the tests if the security is set on hadoop or not
    IS_SECURE = (hadoopConf.get(WebHdfsKeys.HADOOP_SECURITY_PROPERTY).equalsIgnoreCase("kerberos")) ? true : false;
    
    //if secure login from keytab
    if ( IS_SECURE ) {
      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(WebHdfsKeys.USER,WebHdfsKeys.USER_KEYTAB_FILE);
    } else {
      ugi = UserGroupInformation.createRemoteUser(USER);
    }
    
    //setup the local fs and the hadoop fs
    //generate the local fs
    localConf.set(HadoopKeys.FS_DEFAULT_NAME, "file:///");
    localFS = FileSystem.get(localConf);
    
    //get the HDFS url from the configs
    HDFS_URL=hadoopConf.get("fs.default.name", "hdfs://localhost:8020");
    
    //generate the hadoop fs
    HDFS_URI = new URI(HDFS_URL);
    
    //create a dfs object as the ugi with the login
    dfs = ugi.doAs(new PrivilegedExceptionAction<DistributedFileSystem>() {
      @Override
      public DistributedFileSystem run() throws Exception {
        DistributedFileSystem dfs_temp = new DistributedFileSystem();
        dfs_temp.initialize(HDFS_URI, hadoopConf);
        return dfs_temp;
      }
    });
    
    dfs_http_address=dfs.getConf().get("dfs.http.address", "localhost:50070").split(":");
    
    //get the new host and port and set the other values
    HOST=dfs_http_address[0];
    PORT=Integer.valueOf(dfs_http_address[1]);
    URL=SCHEME+"://"+HOST+":"+PORT;
    
    deleteAsSuperUser(HDFS_DATA_DIR);
    
    //load the data to hdfs
    HadoopUtils.loadDataToHadoop(localFS,WebHdfsKeys.LOCAL_DATA_DIR, dfs, HDFS_DATA_DIR, hadoopConf);
    
    //if security is on kinit and get the delegation token
    if ( IS_SECURE ) {
      //get the token for the user
      DToken = WebHdfsUtils.getDToken(HOST,PORT,USER,WebHdfsKeys.USER_KEYTAB_FILE,USER);
      LOG.info("DT for USER " + USER + " -> " +DToken);
      //get the toke for the super user
      superUserDToken = WebHdfsUtils.getDToken(HOST,PORT,WebHdfsKeys.HADOOP_SUPER_USER,WebHdfsKeys.HADOOP_SUPER_USER_KEYTAB_FILE,WebHdfsKeys.HADOOP_SUPER_USER);
      LOG.info("DT for SUPER USER " + WebHdfsKeys.HADOOP_SUPER_USER + " -> " +superUserDToken);
    }
    
    //give everybody write permision
    dfs.setPermission(new Path(HDFS_DATA_DIR), new FsPermission("777"));
  }
  
  //Default tear down that each class can use
  @AfterClass
  public static void tearDown() throws Exception {
    localFS.close();
    dfs.close();
  }
  
  //before every test if security is on then add the token parameter
  @Before
  public void testSetup() throws IOException, InterruptedException {
    //generate the query parameters
    QPARAMS = new ArrayList<NameValuePair>();
    SUPER_USER_QPARAMS = new ArrayList<NameValuePair>();
    //if security is on add the delegation token
    if (IS_SECURE) {
      QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, DToken));
      SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, superUserDToken));
    }
    else {
      QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_USER_NAME, WebHdfsKeys.USER));
      SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_USER_NAME, WebHdfsKeys.HADOOP_SUPER_USER));
    }
  }
  
  //method to check failed response
  public static void checkFailedResponse(String exception, String javaClassName, String message, Document doc) {
    //check the response content
    XPathUtils.checkXpathTextContent(WebHdfsKeys.XPATH_FAIL_EXCEPTION,exception,doc);
    XPathUtils.checkXpathTextContent(WebHdfsKeys.XPATH_FAIL_JAVA_CLASSNAME,javaClassName,doc);
    XPathUtils.checkXpathTextContent(WebHdfsKeys.XPATH_FAIL_MSG,message,doc);
  }
  
  //method to check failed response with regex
  public static void checkFailedResponseWithRegex(String exception, String javaClassName, String message, Document doc) {
    //check the response content
    XPathUtils.checkXpathTextContentWithRegex(WebHdfsKeys.XPATH_FAIL_EXCEPTION,exception,doc);
    XPathUtils.checkXpathTextContentWithRegex(WebHdfsKeys.XPATH_FAIL_JAVA_CLASSNAME,javaClassName,doc);
    XPathUtils.checkXpathTextContentWithRegex(WebHdfsKeys.XPATH_FAIL_MSG,message,doc);
  }
  
  //method ot make the custom put call
  public Response Put(String scheme, String host, int port,String path, List<NameValuePair> qparams, String fileName) throws Exception {
    Request rqs = new Request();
    rqs.setUri(scheme,host,port,path,qparams);
    //make the put request
    Response rsp = rqs.Put();
    HttpResponse httpResponse = rqs.getHttpResponse();
    
    //check the status code
    int expCode=HttpStatus.SC_TEMPORARY_REDIRECT;
    int actCode = rsp.getStatusCode();
    assertEquals("Check if the put call lead to a redirect", expCode, actCode);
    
    //capture the response and get the redirected header
    //get the location header to find out where to redirect to
    Header locationHeader = httpResponse.getFirstHeader("location");
    if (locationHeader == null) {
        // got a redirect response, but no location header
        throw new ProtocolException(
                "Received redirect response " + httpResponse.getStatusLine()
                + " but no location header");
    }
    String location = locationHeader.getValue();
    if (LOG.isDebugEnabled()) {
        LOG.debug("Redirect requested to location '" + location + "'");
    }

    //make the redirected request
    URI uri = new URI(location);
    rqs = new Request(uri);
    File file = new File(fileName);
    rsp = rqs.Put(file);
    return rsp;
  }
  
  //method ot make the custom put call
  public Response Post(String scheme, String host, int port,String path, List<NameValuePair> qparams, String fileName) throws Exception {
    Request rqs = new Request();
    rqs.setUri(scheme,host,port,path,qparams);
    //make the put request
    Response rsp = rqs.Post();
    HttpResponse httpResponse = rqs.getHttpResponse();
    
    //check the status code
    int expCode=HttpStatus.SC_TEMPORARY_REDIRECT;
    int actCode = rsp.getStatusCode();
    assertEquals("Check if the put call lead to a redirect", expCode, actCode);
    
    //capture the response and get the redirected header
    //get the location header to find out where to redirect to
    Header locationHeader = httpResponse.getFirstHeader("location");
    if (locationHeader == null) {
        // got a redirect response, but no location header
        throw new ProtocolException(
                "Received redirect response " + httpResponse.getStatusLine()
                + " but no location header");
    }
    String location = locationHeader.getValue();
    if (LOG.isDebugEnabled()) {
        LOG.debug("Redirect requested to location '" + location + "'");
    }

    //make the redirected request
    URI uri = new URI(location);
    rqs = new Request(uri);
    File file = new File(fileName);
    rsp = rqs.Post(file);
    return rsp;
  }
  
  public static void deleteAsSuperUser(String path) throws IOException, InterruptedException, URISyntaxException, ParserConfigurationException, SAXException, JSONException {
    
    //generate the query parameters
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();
    
    //if security is on kinit and get the delegation token
    if ( IS_SECURE ) {
      String token = WebHdfsUtils.getDToken(HOST,PORT,WebHdfsKeys.HADOOP_SUPER_USER,WebHdfsKeys.HADOOP_SUPER_USER_KEYTAB_FILE,WebHdfsKeys.HADOOP_SUPER_USER);
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, token));
    }
    else {
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_USER_NAME, WebHdfsKeys.HADOOP_SUPER_USER));
    }
    
    //make the delete call
    WebHdfsUtils.deletePathUsingWebHdfs(HOST,PORT,path, qparams);
  }
}
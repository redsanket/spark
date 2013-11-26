package org.hw.webhdfs;



import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestGetFileCheckSum extends WebHdfs {
  
  //get file checksum of a file
  @Test 
  public void getCheckSumFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILECHECKSUM));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    XPathUtils.checkXpathTextContent("/FileChecksum/algorithm",dfs.getFileChecksum(hadoopPath).getAlgorithmName(),rsp.getDocument());
    String expBytes=new String(Hex.encodeHex(dfs.getFileChecksum(hadoopPath).getBytes()));
    XPathUtils.checkXpathTextContent("/FileChecksum/bytes", expBytes,rsp.getDocument());
    XPathUtils.checkXpathTextContent("/FileChecksum/length", String.valueOf(dfs.getFileChecksum(hadoopPath).getLength()),rsp.getDocument());
  }
  
  //get file checksum of a large file
  @Test 
  public void getCheckSumLargeFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILECHECKSUM));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    XPathUtils.checkXpathTextContent("/FileChecksum/algorithm",dfs.getFileChecksum(hadoopPath).getAlgorithmName(),rsp.getDocument());
    String expBytes=new String(Hex.encodeHex(dfs.getFileChecksum(hadoopPath).getBytes()));
    XPathUtils.checkXpathTextContent("/FileChecksum/bytes", expBytes,rsp.getDocument());
    XPathUtils.checkXpathTextContent("/FileChecksum/length", String.valueOf(dfs.getFileChecksum(hadoopPath).getLength()),rsp.getDocument());
  }
  
  //get file checksum of a dir
  @Test 
  public void getCheckSumOnDir() throws Exception{
    
    String path=HDFS_DATA_DIR;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILECHECKSUM));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 404 with appropriate content type
    //HDFS-2440
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    if ( VERSION.equalsIgnoreCase("23") ) {
      checkFailedResponseWithRegex("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path+".*",rsp.getDocument());
    } else {
      checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());
    }
  }
  
//get file checksum of a empty file
  @Test 
  public void getCheckSumEmptyFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/empty_file";
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILECHECKSUM));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    XPathUtils.checkXpathTextContent("/FileChecksum/algorithm",dfs.getFileChecksum(hadoopPath).getAlgorithmName(),rsp.getDocument());
    String expBytes=new String(Hex.encodeHex(dfs.getFileChecksum(hadoopPath).getBytes()));
    XPathUtils.checkXpathTextContent("/FileChecksum/bytes", expBytes,rsp.getDocument());
    XPathUtils.checkXpathTextContent("/FileChecksum/length", String.valueOf(dfs.getFileChecksum(hadoopPath).getLength()),rsp.getDocument());
  }
  
//get filechecksum of a invalid path
  @Test 
  public void getFileCheckSumInvalidPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/this_is_invalid_path";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETCONTENTSUMMARY));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());   
  }

}

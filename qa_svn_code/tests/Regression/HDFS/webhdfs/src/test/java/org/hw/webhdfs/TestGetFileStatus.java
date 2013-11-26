package org.hw.webhdfs;

import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;


import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestGetFileStatus extends WebHdfs {
  
  @Test 
  public void listDir() throws Exception {
    
    String path = HDFS_DATA_DIR;
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILESTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the different items
    XPathUtils.checkXpathTextContent("FileStatus/accessTime",String.valueOf(fs.getAccessTime()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/blockSize",String.valueOf(fs.getBlockSize()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/group",fs.getGroup(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/type","DIRECTORY",rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/length",String.valueOf(fs.getLen()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/pathSuffix",fs.getLocalName(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/modificationTime",String.valueOf(fs.getModificationTime()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/owner",fs.getOwner(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/permission",String.format("%o", fs.getPermission().toShort()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/replication",String.valueOf(fs.getReplication()),rsp.getDocument());
  }
  
  @Test 
  public void listFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/empty_file";
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILESTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the different items
    XPathUtils.checkXpathTextContent("FileStatus/accessTime",String.valueOf(fs.getAccessTime()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/blockSize",String.valueOf(fs.getBlockSize()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/group",fs.getGroup(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/type","FILE",rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/length",String.valueOf(fs.getLen()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/pathSuffix",fs.getLocalName(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/modificationTime",String.valueOf(fs.getModificationTime()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/owner",fs.getOwner(),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/permission",String.format("%o", fs.getPermission().toShort()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("FileStatus/replication",String.valueOf(fs.getReplication()),rsp.getDocument());
  }
  
  
  @Test 
  //HDFS-2426
  public void listInvalidPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILESTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());
  }
  
  @Test 
  public void listPathWithSpecialChars() throws Exception{
    
    String fileName ="file_name_with_special_chars_-@#$^";
    try {
      fileName=URLEncoder.encode(fileName,"UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.error(e.getMessage(),e.fillInStackTrace());
      fail("Could not encode fileName " + fileName);
    }
   
    String path=HDFS_DATA_DIR+"/"+fileName;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETFILESTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }

}

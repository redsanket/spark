package org.hw.webhdfs;



import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestListStatus extends WebHdfs{
  
  @Test 
  public void listDir() throws Exception{
    String path=HDFS_DATA_DIR;
    DirectoryListing dl = dfs.getClient().listPaths(path, HdfsFileStatus.EMPTY_NAME);
    HdfsFileStatus[] statuses = dl.getPartialListing();
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    int i=1;
    for(HdfsFileStatus fs : statuses) {
      String type = (fs.isDir()) ? "DIRECTORY" : "FILE";
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/accessTime",String.valueOf(fs.getAccessTime()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/blockSize",String.valueOf(fs.getBlockSize()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/group",fs.getGroup(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/type",type,rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/length",String.valueOf(fs.getLen()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/pathSuffix",fs.getLocalName(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/modificationTime",String.valueOf(fs.getModificationTime()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/owner",fs.getOwner(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/permission",String.format("%o", fs.getPermission().toShort()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/replication",String.valueOf(fs.getReplication()),rsp.getDocument());
      i++;
    }
    XPathUtils.checkNumOfNodesReturned("FileStatuses/FileStatus", i-1, rsp.getDocument());
  }
  
  @Test 
  public void listFile() throws Exception{
    String path=HDFS_DATA_DIR+"/empty_file";
    DirectoryListing dl = dfs.getClient().listPaths(path, HdfsFileStatus.EMPTY_NAME);
    HdfsFileStatus[] statuses = dl.getPartialListing();
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    int i=1;
    for(HdfsFileStatus fs : statuses) {
      String type=(fs.isDir()) ? "DIRECTORY" : "FILE";
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/accessTime",String.valueOf(fs.getAccessTime()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/blockSize",String.valueOf(fs.getBlockSize()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/group",fs.getGroup(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/type",type,rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/length",String.valueOf(fs.getLen()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/pathSuffix",fs.getLocalName(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/modificationTime",String.valueOf(fs.getModificationTime()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/owner",fs.getOwner(),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/permission",String.format("%o", fs.getPermission().toShort()),rsp.getDocument());
      XPathUtils.checkXpathTextContent("FileStatuses/FileStatus["+i+"]/replication",String.valueOf(fs.getReplication()),rsp.getDocument());
      i++;
    }
    XPathUtils.checkNumOfNodesReturned("FileStatuses/FileStatus", i-1, rsp.getDocument());
  }
  
  @Test 
  public void listInvalidPath() throws Exception {
    
    String path=HDFS_DATA_DIR+"/invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.LISTSTATUS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File "+path+" does not exist.",rsp.getDocument());
  }
}

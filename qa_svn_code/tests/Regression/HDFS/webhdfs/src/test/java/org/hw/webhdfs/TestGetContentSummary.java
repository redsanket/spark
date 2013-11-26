package org.hw.webhdfs;


import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestGetContentSummary extends WebHdfs {
  
  //get content summary of a file
  @Test 
  public void getContentSummaryFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETCONTENTSUMMARY));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("/ContentSummary/directoryCount",String.valueOf(dfs.getContentSummary(hadoopPath).getDirectoryCount()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/fileCount",String.valueOf(dfs.getContentSummary(hadoopPath).getFileCount()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/length",String.valueOf(dfs.getContentSummary(hadoopPath).getLength()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/quota",String.valueOf(dfs.getContentSummary(hadoopPath).getQuota()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/spaceConsumed",String.valueOf(dfs.getContentSummary(hadoopPath).getSpaceConsumed()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/spaceQuota",String.valueOf(dfs.getContentSummary(hadoopPath).getSpaceQuota()),rsp.getDocument());
  }
  
  //get content summary of a dir
  @Test 
  public void getContentSummaryDir() throws Exception{
    
    String path=HDFS_DATA_DIR;
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GETCONTENTSUMMARY));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("/ContentSummary/directoryCount",String.valueOf(dfs.getContentSummary(hadoopPath).getDirectoryCount()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/fileCount",String.valueOf(dfs.getContentSummary(hadoopPath).getFileCount()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/length",String.valueOf(dfs.getContentSummary(hadoopPath).getLength()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/quota",String.valueOf(dfs.getContentSummary(hadoopPath).getQuota()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/spaceConsumed",String.valueOf(dfs.getContentSummary(hadoopPath).getSpaceConsumed()),rsp.getDocument());
    XPathUtils.checkXpathTextContent("/ContentSummary/spaceQuota",String.valueOf(dfs.getContentSummary(hadoopPath).getSpaceQuota()),rsp.getDocument());
  }
  
  //get content summary of a invalid path
  @Test 
  public void getContentSummaryInvalidPath() throws Exception{
    
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

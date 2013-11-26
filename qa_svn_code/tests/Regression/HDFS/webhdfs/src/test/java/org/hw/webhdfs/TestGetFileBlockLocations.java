package org.hw.webhdfs;

import static org.junit.Assert.assertNull;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;
import org.w3c.dom.Document;


public class TestGetFileBlockLocations extends WebHdfs {

  @Test 
  public void getFile() throws Exception {
    
    String path = HDFS_DATA_DIR+"/file_large_data.txt";
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GET_BLOCK_LOCATIONS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    validateResponse(path,0,fs.getLen(),rsp.getDocument());
  }
  
  @Test 
  //HDFS-2508
  public void getLargeFile() throws Exception {
    
    String path = HDFS_LARGE_FILE;
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GET_BLOCK_LOCATIONS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  
    validateResponse(path,0,fs.getLen(),rsp.getDocument());
  }
  
  @Test 
  //HDFS-2508
  public void getLargeFileWithLen() throws Exception {
    
    String path = HDFS_LARGE_FILE;
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GET_BLOCK_LOCATIONS));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, String.valueOf(fs.getLen())));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  
    validateResponse(path,0,fs.getLen(),rsp.getDocument());
  }
  
  @Test 
  //HDFS-2508
  public void getLargeFileWithLenAndOffset() throws Exception {
    
    String path = HDFS_LARGE_FILE;
    HdfsFileStatus fs = dfs.getClient().getFileInfo(path);
    long length=fs.getLen();
    long offset = length/2;
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GET_BLOCK_LOCATIONS));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, String.valueOf(length)));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OFFSET, String.valueOf(offset)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  
    validateResponse(path,offset,length,rsp.getDocument());
  }
  
  @Test 
  //HDFS-2508
  public void getDir() throws Exception {
    
    String path = HDFS_DATA_DIR;
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.GET_BLOCK_LOCATIONS));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    
    // For Hadoop 1.0:
    //check if we get 200 with appropriate content type
    // WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    //check the response content
    // assertNull("Response is null",rsp.getDocument());

    // For Hadoop 0.23
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);

    //check the response content
    checkFailedResponse("FileNotFoundException",
                        "java.io.FileNotFoundException",
                        "File does not exist: "+path,
                        rsp.getDocument());    
  }
  
  //validate a pos response
  private void validateResponse(String path,long offset, long length,Document doc) throws Exception {
    Class<?> dfsClientClass = null;
    dfsClientClass = Class.forName("org.apache.hadoop.hdfs.DFSClient");
    
    DFSClient dfsClient=dfs.getClient();
    ClientProtocol namenode=null;
    
    //try to get a handle to the namenode object
    try {
      Field field = dfsClientClass.getField("namenode");
      namenode = (ClientProtocol)field.get(dfsClient);
    } catch (NoSuchFieldException e) {
      //if the field does not exist then use the method
      Method method = dfsClientClass.getMethod("getNamenode", null);
      namenode = (ClientProtocol) method.invoke(dfsClient, null);
    }
    
    LocatedBlocks locatedBlocks = namenode.getBlockLocations(path, offset, length);
    
    //check the different items
    XPathUtils.checkXpathTextContent("/LocatedBlocks/fileLength",String.valueOf(locatedBlocks.getFileLength()),doc);
    XPathUtils.checkXpathTextContent("/LocatedBlocks/isUnderConstruction",String.valueOf(locatedBlocks.isUnderConstruction()),doc);
    int i = 1;
    String xpath_prefix=null;
    for (LocatedBlock locatedBlock : locatedBlocks.getLocatedBlocks()) {
      xpath_prefix="/LocatedBlocks/locatedBlocks["+i+"]";
      //block info
      XPathUtils.checkXpathTextContent(xpath_prefix+"/block/blockId",String.valueOf(locatedBlock.getBlock().getBlockId()),doc);
      XPathUtils.checkXpathTextContent(xpath_prefix+"/block/generationStamp",String.valueOf(locatedBlock.getBlock().getGenerationStamp()),doc);
      XPathUtils.checkXpathTextContent(xpath_prefix+"/block/numBytes",String.valueOf(locatedBlock.getBlock().getNumBytes()),doc);
      
      //block token info
      XPathUtils.checkXpathTextContentWithRegex(xpath_prefix+"/blockToken/urlString",".*",doc);
      
      //corrupt and start offset
      XPathUtils.checkXpathTextContent(xpath_prefix+"/isCorrupt",String.valueOf(locatedBlock.isCorrupt()),doc);
      XPathUtils.checkXpathTextContent(xpath_prefix+"/startOffset",String.valueOf(locatedBlock.getStartOffset()),doc);
      
      //locations
      int j=1;
      for (DatanodeInfo location : locatedBlock.getLocations()) {
        //use the hostname to figure out the xpath so you check against the right datanode
        String location_prefix=xpath_prefix+"/locations[hostName='"+location.getHostName()+"']";
        // For Hadoop 1.0, use .toString()
        // XPathUtils.checkXpathTextContent(location_prefix+"/adminState",location.getAdminState().toString(),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/adminState",location.getAdminState().name(),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/capacity",String.valueOf(location.getCapacity()),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/dfsUsed",String.valueOf(location.getDfsUsed()),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/infoPort",String.valueOf(location.getInfoPort()),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/ipcPort",String.valueOf(location.getIpcPort()),doc);
        XPathUtils.checkXpathTextContentWithRegex(location_prefix+"/lastUpdate","[0-9]{13}",doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/name",location.getName(),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/networkLocation",String.valueOf(location.getNetworkLocation()),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/remaining",String.valueOf(location.getRemaining()),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/storageID",location.getStorageID(),doc);
        XPathUtils.checkXpathTextContent(location_prefix+"/xceiverCount",String.valueOf(location.getXceiverCount()),doc);
        j++;
      }
      XPathUtils.checkNumOfNodesReturned(xpath_prefix+"/locations", j-1, doc);
      i++;
    }
    XPathUtils.checkNumOfNodesReturned("/LocatedBlocks/locatedBlocks", i-1, doc);
  }
}

package org.hw.webhdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestRename extends WebHdfs{
  
  //Rename a file
  @Test 
  public void renameFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_to_be_renamed.txt";
    String newPath=HDFS_DATA_DIR+"/file_to_be_renamed_new.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.RENAME));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DESTINATION,newPath));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
   //check if the new path exists
   assertTrue("Check path('+"+newPath+"') exists on hadoop",dfs.exists(new Path(newPath)));
   //check the old path does not exists
   assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
  
  
  //Rename a dir
  @Test 
  public void renameDir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_to_be_renamed";
    String newPath=HDFS_DATA_DIR+"/dir_to_be_renamed_new";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.RENAME));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DESTINATION,newPath));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the new path exists
    assertTrue("Check path('+"+newPath+"') exists on hadoop",dfs.exists(new Path(newPath)));
    //check the old path does not exists
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
  
  //Rename a dir that has children
  @Test 
  public void renameDirThatHasChildren() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_with_children_to_be_renamed";
    String newPath=HDFS_DATA_DIR+"/dir_with_children_to_be_renamed_new";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.RENAME));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DESTINATION,newPath));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the new path exists
    assertTrue("Check path('+"+newPath+"') exists on hadoop",dfs.exists(new Path(newPath)));
    //check the old path does not exists
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
    //make sure the child exists with the new path
    assertTrue("Check path('+"+newPath+"/child') exists on hadoop",dfs.exists(new Path(newPath+"/child")));
  }
  
  
  //Rename a path and give new path that is invalid
  //HDFS-2428
  @Test 
  public void renamePathToEmtpyPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_with_children_to_be_renamed";
    String newPath="";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.RENAME));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DESTINATION,newPath));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Can not create a Path from a null string",rsp.getDocument());
  }

}

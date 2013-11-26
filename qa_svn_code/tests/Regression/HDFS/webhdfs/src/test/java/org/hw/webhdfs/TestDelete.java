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

public class TestDelete extends WebHdfs{
  
  //Delete a file
  @Test 
  public void deleteFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_to_be_deleted.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //make sure response co
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the path does not exist on hadoop
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
  
  //Delete a dir
  @Test 
  public void deleteDir() throws Exception{
    
    //create the dir on hdfs
    String path=HDFS_DATA_DIR+"/dir_to_be_deleted";
    dfs.mkdirs(new Path(path));
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the path does not exist on hadoop
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
  
  //Delete a dir that has children and dont make it recursive
  @Test 
  public void deleteDirWithChildrenNotRecursive() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_with_children";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    //check if we get 403 with appropriate content type
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response message
    checkFailedResponse("IOException","java.io.IOException",path+" is non empty",rsp.getDocument());
    
    //check if the path exist on hadoop
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
  }
  
  //Delete a dir that has children and dont make it recursive
  @Test 
  public void deleteDirWithChildrenRecursiveFalse() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_with_children2";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_RECURSIVE,"false"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    //check if we get 403 with appropriate content type
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response message
    checkFailedResponse("IOException","java.io.IOException",path+" is non empty",rsp.getDocument());
    
    //check if the path exist on hadoop
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
  }
  
  //Delete a dir that has children and make it recursive
  @Test 
  public void deleteDirWithChildrenRecursiveTrue() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_to_be_deleted_recursive";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_RECURSIVE,"true"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response message
    XPathUtils.checkXpathTextContent("/boolean","true",rsp.getDocument());
    
    //check path does not exists
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
  
  //Delete a dir that does not exist
  //HDFS-2429
  @Test 
  public void deleteDirThatDoesNotExist() throws Exception{
    
    String path=HDFS_DATA_DIR+"/this_dir_does_not_exist";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
  //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response message
    XPathUtils.checkXpathTextContent("/boolean","false",rsp.getDocument());
  }
  
  //Delete a dir and send invalid value for recursive
  //HDFS-2428
  @Test 
  public void deletePathRecursiveValueInvalid() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_not_to_be_deleted";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.DELETE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_RECURSIVE,"invalid"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Delete();
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_RECURSIVE+"\": Failed to parse \"invalid\" to Boolean.",rsp.getDocument());
    
    //check path exists
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
    
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
  }
}

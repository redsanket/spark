package org.hw.webhdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.junit.Test;

public class TestSetPermission extends WebHdfs{
  //test set permission on a file
  @Test 
  public void setPermOnFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_file_permission_tests/file_change_perm.txt";
    String permission="500";
    //expected permission is different as this is a file, file cant have execute permission
    String expPermission="400";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETPERMISSION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    assertEquals("Check permission for path: "+path,new FsPermission(expPermission).toString(),dfs.getFileStatus(new Path (path)).getPermission().toString());
  
    //check content is null
    assertTrue("Check response is empty", rsp.getContent().isEmpty());
  }
  
  //test set permission on a dir
  @Test 
  public void setPermOnDir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_file_permission_tests";
    String permission="700";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETPERMISSION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    assertEquals("Check permission for path: "+path,new FsPermission(permission).toString(),dfs.getFileStatus(new Path (path)).getPermission().toString());
  }
  
  //test set permission on a path that does not exists
  @Test 
  public void setPermOnInvalidPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/path_does_not_exist";
    String permission="500";
    
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETPERMISSION));
    
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());
  }
  
  //test set invalid permission on a path
  @Test 
  public void setInvalidPerm() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_file_permission_tests/file_invalid_perm.txt";
    String permission="955";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETPERMISSION));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_PERMISSION+"\": Failed to parse \"955\" as a radix-8 short integer.",rsp.getDocument());
    
     //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
}

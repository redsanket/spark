package org.hw.webhdfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.junit.Test;

//until you have a better way to handle super user
public class TestSetOwner extends WebHdfs{
  
  //test set owner of a file
  @Test 
  public void setOwnerOfAFile() throws Exception{
    
    final String path=HDFS_DATA_DIR+"/dir_set_owner/file_change_owner.txt";
    String owner="new_owner";
    String expGroup = null;
    expGroup = dfs.getFileStatus(new Path (path)).getGroup();
   
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OWNER, owner));
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();       
    
   //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    String actOwner = dfs.getFileStatus(new Path (path)).getOwner();
    String actGroup = dfs.getFileStatus(new Path (path)).getGroup();
      
    assertEquals("Check owner for path: "+path,owner,actOwner);
    assertEquals("Check group for path: "+path,expGroup,actGroup);
    
    //HDFS-2431
    assertTrue("Check response is empty",rsp.getContent().isEmpty());
  }
  
  //test set group of a file
  //HDFS-2437
  @Test 
  public void setGroupOfAFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_owner/file_change_group.txt";
    String owner=USER;
    String group="new_group";
   
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_GROUP, group));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    String actOwner = dfs.getFileStatus(new Path (path)).getOwner();
    String actGroup = dfs.getFileStatus(new Path (path)).getGroup();
      
    assertEquals("Check owner for path: "+path,owner,actOwner);
    assertEquals("Check group for path: "+path,group,actGroup);
  }
  
  //test set owner and  group of a file
  //HDFS-2437
  @Test 
  public void setOwnerAndGroupOfAFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_owner/file_change_owner_group.txt";
    String owner="new_user";
    String group="new_group";
   
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OWNER, owner));
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_GROUP, group));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    String actOwner = dfs.getFileStatus(new Path (path)).getOwner();
    String actGroup = dfs.getFileStatus(new Path (path)).getGroup();
      
    assertEquals("Check owner for path: "+path,owner,actOwner);
    assertEquals("Check group for path: "+path,group,actGroup);
  }
  
  //test set owner and  group of a Dir
  //HDFS-2437
  @Test 
  public void setOwnerAndGroupOfADir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_owner";
    String owner="new_user";
    String group="new_group";
   
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OWNER, owner));
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_GROUP, group));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    String actOwner = dfs.getFileStatus(new Path (path)).getOwner();
    String actGroup = dfs.getFileStatus(new Path (path)).getGroup();
      
    assertEquals("Check owner for path: "+path,owner,actOwner);
    assertEquals("Check group for path: "+path,group,actGroup);
  }
  
  //test set owner on an invalid path
  @Test 
  public void setOwnerInvalidPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/this_is_invalid_path";
    String owner="new_user";
    
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OWNER, owner));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());
  }
  
//test send empty value for owner
  //HDFS-2438
  @Test 
  public void setEmptyOwner() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_owner/file_change_owner.txt";
    String owner="";
    
    //generate the query parameters
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETOWNER));
    SUPER_USER_QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OWNER, owner));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,SUPER_USER_QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Both owner and group are empty.",rsp.getDocument());
  }
}

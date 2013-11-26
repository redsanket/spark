package org.hw.webhdfs;

import static org.junit.Assert.*;

import java.net.URLEncoder;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.hw.wsfrm.XPathUtils;
import org.junit.Test;

public class TestMkdirs extends WebHdfs{
  
  //test creating a dir
  @Test 
  public void createDir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/new_dir";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.MKDIRS));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the path exists on hadoop
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
    //check for the permission
    assertEquals("[HDFS-2427]Check permission for path: "+path,WebHdfsKeys.DEFAULT_PERMISSION_DIR,dfs.getFileStatus(new Path (path)).getPermission().toString());
  }
  
  //test creating multiple dirs
  @Test 
  public void createMultipleDirs() throws Exception{
    
    String path=HDFS_DATA_DIR+"/multiple_dir/new_dir";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.MKDIRS));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
   
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
  }
  
  //test creating a dir with special characters
  @Test 
  public void createDirWithSpecialChars() throws Exception{
    
    String name="special_chars-!@#$^";
    String encName = null;
    
    encName=URLEncoder.encode(name, "UTF-8");
    
    String path=HDFS_DATA_DIR+"/"+name;
    String encPath=HDFS_DATA_DIR+"/"+encName;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.MKDIRS));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+encPath,QPARAMS);
    Response rsp = rqs.Put();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
  }
  
  //test creating a dir and give a permission
  @Test 
  public void createDirWithPermission() throws Exception{
    
    String path=HDFS_DATA_DIR+"/new_dir_with_permission";
    String permission="700";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.MKDIRS));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    XPathUtils.checkXpathTextContent("boolean","true",rsp.getDocument());
    
    //check if the path exists
    assertTrue("Check path('+"+path+"') exists on hadoop",dfs.exists(new Path(path)));
      
    //check that the permission of the file is set to 700
    assertEquals("[HDFS-2427]Check permission for path: "+path,new FsPermission(permission).toString(),dfs.getFileStatus(new Path (path)).getPermission().toString());
  }
  
  //test creating a dir and give an invalid permission
  //HDFS-2428
  @Test 
  public void createDirWithInvalidPermission() throws Exception{
    
    String path=HDFS_DATA_DIR+"/new_dir_with_invalid_permission";
    String permission="955";
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.MKDIRS));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION, permission));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_PERMISSION+"\": Failed to parse \"955\" as a radix-8 short integer.",rsp.getDocument());
    
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);

    //check if the path does not exists on hadoop
    assertFalse("Check path('+"+path+"') does not exist on hadoop",dfs.exists(new Path(path)));
  }
}

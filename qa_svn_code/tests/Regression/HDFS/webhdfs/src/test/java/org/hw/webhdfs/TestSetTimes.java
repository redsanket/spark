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

public class TestSetTimes extends WebHdfs{
  //test set access time on a file in the past
  @Test 
  public void setAccessTimeOnFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_times/file_change_acces_time.txt";
    long currTime = System.currentTimeMillis()-600000;
    String time=String.valueOf(currTime);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_ACCESS_TIME, time));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    long actTime = dfs.getFileStatus(new Path (path)).getAccessTime();
    assertEquals("Check access time for path: "+path,currTime,actTime);
    assertTrue("check response is empty",rsp.getContent().isEmpty());
  }
  
  //test set access time on a file in the future
  //HDFS-2435
  @Test 
  public void setAccessTimeOnFileToFuture() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_times/file_change_acces_time.txt";
    long currTime = System.currentTimeMillis()+600000;
    String time=String.valueOf(currTime);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_ACCESS_TIME, time));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long actTime = dfs.getFileStatus(new Path (path)).getAccessTime();
    assertEquals("Check access time for path: "+path,currTime,actTime);
  }
  
  //test set mod time on a file
  @Test 
  public void setModTimeOnFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_times/file_change_mod_time.txt";
    //time in the past
    long currTime = System.currentTimeMillis()-600000;
    String time=String.valueOf(currTime);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_MODIFICATION_TIME, time));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    long actTime = dfs.getFileStatus(new Path (path)).getModificationTime();
    assertEquals("Check modification time for path: "+path,currTime,actTime);
  }
  
  //test set mod time on a file to future
  //HDFS-2435
  @Test 
  public void setModTimeOnFileToFuture() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_times/file_change_mod_time.txt";
    //time in the past by 1 min
    long currTime = System.currentTimeMillis()+600000;
    String time=String.valueOf(currTime);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_MODIFICATION_TIME, time));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    long actTime = dfs.getFileStatus(new Path (path)).getModificationTime();
    assertEquals("Check modification time for path: "+path,currTime,actTime);
  }
  
  //test send invalid value for time
  //HDFS-2435
  @Test 
  public void setInvalidTime() throws Exception{
    
    String path=HDFS_DATA_DIR+"/dir_set_times/file_set_invalid_time.txt.txt";
    //time in the past by 1 min
    String time="invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_MODIFICATION_TIME, time));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"modificationtime\": Failed to parse \"invalid\" as a radix-10 long integer.",rsp.getDocument());
    
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //test set access and mod time on a file
  @Test 
  public void setAccessAndModTimesOnADir() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_set_times.txt";
    long accessTime = System.currentTimeMillis()-600000;
    long modTime = System.currentTimeMillis()-900000;
    
    String strAccessTime=String.valueOf(accessTime);
    String strModTime=String.valueOf(modTime);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.SETTIMES));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_ACCESS_TIME, strAccessTime));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_MODIFICATION_TIME, strModTime));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check if the path exists on hadoop
    long actAccessTime = dfs.getFileStatus(new Path (path)).getAccessTime();
    long actModTime = dfs.getFileStatus(new Path (path)).getModificationTime();
      
    assertEquals("Check access time for path: "+path,accessTime,actAccessTime);
    assertEquals("Check mod time for path: "+path,modTime,actModTime);
  }
}

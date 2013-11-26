package org.hw.webhdfs;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.junit.Test;

public class TestAppend extends WebHdfs {
  
  //append to a file
  @Test 
  public void appendAFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_append.txt";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
    String expFile=LOCAL_DATA_DIR+"/file_appended.txt";
    
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    
    Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(expFile, actData);
    
    //check the response content is null
    assertTrue("Check that the response content is empty",rsp.getContent().isEmpty());
  }
  
  //append to a file
  @Test 
  public void appendALargeFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_append_large.txt";
    String fileName=LOCAL_DATA_DIR+"/file_large_data.txt";
    String expFile=LOCAL_DATA_DIR+"/file_appended_large.txt";
    
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    
    Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(expFile, actData);
  }
  
  //append to a file and send buffer size
  @Test 
  public void appendAFileSendBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_append_buffer_size.txt";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
    String expFile=LOCAL_DATA_DIR+"/file_appended.txt";
    
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,"512"));
    
    Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(expFile, actData);
    
    //check the response content is null
    assertTrue("Check that the response content is empty",rsp.getContent().isEmpty());
  }
  
  //append to a file and send invalid buffer size
  @Test 
  public void appendAFileSendInvalidBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_append_buffer_size_invalid.txt";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
    String bufferSize="invalid";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Post(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_BUFFER_SIZE+"\": Failed to parse \"invalid\" as a radix-10 integer.",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //append to a file and send invalid buffer size
  //ignore until the buffersize bug is fixed
  @Test 
  public void appendAFileSendZeroBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_append_buffer_size_invalid.txt";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
    String bufferSize="0";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Post(new File (fileName));
    
    //Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"buffersize\": Invalid parameter range: buffersize = 0 < 1",rsp.getDocument());
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //append to a dir
  @Test 
  public void appendToADir() throws Exception {
    
    String path=HDFS_DATA_DIR+"/append_dir";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    
    Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    if ( VERSION.equalsIgnoreCase("23") ) {
      checkFailedResponseWithRegex("FileAlreadyExistsException","org.apache.hadoop.fs.FileAlreadyExistsException","Cannot create file "+path+"; already exists as a directory.*",rsp.getDocument());
    }
    else {
      checkFailedResponse("IOException","java.io.IOException","java.io.IOException: Cannot create file "+ path +"; already exists as a directory.",rsp.getDocument());
    }    
  }
  
  //append to a file that does not exist
  @Test 
  public void appendToFileThatDoesNotExist() throws Exception {
    
    String path=HDFS_DATA_DIR+"/append_dir/invalid_file.txt";
    String fileName=LOCAL_DATA_DIR+"/file_to_append_with.txt";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Post(new File (fileName));
    
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File "+ path +" not found.",rsp.getDocument());  
  }
  
  //append to a file
  @Test 
  public void appendWithEmptyData() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_to_be_appended_with_empty.txt";
    String fileName=LOCAL_DATA_DIR+"/empty_file";
    String expFile=LOCAL_DATA_DIR+"/file_to_be_appended_with_empty.txt";
    
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.APPEND));
    
    
    Response rsp = Post(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(expFile, actData);
  }
}

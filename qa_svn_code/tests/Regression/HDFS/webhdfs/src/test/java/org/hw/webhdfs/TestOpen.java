package org.hw.webhdfs;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.junit.Test;

public class TestOpen extends WebHdfs{

  //test open a small file
  @Test 
  public void openSmallFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    WSFRMUtils.compareFileToString(LOCAL_DATA_DIR+"/file_small_data.txt",rsp.getContent());
  }
  
  //test open a large file
  @Test 
  public void openLargeFile() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    WSFRMUtils.compareFileToString(LOCAL_DATA_DIR+"/file_large_data.txt",rsp.getContent());
  }
  
  //test open a file with utf-8 chars
  @Test 
  public void openFileWithUtf8() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_with_utf8_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    WSFRMUtils.compareFileToString(LOCAL_DATA_DIR+"/file_with_utf8_data.txt",rsp.getContent());
  }
  
  //test open a file and send a path to dir
  @Test 
  public void openDir() throws Exception{
    
    String path=HDFS_DATA_DIR;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response content
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File does not exist: "+path,rsp.getDocument());
  }
  
 //test open an invalid path
  //HDFS-2439
  @Test 
  public void openInvalidPath() throws Exception{
    
    String path=HDFS_DATA_DIR+"/this_is_invalid_path";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 404 with appropriate content type
    WSFRMUtils.checkHttp404(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response content
    checkFailedResponse("FileNotFoundException","java.io.FileNotFoundException","File "+path+" not found.",rsp.getDocument());
  }
  
  //test open file send buffer size
  @Test
  public void openFileSendBufferSize() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE, "64"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    WSFRMUtils.compareFileToString(LOCAL_DATA_DIR+"/file_small_data.txt",rsp.getContent());
  }
  
  //test open file send 0 buffer size
  //HDFS-2440
  @Test
  public void openFileSendZeroBufferSize() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE, "0"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"buffersize\": Invalid parameter range: buffersize = 0 < 1",rsp.getDocument());
  }
  
  //test open file send invalid buffer size
  //HDFS-2440
  @Test
  public void openFileSendInvalidBufferSize() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_small_data.txt";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE, "invalid"));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"buffersize\": Failed to parse \"invalid\" as a radix-10 integer.",rsp.getDocument());
    
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //test open a file and send an offset
  @Test 
  public void openFileWithOffset() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    int offset=100;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OFFSET, String.valueOf(offset)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //make a hadoop call and read the same file with the same offset
    Path hadoopPath = new Path(path);
    FileStatus fs = dfs.getFileStatus(hadoopPath);
    Long length = fs.getLen();
      
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int) (length-offset)];
    inps.readFully(offset, buffer);
    WSFRMUtils.compareByteArrayToString(buffer,rsp.getContent());
    inps.close();
  }
  
  //test open a file and send a length
  @Test 
  public void openFileWithLength() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    int length=100;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, String.valueOf(length)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //make a hadoop call and read the same file with the same offset
    Path hadoopPath = new Path(path);
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[length];
    inps.readFully(0,buffer,0,length);
    WSFRMUtils.compareByteArrayToString(buffer,rsp.getContent());
    inps.close();
  }
  
  //test open a file and send a length and offset
  @Test 
  public void openFileWithOffsetAndLength() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    int length=100;
    int offset=344;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, String.valueOf(length)));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OFFSET, String.valueOf(offset)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //make a hadoop call and read the same file with the same offset
    Path hadoopPath = new Path(path);
      
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[length];
      
    inps.readFully(offset,buffer,0,length);
    WSFRMUtils.compareByteArrayToString(buffer,rsp.getContent());
    inps.close();  
  }
  
  //test open a file and send offset > length
  @Test 
  public void openFileSendOffsetOutOfBounds() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    Path hadoopPath = new Path(path);
    FileStatus fs =null;
    long actLength=0;
    fs = dfs.getFileStatus(hadoopPath);
    actLength=fs.getLen();
    
    Long offset=actLength+1;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OFFSET, String.valueOf(offset)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 403 with appropriate content type
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    //check the response content
    checkFailedResponse("IOException","java.io.IOException","Offset="+offset+" out of the range [0, "+actLength+"); OPEN, path="+path,rsp.getDocument());
  }
  
//test open a file and send length > length
  @Test 
  public void openFileSendLengthOutOfBounds() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    Path hadoopPath = new Path(path);
    FileStatus fs =null;
    long actLength=0;
    fs = dfs.getFileStatus(hadoopPath);
    actLength=fs.getLen();
    
    Long length=actLength+1;
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, String.valueOf(length)));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check if we get 200 with appropriate content type
    WSFRMUtils.checkHttp200(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    WSFRMUtils.compareFileToString(LOCAL_DATA_DIR+"/file_large_data.txt",rsp.getContent());
  }
  
  //test open a file and send invalid offset
  //HDFS-2440
  @Test 
  public void openFileSendInvalidOffset() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    String offset="invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OFFSET, offset));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"offset\": Failed to parse \"invalid\" as a radix-10 long integer.",rsp.getDocument());
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
//test open a file and send invalid length
  //HDFS-2440
  @Test 
  public void openFileSendInvalidLength() throws Exception{
    
    String path=HDFS_DATA_DIR+"/file_large_data.txt";
    String length="invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.OPEN));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_LENGTH, length));
    
    Request rqs = new Request();
    rqs.setUri(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Get();
    //check the response content
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"length\": Failed to parse \"invalid\" as a radix-10 long integer.",rsp.getDocument());
    //check if we get 400 with appropriate content type
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
}

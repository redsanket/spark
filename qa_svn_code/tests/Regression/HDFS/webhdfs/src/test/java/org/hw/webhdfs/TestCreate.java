package org.hw.webhdfs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.hw.wsfrm.Request;
import org.hw.wsfrm.Response;
import org.hw.wsfrm.WSFRMKeys;
import org.hw.wsfrm.WSFRMUtils;
import org.junit.Test;

public class TestCreate extends WebHdfs {
  //create a file
  @Test 
  public void createSmallFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createSmallFile.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  
    //check the default block size
    long actBlockSize = dfs.getFileStatus(hadoopPath).getBlockSize();
    long expBlockSize = Long.valueOf(hadoopConf.get("dfs.block.size"));
    assertEquals("Check default block size for path: "+path,expBlockSize, actBlockSize);
    
    //check the default permission
    String actPerm = dfs.getFileStatus(hadoopPath).getPermission().toString();
    assertEquals("Check default permission for path : "+path, WebHdfsKeys.DEFAULT_PERMISSION_FILE,actPerm);
    
    //check the default replication HDFS-2456
    short expRep = Short.valueOf(hadoopConf.get("dfs.replication"));
    short actRep = dfs.getFileStatus(hadoopPath).getReplication();
    assertEquals("Check replication for path: "+path, expRep, actRep);
  
    //check the response content is null
    assertTrue("Check that the response content is empty",rsp.getContent().isEmpty());
  }
  
  //create a file
  @Test 
  public void createLargeFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createLargeFile.txt";
    String fileName=LOCAL_DATA_DIR+"/file_large_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
  //create a file but the file exists
  @Test 
  public void createFileThatExists() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_that_exists.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 403 with appropriate content type
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    if ( VERSION.equalsIgnoreCase("23") ) {
      //check the response message  
      checkFailedResponseWithRegex("FileAlreadyExistsException","org.apache.hadoop.fs.FileAlreadyExistsException","failed to create file /tmp/webhdfs_data/file_that_exists.txt on client .* because the file exists.*",rsp.getDocument());
    } else {
      //check the response message  
      checkFailedResponseWithRegex("IOException","java.io.IOException","java.io.IOException: failed to create file /tmp/webhdfs_data/file_that_exists.txt on client .* either because the filename is invalid or the file exists",rsp.getDocument());
    }
  }
  
  //create a file but the file exists, send overwrite to false
  @Test 
  public void createFileThatExistsOverwriteFalse() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_that_exists.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"false"));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
  //check if we get 403 with appropriate content type
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    
    if ( VERSION.equalsIgnoreCase("23") ) {
      //check the response message  
      checkFailedResponseWithRegex("FileAlreadyExistsException","org.apache.hadoop.fs.FileAlreadyExistsException","failed to create file /tmp/webhdfs_data/file_that_exists.txt on client .* because the file exists.*",rsp.getDocument());
    } else {
      //check the response message  
      checkFailedResponseWithRegex("IOException","java.io.IOException","java.io.IOException: failed to create file /tmp/webhdfs_data/file_that_exists.txt on client .* either because the filename is invalid or the file exists",rsp.getDocument());
    }  
  }
  
  //create a file send overwrite = true when file does not exist
  @Test 
  public void createFileOverWriteWhenFileDoesNotExist() throws Exception {
    
    String path=HDFS_DATA_DIR+"/create_file_overwrite_when_file_does_not_exist.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"true"));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
  //create a file send overwrite
  @Test 
  public void createFileOverWrite() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_that_exists_overwrite.txt";
    String fileName=LOCAL_DATA_DIR+"/file_large_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"true"));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
  //create a file send overwrite, and make the file small
  @Test 
  public void createFileOverWriteMakeSmall() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_that_exists_make_small.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"true"));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
  //create a file send invalid value for overwrite
  @Test 
  public void createFileSendInvalidOverwrite() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_that_exists.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"invalid"));
    
    //HDFS-2458
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"overwrite\": Failed to parse \"invalid\" to Boolean.",rsp.getDocument());  
    //check if we get 400 with appropriate content type
    //HDFS-2460
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //try to overwrite a readonly file
  //HDFS-2463
  @Test 
  public void createFileOverWriteReadOnlyFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/read_only_file.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //make the file readonly
    Path hadoopPath = new Path (path);
    dfs.setPermission(hadoopPath, new FsPermission("400"));
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"true"));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //HDFS-2463
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
    if ( VERSION.equalsIgnoreCase("23") ) {
      checkFailedResponseWithRegex("AccessControlException","org.apache.hadoop.security.AccessControlException","Permission denied: user="+USER+", access=WRITE, inode=\""+path+"\":"+USER+":.*:-r--------.*",rsp.getDocument());
    } else {
      checkFailedResponseWithRegex("AccessControlException","org.apache.hadoop.security.AccessControlException","org.apache.hadoop.security.AccessControlException: Permission denied: user="+USER+", access=WRITE, inode=\"read_only_file.txt\":"+USER+":.*:r--------",rsp.getDocument());
    }
  }
  
  //create a file overwrite as user2
  @Test 
  public void createFileOverWriteAsUser2() throws Exception {
    
    String path=HDFS_DATA_DIR+"/file_update_as_user2.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String token;
    //generate the query parameters
    List<NameValuePair> qparams = new ArrayList<NameValuePair>();
    
    if (IS_SECURE) {
      token = WebHdfsUtils.getDToken(HOST,PORT,WebHdfsKeys.USER2,WebHdfsKeys.USER2_KEYTAB_FILE,WebHdfsKeys.USER2);
      qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_DELEGATION, token));
    }
    
    //generate the query parameters
    qparams.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_USER_NAME,USER2));
    qparams.add(new BasicNameValuePair(WebHdfsKeys.PARAM_OVERWRITE,"true"));
    
    //HDFS-2460
    //Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    //Response rsp = rqs.Put(new File (fileName));
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,qparams,fileName);
    if ( VERSION.equalsIgnoreCase("23") ) {
      checkFailedResponseWithRegex("AccessControlException","org.apache.hadoop.security.AccessControlException","Permission denied: user="+USER2+", access=WRITE, inode=\""+path+"\":"+USER+":.*:-rw-r--r--.*",rsp.getDocument());
    } else {
      checkFailedResponseWithRegex("AccessControlException","org.apache.hadoop.security.AccessControlException","org.apache.hadoop.security.AccessControlException: Permission denied: user="+USER2+", access=WRITE, inode=\"file_update_as_user2.txt\":"+USER+":.*:.*",rsp.getDocument());  
    }
    
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send permission
  //HDFS-2468
  @Test 
  public void createFileSendPermission() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendPermission.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String perm="666";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION,perm));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    //check the permission
    String actPerm = dfs.getFileStatus(hadoopPath).getPermission().toString();
    assertEquals("Check permission for path : "+path, new FsPermission(perm).toString(),actPerm);
  }
    
  //create a file and send invalid permission
  @Test 
  public void createFileSendInvalidPermission() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendInvalidPermission.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String perm="955";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION,perm));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_PERMISSION+"\": Failed to parse \"955\" as a radix-8 short integer.",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send empty permission
  @Test 
  public void createFileSendEmptyPermission() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendEmptyPermission.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String perm="";
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION,perm));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_PERMISSION+"\": Failed to parse \"\" as a radix-8 short integer.",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send buffersize
  @Test 
  public void createFileSendBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendBufferSize.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
    String bufferSize="64";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
  //create a file and send invalid buffersize
  @Test 
  public void createFileSendInvalidBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendInvalidBufferSize.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String bufferSize="invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_BUFFER_SIZE+"\": Failed to parse \"invalid\" as a radix-10 integer.",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send 0 buffersize
  //ignore until jira is fixed
  //HDFS-2469
  @Test 
  public void createFileSendZeroBufferSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendZeroBufferSize.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String bufferSize="0";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"buffersize\": Invalid parameter range: buffersize = 0 < 1",rsp.getDocument());
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send block
  @Test 
  public void createFileSendBlockSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendBlockSize.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
    String blockSize="512";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BLOCK_SIZE,blockSize));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    long actBlockSize = dfs.getFileStatus(hadoopPath).getBlockSize();
    long expBlockSize = Long.valueOf(blockSize);
    assertEquals("Check block size for path: "+path,expBlockSize,actBlockSize);
    
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
  
//create a file and send invalid blocksize
  @Test 
  public void createFileSendInvalidBlockSize() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendInvalidBlockSize.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String blockSize="513";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BLOCK_SIZE,blockSize));
    
    //Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    //Response rsp = rqs.Put(new File (fileName));
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    checkFailedResponse("IOException","java.io.IOException","io.bytes.per.checksum(512) and blockSize(513) do not match. blockSize should be a multiple of io.bytes.per.checksum",rsp.getDocument());  
    WSFRMUtils.checkHttp403(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send replication
  @Test 
  public void createFileSendReplication() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendReplication.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
    String replication="5";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION,replication));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    short actReplication = dfs.getFileStatus(hadoopPath).getReplication();
    long expReplication = Short.valueOf(replication);
    assertEquals("Check replication for path: "+path,expReplication,actReplication);
  }
  
  //create a file and send invalid replication
  @Test 
  public void createFileSendInvalidReplication() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendInvalidReplication.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String replication="invalid";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION,replication));
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \""+WebHdfsKeys.PARAM_REPLICATION+"\": Failed to parse \"invalid\" as a radix-10 short integer.",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file and send 0 replication
  @Test 
  public void createFileSendZeroReplication() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendZeroReplication.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String replication="0";
    
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION,replication));
    
    
    Request rqs = new Request(SCHEME,HOST,PORT,PATH+path,QPARAMS);
    Response rsp = rqs.Put(new File (fileName));
    
    checkFailedResponse("IllegalArgumentException","java.lang.IllegalArgumentException","Invalid value for webhdfs parameter \"replication\": Invalid parameter range: replication = 0 < 1",rsp.getDocument());  
    WSFRMUtils.checkHttp400(rsp,WSFRMKeys.JSON_CONTENT_TYPE);
  }
  
  //create a file send all params
  @Test 
  public void createFileSendAllParams() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createFileSendAllParams.txt";
    String fileName=LOCAL_DATA_DIR+"/file_small_data.txt";
    String perm="666";
    String bufferSize="512";
    String blockSize="1024";
    String replication="5";
        
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BUFFER_SIZE,bufferSize));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_BLOCK_SIZE,blockSize));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_PERMISSION,perm));
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.PARAM_REPLICATION,replication));
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  
    //check the default block size
    long actBlockSize = dfs.getFileStatus(hadoopPath).getBlockSize();
    long expBlockSize = Long.valueOf(blockSize);
    assertEquals("Check block size for path: "+path,expBlockSize, actBlockSize);
    
    //check the default replication HDFS-2456
    short expRep = Short.valueOf(replication);
    short actRep = dfs.getFileStatus(hadoopPath).getReplication();
    assertEquals("Check replication for path: "+path, expRep, actRep);
  
    //check the response content is empty
    assertTrue("Check that the response content is empty",rsp.getContent().isEmpty());
    
    //check the default permission
    String actPerm = dfs.getFileStatus(hadoopPath).getPermission().toString();
    assertEquals("Check permission for path : "+path, new FsPermission(perm).toString(),actPerm);
  }
  
  //create a empty file
  @Test 
  public void createEmptyFile() throws Exception {
    
    String path=HDFS_DATA_DIR+"/createEmtpyFile.txt";
    String fileName=LOCAL_DATA_DIR+"/empty_file";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    FSDataInputStream inps = dfs.open(hadoopPath);
    byte[] buffer = new byte[(int)len];
    //read the data
    inps.readFully(0, buffer);
    inps.close();
    String actData = new String (buffer);
    //compare the data
    WSFRMUtils.compareFileToString(fileName, actData);
  }
}

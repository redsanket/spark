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

public class TestCreateGb extends WebHdfs {

  //create a file
  @Test 
  public void createLarge3GBFile() throws Exception {
    
    String token;
    if (IS_SECURE) {
      token = WebHdfsUtils.getDToken(HOST,PORT,WebHdfsKeys.USER,WebHdfsKeys.USER_KEYTAB_FILE,WebHdfsKeys.USER);
    }

    String path=HDFS_DATA_DIR+"/create_file_3Gb";
    String fileName=LOCAL_DATA_DIR+"/file_3Gb";
    //get the data from hadoop
    Path hadoopPath = new Path(path);
        
    //generate the query parameters
    QPARAMS.add(new BasicNameValuePair(WebHdfsKeys.OPERATOR, WebHdfsKeys.CREATE));
    
    
    Response rsp = Put(SCHEME,HOST,PORT,PATH+path,QPARAMS,fileName);
    
    //check if we get 201 with appropriate content type
    WSFRMUtils.checkHttp201(rsp,WSFRMKeys.APP_OCTET_STREAM_CONTENT_TYPE);
    
    long len = dfs.getFileStatus(hadoopPath).getLen();
    System.out.println("length = " + len);

    // FSDataInputStream inps = dfs.open(hadoopPath);
    // byte[] buffer = new byte[(int)len];
    // //read the data
    // inps.readFully(0, buffer);
    // inps.close();
    // String actData = new String (buffer);
    // //compare the data
    // WSFRMUtils.compareFileToString(fileName, actData);

  }

}

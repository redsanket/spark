package org.hw.webhdfs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.*;

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

public class TestDistcp extends WebHdfs {

  // Copy a large 1Gb file via distcp
  @Test 
  public void distcpLarge1GbFile() throws Exception {
      distcpFile("file_1Gb", "/test_distcp_1Gb.file");
  }

  // Copy a large 2Gb file via distcp
  @Test 
  public void distcpLarge2GbFile() throws Exception {
      distcpFile("file_2Gb", "/test_distcp_2Gb.file");
  }

  // Copy a large 3Gb file via distcp
  @Test 
  public void distcpLarge3GbFile() throws Exception {
      distcpFile("file_3Gb", "/test_distcp_3Gb.file");
  }

  // Copy a large file via distcp
  public void distcpFile(String inputFile, String outputFile) throws Exception {

    String token;
    if (IS_SECURE) {
      token = WebHdfsUtils.getDToken(HOST,PORT,WebHdfsKeys.USER,WebHdfsKeys.USER_KEYTAB_FILE,WebHdfsKeys.USER);
    }

    //get the new host and port and set the other values
    String[] dfs_http_address=dfs.getConf().get("dfs.http.address", "localhost:50070").split(":");
    String HOST=dfs_http_address[0];
    int PORT=Integer.valueOf(dfs_http_address[1]);

    //get the HDFS url from the configs
    String URL=WebHdfsKeys.WEBHDFS+"://"+HOST+":"+PORT;
    String sourceFile=URL+HDFS_DATA_DIR+"/"+inputFile;
    // String sourceFile=URL+HDFS_DATA_DIR+"/file_large_data_1GB.txt";
    // String sourceFile=URL+HDFS_DATA_DIR+"/file_large_data_2GB.txt";
    // String sourceFile=URL+HDFS_DATA_DIR+"/file_large_data_3GB.txt.old";
    //String sourceFile=URL+HDFS_DATA_DIR+"/file_large_data_3GB.txt";

    //get the HDFS url from the configs
    String HDFS_URL=hadoopConf.get("fs.default.name", "hdfs://localhost:8020");
    // String targetFile=HDFS_URL+HDFS_DATA_DIR+"/test_distcp.file";
    String targetFile=HDFS_URL+HDFS_DATA_DIR+outputFile;

    // E.g.
    // /home/gs/gridre/yroot.theoden/share/hadoop/bin/hadoop distcp
    // webhdfs://gsbl90269.blue.ygrid.yahoo.com:50070/tmp/webhdfs_data/file_large_data_3GB.txt
    // hdfs://gsbl90269.blue.ygrid.yahoo.com:8020/tmp/webhdfs_data/test_distcp.file 
    String[] command = { "/home/gs/gridre/yroot.theoden/share/hadoop/bin/hadoop",
                         "distcp", sourceFile, targetFile };
    
    //run the cmd and capture the output
    List<String> output = WebHdfsUtils.run_system_cmd(command);

      Boolean foundStatus=false;
      Iterator<String> it = output.iterator();
      while (((!foundStatus) && it.hasNext())) {
        //check if the status has been found
          if ((!foundStatus)&&(it.next().matches(".*.Job job_\\d+_\\d+ completed successfully"))) {
          foundStatus=true;
        }
      }

      if (!foundStatus) {
    System.out.println("output = ");
    for (String strings : output) {
        System.out.println(strings);
    }
      }

      assertTrue("Distcp job completed successfully" , foundStatus);

  }

}


package org.hw.hadoop;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HadoopUtils {
  
  //method to load data from local fs to hadoop
  public static boolean loadDataToHadoop(FileSystem localFS, String localSrc, FileSystem hadoopFS, String hadoopDst, Configuration conf) throws IOException {
    Path localPath= new Path(localSrc);
    Path hadoopPath = new Path(hadoopDst);
    Boolean result=false;
    
    result = FileUtil.copy(localFS, localPath, hadoopFS, hadoopPath, false, true,conf);
    
    return result;
  }
  
  //method to delete data from hadoop
  public static boolean deleteFromHdfs(DistributedFileSystem dfs, String src, Boolean recursive) throws IOException {
    Boolean result=false;
    Path path = new Path(src);
    
    result = dfs.delete(path, recursive);
    
    return result;
  }
}
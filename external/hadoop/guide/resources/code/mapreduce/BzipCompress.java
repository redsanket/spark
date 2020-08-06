package com.verizonmedia.hadoop.examples;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;

public class BzipCompress {

 public static void main(String[] args) {
  Configuration conf = new Configuration();
  InputStream in = null;
  OutputStream out = null;
  try {
   FileSystem fs = FileSystem.get(conf);
   // Input file - local file system
   in = new BufferedInputStream(new FileInputStream
           ("netjs/Hadoop/Data/log.txt"));
   // Output file path in HDFS
   Path outFile = new Path("/user/out/test.bz2");
   // Verifying if the output file already exists
   if (fs.exists(outFile)) {
    System.out.println("Output file already exists");
    throw new IOException("Output file already exists");
   }
   
   out = fs.create(outFile);
   
   // bzip2 compression 
   CompressionCodecFactory factory = new CompressionCodecFactory(conf);
   CompressionCodec codec = factory.getCodecByClassName
     ("org.apache.hadoop.io.compress.BZip2Codec");
   CompressionOutputStream compressionOutputStream = codec.createOutputStream(out);
   
   try {
    IOUtils.copyBytes(in, compressionOutputStream, 4096, false);
    compressionOutputStream.finish();
    
   } finally {
    IOUtils.closeStream(in);
    IOUtils.closeStream(compressionOutputStream);
   }  
  } catch (IOException e) {
   e.printStackTrace();
  }
 }
}
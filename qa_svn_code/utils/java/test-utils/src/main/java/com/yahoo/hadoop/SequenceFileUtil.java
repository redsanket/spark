package com.yahoo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

public class SequenceFileUtil {
  public static CompressionCodec getCompressionCodec(Configuration conf, Path path) throws IOException {
    return getReader(conf, path).getCompressionCodec();
  }

  public static SequenceFile.CompressionType getCompressionType(Configuration conf, Path path) throws IOException {
    return getReader(conf, path).getCompressionType();
  }

  private static SequenceFile.Reader getReader(Configuration conf, Path path) throws IOException {
    return new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
  }

  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();

    //TODO make this cleaner and handle more options
    // find out what to run
    int firstArg = 0;
    String method = args[firstArg++];
    String file = args[firstArg++];

    if ("getCompressionType".equalsIgnoreCase(method)) {
      System.out.println(getCompressionType(conf, new Path(file)));
    } else if ("getCompressionCodec".equalsIgnoreCase(method)) {
      CompressionCodec codec = getCompressionCodec(conf, new Path(file));
      if(codec != null) {
        System.out.println(codec.getClass().getName());
      } else {
        System.out.println("");
      }

    }
  }
}

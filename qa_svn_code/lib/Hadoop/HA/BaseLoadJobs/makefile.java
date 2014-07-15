import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;

public class makefile {

  public static void main(String[] args) throws Exception {

     JobConf conf = new JobConf();
     FileSystem fs = FileSystem.get(conf);

     FSDataOutputStream dostream=null ;
     String s = "this is a string of words that can be counted which is just swell and keen and how about that sandwich\n";

     Path myfile = new Path("/tmp/mypath/myfile.txt");
     if (fs.isFile(myfile)) {
         System.out.println("INFO: "+myfile+" exists, overwriting");
     }
     else {
         System.out.println("INFO: Creating "+myfile);
       try {
           dostream = new FSDataOutputStream(fs.create(fs, myfile, new FsPermission("644"))); 
           dostream.writeChars(s);
           dostream.flush();
           dostream.close();
         } catch (IOException ioe) {
           System.err.println("FAIL: can't create file: " + ioe);
         }
     }
  }
}


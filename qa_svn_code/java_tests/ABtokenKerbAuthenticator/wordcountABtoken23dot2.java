// 20.205 version of wordcount.java, modified to exhibit the token renewal issue
// seen on AxoniteBlue per 4994229/hadoop7853, phw 20120304 
//
//  javac -Xlint:deprecation  -cp  /home/gs/gridre/yroot.omegak/share/hadoop/*:\
//     /home/gs/gridre/yroot.omegak/share/hadoop/lib/*:/home/gs/gridre/yroot.omegak/share/hadoop/modules/*\
//     -d out wordcountABtoken23dot2.java
//
//  jar -cvf  wordcount23dot2token.jar  -C out .
//
import java.io.*;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// add imports for ABtoken issue
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;


public class wordcountABtoken23dot2 {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();

     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line);
       while (tokenizer.hasMoreTokens()) {
         word.set(tokenizer.nextToken());
         output.collect(word, one);
       }
     }
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
       int sum = 0;
       while (values.hasNext()) {
         sum += values.next().get();
       }
       output.collect(key, new IntWritable(sum));
     }
   }

   public static void main(String[] args) throws Exception {
    System.out.println("=====================================================================================");
    System.out.println("wordcountABtoken23dot2: exhibit token issue seen on AxoniteBlue with 20.205patch1");
    System.out.println("wordcountABtoken23dot2: If a regression exists, and after 24 hours of cluster uptime,"); 
    System.out.println("this job fails with \"unable to connect, unable to establish security context\"\n");
    System.out.println("=====================================================================================");

     JobConf conf = new JobConf(wordcountABtoken23dot2.class);
     conf.setJobName("wordcountABtoken23dot2");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     // create bogus delegation token
     FileSystem fs = FileSystem.get(conf);
     Token<?> token = fs.getDelegationToken("NotGoodToken");
     token.setKind(new Text("NOT_A_GOOD_DELEGATION_TOKEN"));
     token.setService(new Text("not a service"));
     Credentials cred = conf.getCredentials();
     cred.addToken(token.getService(), token);

     JobClient.runJob(conf);
   }
}


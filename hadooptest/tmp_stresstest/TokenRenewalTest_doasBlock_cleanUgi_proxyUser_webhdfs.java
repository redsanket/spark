// TokenRenewalTest_doasBlock_cleanUgi_proxyUser - acquire and attempt renew of HDFS and RM DT
// tokens from a login user context but using a new remoteUser UGI to clear the security 
// context. The token renewal should fail since renewers don't match, the tokens should be valid 
// and can be passed to a doAs block where these tokens are the only credentials available to
// submit jobs. However, in the proxy user case, we can't use these tokens, they don't match
// the UGI security context. So, in the doAs context, obtain the UGI for a different (proxy)
// user than the original user, and we will call the doAs block without any credentials, which
// should force the Rm and NN to have to get new tokens, and this should succeed becuase of
// the fallback to the login user's TGT. 
// 20130109 phw
//
// Config needs: on RM, property 'yarn.resourcemanager.delegation.token.renew-interval' 
//               was reduced to 60 (seconds) to force token renewal between jobs, actual
//               setting is in mSec so it's '60000'.
//
//               on RM and NN, add to 'local-superuser-conf.xml';
//               <property>
//                  <name>hadoop.proxyuser.YOUR_USER.groups</name>
//                  <value>users</value>
//               </property>
//               <property>
//                  <name>hadoop.proxyuser.YOUR_USER.hosts</name>
//                  <value>YOUR_HOST.blue.ygrid.yahoo.com</value>
//               </property>
//
// Input needs: the wordcount needs some sizeable text input in hdfs at '/data/in' with
//              path perms 777 (or at least 644)
//
// Output Expectations: token acquisition success with renewal failure, these creds are 
// added to the new ugi, this ugi is passed to a soad block where we try to run two wordcount
// jobs in succession. We should see teh same token behavior as we do in the normal (non doas)
// case, which that Rm and HDFS DTs are acquired, after job submission we automatically 
// get an MR (job history) DT token, in addition to having the existing acquired tokens.
// After a predefiend timeout, <10 minutes for RM and <1 minute for HDFS, the tokens are 
// automatically cancelled.

import java.io.*;
import java.util.*;
import java.net.*;
import java.security.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.security.*;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.security.*;
import org.apache.hadoop.security.token.*;
import org.apache.hadoop.security.token.delegation.*;
import org.apache.hadoop.tools.*;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;

public class TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs extends Configured implements Tool {

  int failFlags = 10000; // test result init but not updated yet

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs(), args);
    System.exit(ret-10000); // if no errors, this will be 0
  }

  public int run(String [] args) throws Exception {

    Configuration conf = new Configuration();
    Cluster cluster = new Cluster(conf);
    DFSClient dfs = new DFSClient(conf);
    FileSystem fs = FileSystem.get(conf);

    // list out our config prop change, should be 60 (seconds)
    System.out.println("Check the renew property setting, yarn.resourcemanager.delegation.token.renew-interval: " + conf.get("yarn.resourcemanager.delegation.token.renew-interval"));
    // don't cancel our tokens so we can use them in second
    conf.setBoolean("mapreduce.job.complete.cancel.delegation.tokens", false);

    UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("dfsload@DEV.YGRID.YAHOO.COM");
    //UserGroupInformation ugiOrig = UserGroupInformation.createRemoteUser("patwhite@DS.CORP.YAHOO.COM");

    // renewer is me
    //Token<? extends TokenIdentifier> myTokenRm = new Token(cluster.getDelegationToken(new Text(ugiOrig.getShortUserName())) );
    //Token<? extends TokenIdentifier> myTokenHdfsFs = new Token(fs.getDelegationToken(ugiOrig.getShortUserName()) );

    // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
    Token<? extends TokenIdentifier> myTokenRm = cluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));
    // Note well, need to use fs.addDelegationTokens() to get an HDFS DT that is recognized by the fs,
    // trying to use fs.getDelegationToken() appears to work but doesn't, fs still auto fetches a token
    // so said token is not being recognized

    Credentials creds = new Credentials();
    creds.addToken(new Text("MyTokenAliasRM"), myTokenRm);

    // TODO: should capture this list and iterate over it, not grab first element...
    Token<?> myTokenHdfsFs = fs.addDelegationTokens("mapredqa", creds)[0];

    // let's see what we got...
    System.out.println("myTokenRm: " + myTokenRm.getIdentifier());
    System.out.println("myTokenRm kind: " + myTokenRm.getKind() + "\n");
    System.out.println("myTokenHdfsFs: " + myTokenHdfsFs.getIdentifier());
    System.out.println("myTokenHdfsFs kind: " + myTokenHdfsFs.getKind() + "\n");

    // add creds to UGI, this adds the two RM tokens, the HDFS token was added already as part
    // of the addDelegationTokens()
    ugiOrig.addCredentials(creds);
    System.out.println("From OriginalUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + creds.numberOfTokens() + " tokens");

    // write our tokenfile
    //creds.writeTokenStorageFile(new Path("/tmp/tokenfile_orig"), conf);

    // we have 2 tokens now, 1 HDFS_DELEGATION_TOKEN and 1 RM_DELEGATION_TOKEN, let's verify 
    // we can't renew them, since renewers don't match 

    long renewTimeHdfs = 0, renewTimeRm = 0;
    System.out.println("\nLet's try to renew our tokens, should fail since renewers don't match...");
    System.out.println("First our HDFS_DELEGATION_TOKEN: ");
    try { renewTimeHdfs = myTokenHdfsFs.renew(conf); }
    catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
    if (renewTimeHdfs > 1357252344100L)  
    {
      failFlags=failFlags+1; // security failure for HDFS token
      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeHdfs we got back is: " + renewTimeHdfs);
    }

    System.out.println("\nOur first RM_DELEGATION_TOKEN: ");
    try { renewTimeRm = myTokenRm.renew(conf); }
    catch (Exception e) { System.out.println("Success, renew failed as expected since we're not the priv user"); }
    if (renewTimeRm > 1357252344100L) 
    {
      failFlags=failFlags+2; //security failure for RM token  
      System.out.println("FAILED! We were allowed to renew a token as ourselves when renewer is priv user.\nThe renewTimeRm we got back is:  " + renewTimeRm + "\n");
    }

    int numTokens = ugiOrig.getCredentials().numberOfTokens();
    System.out.println("We have a total of " + numTokens  + " tokens");
    System.out.println("Dump all tokens currently in our Credentials:");
    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

    // instantiate a seperate object to use for submitting jobs, but don't use
    // the tokens we got since they won't work in the doAs due to mismatching
    // user authentications
    DoasUser du = new DoasUser();
    du.go();

    // back to our original context, our two doAs jobs should have ran as the specified
    // proxy user, dump our existing credentials 
    System.out.println("Back from the doAs block to original context... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + creds.numberOfTokens() + " tokens");
    System.out.println("\nDump all tokens currently in our Credentials:");
    System.out.println(ugiOrig.getCredentials().getAllTokens() + "\n");

    return(failFlags);
  }


  // 
  // class DoasUser
  // this is used create a new UGI (new security context) for running wordcount jobs
  // using the credentials passed into said ugi
  //
  public class DoasUser {
    // DO NOT instantiate objects here, dummy, declare for scope but don't snag 'em yet
    // gotta be in the doas run method to get proxy user's context

    private Credentials doasCreds;
    private Cluster doasCluster;
    private Configuration doasConf;
    private FileSystem doasFs;
    private UserGroupInformation ugi;

    // constructor - nothing is passed in, just use the doAs block to get 
    // a new UGI (new security context)
    DoasUser () throws Exception {
      // DO NOT instantiate objects here either, dummy, gotta be in the doas run method to get
      // proxy user's context

      // get a proxy UGI for hadoopqa, since no creds are passed in we have to get tokens
      // using the TGT fallback for the login user
      try { ugi = UserGroupInformation.createProxyUser("hadoopqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); }
      //tryCurrentUser    try { ugi = UserGroupInformation.createProxyUser("hadoopqa@DEV.YGRID.YAHOO.COM", UserGroupInformation.getLoginUser()); }
      catch (Exception e) { System.out.println("Failed, couldn't get UGI object for proxy user: " + e); }
    }

    //constructor - ugi to use is passed in, uses existing creds that come in with said ugi
    DoasUser (UserGroupInformation ugi) {
      doasCreds = ugi.getCredentials();

      this.ugi = ugi;
    }

    public void go() throws Exception {

      // run as the original user
      String retVal = ugi.doAs(new PrivilegedExceptionAction<String>() {
        public String run() throws Exception {

          // MUST instantiate objs here in the doas go() run method to get the proxy context
          // might not have to be in the run method but just be in the go() method, need
          // to try that...
          doasConf = new Configuration();
          doasCluster = new Cluster(doasConf);
          doasCreds = new Credentials();
          doasFs = FileSystem.get(doasConf);

          // get RM and HDFS token within our proxy ugi context, these we should be able to use
          // renewer is not valid, shouldn't matter now after 23.6 design change for renewer
          Token<?> doasRmToken = doasCluster.getDelegationToken(new Text("GARBAGE1_mapredqa"));

          doasCreds.addToken(new Text("MyDoasTokenAliasRM"), doasRmToken);

          // TODO: should capture this list and iterate over it, not grab first element...
          Token<?> doasHdfsToken = doasFs.addDelegationTokens("mapredqa", doasCreds)[0];

          // let's see what we got...
          System.out.println("doasRmToken: " + doasRmToken.getIdentifier());
          System.out.println("doasRmToken kind: " + doasRmToken.getKind() + "\n");
          System.out.println("doasHdfsToken: " + doasHdfsToken.getIdentifier());
          System.out.println("doasHdfsToken kind: " + doasHdfsToken.getKind() + "\n");

          // add creds to UGI, this adds the two RM tokens, the HDFS token was added already as part
          // of the addDelegationTokens()
          ugi.addCredentials(doasCreds);
          System.out.println("From DoasProxyUser... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I have " + doasCreds.numberOfTokens() + " tokens");

         
           // setup and run a wordcount job, this should use the tokens passed into
           // this doAs block, job should succeed
           Job jobDoas = new Job(getConf());
           jobDoas.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs.class);
           jobDoas.setJobName("TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs_wordcountOrigUser_job1");

           jobDoas.setOutputKeyClass(Text.class);
           jobDoas.setOutputValueClass(IntWritable.class);

           jobDoas.setMapperClass(Map.class);
           jobDoas.setCombinerClass(Reduce.class);
           jobDoas.setReducerClass(Reduce.class);

           jobDoas.setInputFormatClass(TextInputFormat.class);
           jobDoas.setOutputFormatClass(TextOutputFormat.class);

           Path outpath = new Path("/tmp/outfoo");
           if ( outpath.getFileSystem(getConf()).isDirectory(outpath) ) {
             outpath.getFileSystem(getConf()).delete(outpath, true);
             System.out.println("Info: deleted output path: " + outpath );
           }
           FileInputFormat.setInputPaths(jobDoas, new Path("webhdfs://gsbl90565/data/in"));
           FileOutputFormat.setOutputPath(jobDoas, outpath);

           // submit the job, this should automatically get us a jobhistory token,
           // but does not seem to do so...
           jobDoas.submit();

           System.out.print("...wait while the doAs job runs.");
           while ( !jobDoas.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( jobDoas.isSuccessful() ) {
               System.out.println("Job completion successful");
               // open perms on the output
               outpath.getFileSystem(getConf()).setPermission(outpath, new FsPermission("777"));
               outpath.getFileSystem(getConf()).setPermission(outpath.suffix("/part-r-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+10; // first job failed
               System.out.println("Job failed");
           }

           System.out.println("\nAfter doasUser first job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
           
           // setup and run another wordcount job, this should exceed the token renewal time of 60 seconds
           // and cause all of our passed-in tokens to be renewed, job should also succeed
           Job jobDoas2 = new Job(getConf());
           jobDoas2.setJarByClass(TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs.class);
           jobDoas2.setJobName("TokenRenewalTest_doasBlock_cleanUgi_proxyUser_webhdfs_wordcountDoasUser_job2");

           jobDoas2.setOutputKeyClass(Text.class);
           jobDoas2.setOutputValueClass(IntWritable.class);

           jobDoas2.setMapperClass(Map.class);
           jobDoas2.setCombinerClass(Reduce.class);
           jobDoas2.setReducerClass(Reduce.class);

           jobDoas2.setInputFormatClass(TextInputFormat.class);
           jobDoas2.setOutputFormatClass(TextOutputFormat.class);

           Path outpath2 = new Path("/tmp/outfoo2");
           if ( outpath2.getFileSystem(getConf()).isDirectory(outpath2) ) {
             outpath2.getFileSystem(getConf()).delete(outpath2, true);
             System.out.println("Info: deleted output path2: " + outpath2 );
           }
           FileInputFormat.setInputPaths(jobDoas2, new Path("webhdfs://gsbl90565/data/in"));
           FileOutputFormat.setOutputPath(jobDoas2, outpath2);

           // submit the second job, this should also automatically get us a 
           // jobhistory token, but doesn't...
           jobDoas2.submit();

           System.out.print("...wait while the second doAs job runs.");
           while ( !jobDoas2.isComplete() ) {
             System.out.print(".");
             Thread.sleep(2000);
           }
           if ( jobDoas2.isSuccessful() ) {
               System.out.println("Job 2 completion successful");
               // open perms on the output
               outpath2.getFileSystem(getConf()).setPermission(outpath2, new FsPermission("777"));
               outpath2.getFileSystem(getConf()).setPermission(outpath2.suffix("/part-r-00000"), new FsPermission("777"));
           } else {
               failFlags=failFlags+20; // second job failed
               System.out.println("Job 2 failed");
           }

           System.out.println("\nAfter doasUser second job... my Creds say i'm: " + UserGroupInformation.getCurrentUser() + " and I now have " + doasCreds.numberOfTokens() + " tokens");
           System.out.println("\nDump all tokens currently in our Credentials:");
           System.out.println(ugi.getCredentials().getAllTokens() + "\n");

           return "This is the doAs block";
          }
        });
      // back out of the go() method, no longer running as the doAs proxy user

      // write out our tokens back out of doas scope
      //doasCreds.writeTokenStorageFile(new Path("/tmp/tokenfile_doas_out"), doasConf);
     }

  }

  // Mapper and Reducer classes for the wordcount job
  public static class Map
      extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, one);
      }
    }
  }
  public static class Reduce
      extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

}

package hadooptest.regression.yarn;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import hadooptest.TestSession;
import hadooptest.Util;
import hadooptest.cluster.fullydistributed.FullyDistributedCluster;
import hadooptest.job.SleepJob;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;

public class EndToEndPipes extends TestSession {
	
	@BeforeClass
	public static void startTestSession() {
		TestSession.start();
		// TestSession.cluster.start();
	}

	
//	public void deleteHdfsDir() throws IOException {

		/*
		FileSystem fs = TestSession.cluster.getFS();
		FileUtil fileUtil = new FileUtil();
		String[] files = fileUtil.list(new File("/"));
		TestSession.logger.info("Hadoop FS = " + Arrays.toString(files));
*/
		
		/*
		fs.copyFromLocalFile(new Path("/home/user/directory/"), 
		  new Path("/user/hadoop/dir"));
		*/
		
		/*
	    local dirName=$1
	    local fs=$2
	    local skipTrash=''

	    #if $3 not empty and equal to 0 then skip trash
	    if [ -n "$3" ] && [ "$3" -eq "0" ]; then
	        skipTrash="-skipTrash"
	    fi

	    local cmd1
	    local cmd2

	    if [ -z "$fs" ]; then
	        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -ls $dirName"
	        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -rm -r $skipTrash $dirName"
	    else
	        cmd1="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -ls $dirName"
	        cmd2="$HADOOP_HDFS_CMD --config $HADOOP_CONF_DIR dfs -fs $fs -rm -r $skipTrash $dirName"
	    fi
	    echo $cmd1
	    eval $cmd1
	    if [ "$?" -eq 0 ]; then
	        echo $cmd2
	        eval $cmd2
	    fi
	    */
	//}
	
	
	/*
	 * A test for running a pipes wordcount job
	 */
	@Test
	public void runPipesTest()  throws IOException {
		
		TestSession.logger.info("******************* Test FS");		
		FileSystem fs = TestSession.cluster.getFS();		

		String src = "/homes/philips/foobar";
		String dst = "/user/philips";
		fs.copyFromLocalFile(new Path(src), new Path(dst));
		assertTrue("Did not find root dir /", fs.exists(new Path("/")));
		assertTrue("Did not find file!!!",  fs.exists(new Path(dst+"/foobar")));
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		/*
		// FileStatus[] status = fs.listStatus(new Path("hdfs://test.com:9000/user/test/in"));  // you need to pass in your hdfs path
		FileStatus[] status = fs.listStatus(new Path("/"));  // you need to pass in your hdfs path
        for (int i=0;i<status.length;i++){
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
                String line;
                line=br.readLine();
                while (line != null){
                        System.out.println(line);
                        line=br.readLine();
                }
        }
		*/
		
		FileUtil fileUtil = new FileUtil();
		String[] files = fileUtil.list(new File("/"));
		TestSession.logger.info("Hadoop FS = " + Arrays.toString(files));

		
		
		
		// FileSystem fs = cluster.getFS();
		//fs.exists(new Path("/"));
		
		
		// cluster.getFileSystem();
	
		/*
		FileSystem fs = cluster.getFileSystem();
		FileUtil fileUtil = new FileUtil();
		String[] files = fileUtil.list(new File("/"));
		TestSession.logger.info("Hadoop FS = " + Arrays.toString(files));
*/
		
		/*		
		  setTestCaseDesc "Pipes-$TESTCASE_ID - Run a pipes wordcount job"
		deleteHdfsDir Pipes 2>&1
		  createHdfsDir Pipes/pipes-${TESTCASE_ID} 2>&1

		#  echo "Copying HADOOP_PREFIX to $ARTIFACTS/hadoop"
		#  cp -R $HADOOP_PREFIX/ $ARTIFACTS/hadoop  >> $ARTIFACTS_FILE 2>&1

		#  cd $ARTIFACTS/hadoop
		#  export ANT_HOME=/homes/hadoopqa/tools/ant/latest/

		#  echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/pipes/"
		#  chmod -R 755 $ARTIFACTS/hadoop/src/c++/pipes/
		#  echo "Changing permissions of $ARTIFACTS/hadoop/src/c++/utils/"
		#  chmod -R 755 $ARTIFACTS/hadoop/src/c++/utils/
		#  echo "Changing permissions of $ARTIFACTS/hadoop/src/examples/pipes/"
		#  chmod -R 755 $ARTIFACTS/hadoop/src/examples/pipes/

		#  echo "Compiling the examples"
		#  echo "$ANT_HOME/bin/ant -Dcompile.c++=yes examples"
		#  $ANT_HOME/bin/ant -Dcompile.c++=yes examples 
		#  if [[ $? -eq 0 ]] ; then
		    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/pipes-$TESTCASE_ID/c++-examples/Linux-i386-32/bin/ Pipes/pipes-${TESTCASE_ID} 2>&1
		    putLocalToHdfs $JOB_SCRIPTS_DIR_NAME/data/pipes-$TESTCASE_ID/input.txt Pipes/pipes-${TESTCASE_ID}/input.txt 2>&1

		    cd - >> $ARTIFACTS_FILE 2>&1

		    echo "PIPES COMMAND: $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR  pipes -conf  $JOB_SCRIPTS_DIR_NAME/data/pipes-${TESTCASE_ID}/word.xml -input Pipes/pipes-${TESTCASE_ID}/input.txt -output Pipes/pipes-$TESTCASE_ID/outputDir -jobco
		nf mapred.job.name="pipesTest-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=*"
		    echo "OUTPUT:"
		    $HADOOP_MAPRED_CMD --config $HADOOP_CONF_DIR  pipes -conf  $JOB_SCRIPTS_DIR_NAME/data/pipes-${TESTCASE_ID}/word.xml -input Pipes/pipes-${TESTCASE_ID}/input.txt -output Pipes/pipes-$TESTCASE_ID/outputDir -jobconf mapred.job.name="p
		ipesTest-$TESTCASE_ID" -jobconf mapreduce.job.acl-view-job=*
		    validatePipesOutput
		    COMMAND_EXIT_CODE=$?
		#  else
		#    REASONS="Compilation failed. Aborting the test. Bug 4213034"
		#    echo $REASONS
		#    COMMAND_EXIT
		*/
		
		
	}
	
}

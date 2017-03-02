package hadooptest;

import java.net.URL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import hadooptest.TestSession;
import hadooptest.TestSessionCore;

/**
 * A centralized class for utility methods common to the whole
 * of the framework.
 */
public class Util {

	/**
	 * Sleeps the current thread for the specified number of seconds.
	 * 
	 * @param seconds the number of seconds to sleep the current thread.
	 * 
	 * @throws InterruptedException if it can not sleep the current Thread.
	 */
	public static void sleep(int seconds) throws InterruptedException {
		Thread.currentThread().sleep(seconds * 1000);
	}
	
	/**
	 * Get the full path to a resource relative path in the jar.
	 * 
	 * @param relativePath the relative path to the resource in the jar.
	 * 
	 * @return String the full path to the resource in the jar.
	 */
	public static String getResourceFullPath(String relativePath) {
		String fullPath = "";
		URL url = 
				Util.class.getClassLoader().getResource(relativePath);
		fullPath = url.getPath();
		TestSessionCore.logger.debug("Resource URL path=" + fullPath);

		return fullPath;
	}

	/**
         * Reads a file of the following format,
         * 
         * Key1: value1
         * Key2: value2
         *
         * into a map and return it.
         */
	public static HashMap<String, Integer> readMapFromFile(String filePath) throws Exception {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            HashMap<String, Integer> resultWordCount = new HashMap<String, Integer>();

            while ((line = reader.readLine()) != null) {
                if (line.length()>0){
                    String word = line.split(":")[0];
                    Integer count = Integer.parseInt(line.split(":")[1].trim());
                    resultWordCount.put(word, count);
                }
            }
            reader.close();

	    return resultWordCount;
	}

	/**
         * get a test user's password from ykeykey 
	 *
	 * This relies on the yinst pkg hadoopqa_headless_keys, which enables ykeykeygetkey
	 * for test users, like 'headless_user_hitusr_1', standalone commandline use is
	 * like: 'ykeykeygetkey headless_user_hitusr_1', there are four user keys right now
	 * for hitusr_[1-4] 
         */
	public static String getTestUserPasswordFromYkeykey(String test_user) throws Exception {
            String[] output = TestSession.exec.runProcBuilder(new String[] {
                    "ykeykeygetkey",
		    test_user});

            if (!output[0].equals("0")) {
                TestSession.logger.info(
                        "Got unexpected non-zero exit code: " + output[0]);
                TestSession.logger.info("stdout: " + output[1]);
                TestSession.logger.info("stderr: " + output[2]);
                throw new Exception("Failed to read password from ykeykey for user " + test_user);
	    }
	    return output[1].trim();
        }

    /***
     *
     * @param src - The resource to be copied.
     * @param dst - The directory where the resource is to be copied. Retains the same name.
     * @param dstRsrcName - If you want to copy the resource with a different name.
     * @return
     * @throws Exception
     */
	public static String copyFileToHDFS(String src, String dst, String dstRsrcName) throws Exception {

		Path srcPath = new Path(src);
        Path dstPath = new Path(dst);

		FileSystem fs = TestSession.cluster.getFS();
		fs.mkdirs(dstPath);
        if (dstRsrcName != null) {
            dstPath = new Path(dst + "/" + dstRsrcName);
        }
		fs.copyFromLocalFile(srcPath, dstPath);
		return dst;
	}

	public static boolean deleteFromHDFS(String path) throws Exception {
        String[] paths = {path};
	    return deleteFromHDFS(paths);
    }

    public static boolean deleteFromHDFS(String[] paths) throws Exception {
        FileSystem fs = TestSession.cluster.getFS();
        boolean result = true;
        for(String path : paths) {
            result &= fs.delete(new Path(path), true);
        }
        return result;
    }
}

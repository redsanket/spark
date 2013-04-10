package hadooptest;

import java.net.URL;

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
		TestSession.logger.debug("Resource URL path=" + fullPath);

		return fullPath;
	}
}

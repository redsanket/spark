package hadooptest;

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
}

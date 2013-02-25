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
	 */
	public static void sleep(int seconds) {
		try {
			Thread.currentThread().sleep(seconds * 1000);
		}
		catch (InterruptedException ie) {
			TestSession.logger.error("Couldn't sleep the current Thread.");
		}
	}
}

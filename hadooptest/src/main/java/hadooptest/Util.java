package hadooptest;

public class Util {

	public static void sleep(int seconds) {
		try {
			Thread.currentThread().sleep(seconds * 1000);
		}
		catch (InterruptedException ie) {
			System.out.println("Couldn't sleep the current Thread.");
		}
	}
}

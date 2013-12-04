package hadooptest.automation.utils.exceptionParsing;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * Since an exception {@link ExceptionPeel} timestamp runs into milliseconds, an
 * ExceptionBucket marks the start/stop boundary in the granularity of Seconds.
 * Multiple exceptions (running into milliseconds, across different files) can
 * fall in one bucket.
 * </p>
 * <p>
 * It implements the {@link Comparable} interface because one would want to know
 * a "time lapse" of exceptions.
 * </p>
 * 
 * @author tiwari
 * 
 */
public class ExceptionBucket implements Comparable<ExceptionBucket> {
	Timestamp startTime;
	Timestamp endTime;
	List<ExceptionPeel> exceptionPeels;

	public List<ExceptionPeel> getExceptionPeels() {
		return exceptionPeels;
	}

	ExceptionBucket(Timestamp begin, Timestamp end) {
		this.startTime = begin;
		this.endTime = end;
		exceptionPeels = Collections
				.synchronizedList(new ArrayList<ExceptionPeel>());
	}

	void addExceptionPeel(ExceptionPeel exceptionPeel) {
		exceptionPeels.add(exceptionPeel);
	}

	public int compareTo(ExceptionBucket o) {

		if (startTime.after(o.startTime)) {
			return 1;
		} else if (startTime.before(o.startTime)) {
			return -1;
		} else {
			return 0;
		}
	}

	int getNumberOfExceptions() {
		return exceptionPeels.size();
	}

}

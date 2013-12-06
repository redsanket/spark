package hadooptest.automation.utils.exceptionParsing;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * <p>
 * Once an exception is encountered in a log file, it is "peeled off" that file
 * into an ExceptionPeel. This has the entire exception blurb as well as a one
 * line summary and ofcourse the timestamp of the exception
 * </p>
 * <p>
 * These exceptions are then bucketed by {@link ExceptionBucketer}
 * </p>
 * 
 * @author tiwari
 * 
 */
public class ExceptionPeel {

	String blurb;
	Timestamp timestamp;
	String fileName;
	String exceptionOneLiner;

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Filename:");
		sb.append(this.fileName);
		sb.append("\nTimestamp:");
		sb.append(this.timestamp);
		sb.append("\nPEEL:");
		sb.append(blurb);
		return sb.toString();
	}

	public String printSummary() {
		StringBuilder sb = new StringBuilder();
		sb.append("\tFilename:[");
		sb.append(this.fileName);
		sb.append("] One Line Summary[:");
		sb.append(exceptionOneLiner);
		sb.append("]");
		return sb.toString();
	}

	ExceptionPeel(String fileName, String timeStamp, String exceptionOneLiner,
			String blurb) throws ParseException {
		this.fileName = fileName;
		this.exceptionOneLiner = exceptionOneLiner;
		this.blurb = blurb;
		Date date = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss,SSS",
				Locale.ENGLISH).parse(timeStamp);
		this.timestamp = new Timestamp(date.getTime());

	}

	public String getBlurb() {
		return blurb;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public String getFilename() {
		return fileName;
	}

}

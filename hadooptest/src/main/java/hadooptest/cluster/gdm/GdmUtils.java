package hadooptest.cluster.gdm;

import java.io.BufferedReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;

public class GdmUtils
{
	private static Logger log = Logger.getLogger(GdmUtils.class.getName());
    private static final String TEST_CONFIGURATION = "/home/y/conf/gdm_qe_test/config.xml";
	private static Configuration conf;
	
	
	static {
		try {
     	    conf = new XMLConfiguration(TEST_CONFIGURATION);
		}
		catch (ConfigurationException e){
			conf = null;
			log.error("Test Configuration not found: " + TEST_CONFIGURATION , e);
		}
	}
	
	public static String getConfiguration(String path){
		return conf.getString(path);
	}
	
	public static String getFilePath(String paramString)
	{
		String fileName = paramString.substring(paramString.lastIndexOf("/")+1);
		URL localURL = GdmUtils.class.getClassLoader().getResource(fileName);
		if (localURL == null) {
			log.debug("File " + paramString + " not found in classpath");
			return paramString;
		}
		log.debug("File " + paramString + " was found in the classpath at path " + localURL.getPath());

		return localURL.getPath();
	}

	public static String readFile(String paramString)
	{
		String str1 = "";
		paramString = getFilePath(paramString);
		try {
			BufferedReader localBufferedReader = new BufferedReader(new FileReader(paramString));
			String str2 = "";
			while ((str2 = localBufferedReader.readLine()) != null) {
				str1 = str1 + str2;
			}
			localBufferedReader.close();
		} catch (FileNotFoundException localFileNotFoundException) {
			log.error(localFileNotFoundException.toString());
		} catch (IOException localIOException) {
			log.error(localIOException.toString());
		}
		log.debug("Read from " + paramString + ": " + str1);
		log.debug("Total Size: " + str1.length());
		return str1;
	}

	public static Calendar getCalendar() {
		Calendar localCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		return localCalendar;
	}

	public static String getCalendarAsString(Calendar paramCalendar) {
		SimpleDateFormat localSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		localSimpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		return localSimpleDateFormat.format(paramCalendar.getTime());
	}

	private static final String treatSingleDigits(int paramInt) {
		return "0" + String.valueOf(paramInt);
	}

	public static String getCalendarAsString() {
		Calendar localCalendar = Calendar.getInstance();

		SimpleDateFormat localSimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		localSimpleDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

		String str = localSimpleDateFormat.format(localCalendar.getTime());
		log.debug(str);
		return str;
	}
}

package hadooptest.hadoop.stress;

import hadooptest.TestSession;
import hadooptest.monitoring.Monitorable;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import coretest.SerialTests;


@Category(SerialTests.class)
public class TestMonitoring extends TestSession{

	Logger logger = Logger.getLogger(TestMonitoring.class);
	@Test
	@Monitorable(cpuPeriodicity = 5, memPeriodicity = 5)
	public void testWithMonitoring() throws InterruptedException {
		
		logger.info("Beginning Stress test.............sleeping for 10 secs");
		Thread.sleep(30000);
		logger.info("Test waking up, to finish!!!!!!!");
	}

	@Test
	@Monitorable(cpuPeriodicity = 10, memPeriodicity = 10)
	public void secondTestBeingMonitored() throws InterruptedException{
		logger.info("Beginning 2nd Stress test.............for the 2nd test!");
		Thread.sleep(30000);
		logger.info("2ns stress test finished");
	}

}

package hadooptest.dfs.regression;

import hadooptest.TestSession;
import hadooptest.automation.factories.yarnClientImpl.IYarnClientFunctionality;
import hadooptest.automation.factories.yarnClientImpl.YarnClientImplFactory;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestYarnClientImplFactory extends TestSession {
	Logger logger = Logger.getLogger(TestYarnClientImplFactory.class);

	@BeforeClass
	public static void startTestSession() throws Exception {
		TestSession.start();
	}

	@Test
	@Deprecated
	public void testFactory() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException, SecurityException,
			NoSuchMethodException, IllegalArgumentException,
			InvocationTargetException {
		IYarnClientFunctionality yarnClientImpl;
		yarnClientImpl = YarnClientImplFactory.get();

		yarnClientImpl.init(TestSession.getCluster().getConf());
		yarnClientImpl.start();
		for (QueueInfo aQueueInfo : yarnClientImpl.getAllQueues()) {
			logger.info("Queue=" + aQueueInfo.getQueueName());
		}

	}
}

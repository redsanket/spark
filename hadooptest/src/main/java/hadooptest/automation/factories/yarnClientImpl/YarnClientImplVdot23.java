package hadooptest.automation.factories.yarnClientImpl;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.log4j.Logger;

public class YarnClientImplVdot23 implements IYarnClientFunctionality {
	 Logger logger = Logger.getLogger(YarnClientImplVdot23.class);
	String classPath = "org.apache.hadoop.yarn.client.YarnClientImpl";
	Class<?>[] paramConfiguration = new Class[1];
	Class<?>[] noParams = {};
	Class<?> yarnCLientImplClass;
	Method method;
	Object obj;

	public YarnClientImplVdot23() throws ClassNotFoundException {
		yarnCLientImplClass = Class.forName(classPath);
		logger.info("Factory created YarnClientImplV0x23 object");

	}

	public void init(Configuration config) throws SecurityException,
			NoSuchMethodException, InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		paramConfiguration[0] = Configuration.class;
		method = yarnCLientImplClass.getDeclaredMethod("init",
				paramConfiguration);

		obj = yarnCLientImplClass.newInstance();
		method.invoke(obj, config);

	}

	public void start() throws SecurityException, NoSuchMethodException,
			InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		method = yarnCLientImplClass.getDeclaredMethod("start", noParams);

		method.invoke(obj, (Object[]) null);

	}

	@SuppressWarnings("unchecked")
	public List<QueueInfo> getAllQueues() throws SecurityException,
			NoSuchMethodException, InstantiationException,
			IllegalAccessException, IllegalArgumentException,
			InvocationTargetException {
		method = yarnCLientImplClass
				.getDeclaredMethod("getAllQueues", noParams);

		List<QueueInfo> queues = (List<QueueInfo>) method.invoke(obj,
				(Object[]) null);

		return queues;
	}
}

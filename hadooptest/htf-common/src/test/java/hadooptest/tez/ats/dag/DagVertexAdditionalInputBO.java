package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

public class DagVertexAdditionalInputBO {
	String name;
	String clazz;
	String initializer;
	
	public void dump(){
		TestSession.logger.info("DUMPING ADDITIONAL INPUT");
		TestSession.logger.info("name:" + name);
		TestSession.logger.info("class:" + clazz);
		TestSession.logger.info("initializer:" + clazz);
	}
}

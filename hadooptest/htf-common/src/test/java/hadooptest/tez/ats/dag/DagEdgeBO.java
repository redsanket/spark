package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

public class DagEdgeBO {
	String edgeId;
	String inputVertexName;
	String outputVertexName;
	String dataMovementType;
	String dataSourceType;
	String schedulingType;
	String edgeSourceClass;
	String edgeDestinationClass;

	public void dump() {
		TestSession.logger.info("");
		TestSession.logger.info(" edgeId:" + edgeId);
		TestSession.logger.info(" inputVertexName:" + inputVertexName);
		TestSession.logger.info(" outputVertexName:" + outputVertexName);
		TestSession.logger.info(" dataMovementType:" + dataMovementType);
		TestSession.logger.info(" dataSourceType:" + dataSourceType);
		TestSession.logger.info(" schedulingType:" + schedulingType);
		TestSession.logger.info(" edgeSourceClass:" + edgeSourceClass);
		TestSession.logger
				.info(" edgeDestinationClass:" + edgeDestinationClass);

	}
}

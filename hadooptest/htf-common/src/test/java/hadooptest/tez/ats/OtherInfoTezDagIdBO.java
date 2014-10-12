package hadooptest.tez.ats;

import java.util.ArrayList;
import java.util.List;

import hadooptest.TestSession;

/**
 * This class provides the real Object to be used for the abstract
 * parent class {@code ATSOtherInfoEntityBO}. The object is instantiated
 * in individual methods in {@code ATSUtils} that consume the "otherinfo"
 * key in the response.
 * @author tiwari
 *
 */

public class OtherInfoTezDagIdBO extends ATSOtherInfoEntityBO {
	public Long startTime;
	public String status;
	public Long initTime;
	public Long timeTaken;
	public String applicationId;
	public DagPlanBO dagPlan;
	public Long endTime;
	public String diagnostics;
	public List<CounterGroup>counters;

	public OtherInfoTezDagIdBO() {
		this.dagPlan = new DagPlanBO();
		this.counters = new ArrayList<CounterGroup>();
	}

	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("Starttime:" + startTime);
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("initTime:" + initTime);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("applicationId:" + applicationId);
		TestSession.logger.info("endTime:" + endTime);
		TestSession.logger.info("diagnostics:" + diagnostics);
		dagPlan.dump();
		for(CounterGroup aCounterGroup:counters){
			aCounterGroup.dump();
		}
	}

	public static class DagPlanBO {
		public String dagName;
		public Long version;

		public List<DagPlanVertexBO> vertices;
		public List<DagPlanEdgeBO> edges;

		public DagPlanBO() {
			this.vertices = new ArrayList<DagPlanVertexBO>();
			this.edges = new ArrayList<DagPlanEdgeBO>();
		}

		public void dump() {
			for (DagPlanVertexBO aVertex : vertices) {
				aVertex.dump();
			}
			for (DagPlanEdgeBO aDagEdgeBO : edges) {
				aDagEdgeBO.dump();
			}

		}

		static public  class DagPlanVertexBO {
			public String vertexName;
			public String processorClass;
			public List<String> inEdgeIds;
			public List<String> outEdgeIds;
			public List<DagVertexAdditionalInputBO> additionalInputs;

			public DagPlanVertexBO() {
				inEdgeIds = new ArrayList<String>();
				outEdgeIds = new ArrayList<String>();
				additionalInputs = new ArrayList<DagVertexAdditionalInputBO>();
			}
			
			static public class DagVertexAdditionalInputBO {
				public String name;
				public String clazz;
				public String initializer;
				
				public void dump(){
					TestSession.logger.info("DUMPING ADDITIONAL INPUT");
					TestSession.logger.info("name:" + name);
					TestSession.logger.info("class:" + clazz);
					TestSession.logger.info("initializer:" + clazz);
				}
			}
			public void dump() {
				TestSession.logger.info("DAG INFO VERTEX PLAN ENTITY");
				TestSession.logger.info("VertexName:" + vertexName);
				TestSession.logger.info("processorClass:" + processorClass);
				for (String anInEdge : inEdgeIds) {
					TestSession.logger.info("inEdge:" + anInEdge);
				}
				for (String outEdge : outEdgeIds) {
					TestSession.logger.info("outEdge:" + outEdge);
				}
			}

		}

		static public class DagPlanEdgeBO {
			public String edgeId;
			public String inputVertexName;
			public String outputVertexName;
			public String dataMovementType;
			public String dataSourceType;
			public String schedulingType;
			public String edgeSourceClass;
			public String edgeDestinationClass;

			public void dump() {
				TestSession.logger.info("");
				TestSession.logger.info("DUMPING EDGE");
				TestSession.logger.info(" edgeId:" + edgeId);
				TestSession.logger.info(" inputVertexName:" + inputVertexName);
				TestSession.logger
						.info(" outputVertexName:" + outputVertexName);
				TestSession.logger
						.info(" dataMovementType:" + dataMovementType);
				TestSession.logger.info(" dataSourceType:" + dataSourceType);
				TestSession.logger.info(" schedulingType:" + schedulingType);
				TestSession.logger.info(" edgeSourceClass:" + edgeSourceClass);
				TestSession.logger.info(" edgeDestinationClass:"
						+ edgeDestinationClass);

			}
		}

	}

}

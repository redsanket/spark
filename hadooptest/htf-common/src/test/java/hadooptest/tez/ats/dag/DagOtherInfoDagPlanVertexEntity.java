package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagOtherInfoDagPlanVertexEntity {
	String vertexName;
	String processorClass;
	List<String> inEdgeIds;
	List<String> outEdgeIds;
	List<DagVertexAdditionalInput> additionalInputs;
	DagOtherInfoDagPlanVertexEntity(){
		inEdgeIds = new ArrayList<String>();
		outEdgeIds = new ArrayList<String>();
		additionalInputs = new ArrayList<DagVertexAdditionalInput>();
	}
	public void dump(){
		TestSession.logger.info("DAG INFO VERTEX PLAN ENTITY");
		TestSession.logger.info("VertexName:" + vertexName);
		TestSession.logger.info("processorClass:" + processorClass);
		for(String anInEdge:inEdgeIds){
			TestSession.logger.info("inEdge:" + anInEdge);
		}
		for(String outEdge:outEdgeIds){
			TestSession.logger.info("outEdge:" + outEdge);
		}
	}
	
}

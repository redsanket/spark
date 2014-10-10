package hadooptest.tez.ats.dag;

import java.util.ArrayList;
import java.util.List;

public class DagPlan {
	String dagName;
	Long version; 
	List<DagOtherInfoDagPlanVertexEntity> vertices;
	DagPlan(){
		this.vertices = new ArrayList<DagOtherInfoDagPlanVertexEntity>();
	}
	public void dump(){
		for (DagOtherInfoDagPlanVertexEntity aVertex:vertices){
			aVertex.dump();
		}
	}
}

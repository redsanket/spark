package hadooptest.tez.ats.dag;

import java.util.ArrayList;
import java.util.List;

public class DagPlanBO {
	String dagName;
	Long version; 
	List<DagOtherInfoDagPlanVertexEntityBO> vertices;
	DagPlanBO(){
		this.vertices = new ArrayList<DagOtherInfoDagPlanVertexEntityBO>();
	}
	public void dump(){
		for (DagOtherInfoDagPlanVertexEntityBO aVertex:vertices){
			aVertex.dump();
		}
	}
}

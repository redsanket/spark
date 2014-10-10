package hadooptest.tez.ats.dag;

import java.util.ArrayList;
import java.util.List;

public class ProcessedDagResponse {
	List<DagEntityBO> entities;
	ProcessedDagResponse(){
		this.entities = new ArrayList<DagEntityBO>();
	}
	
	public void dump(){
		for(DagEntityBO aDagEntity:entities){
			aDagEntity.dump();
		}
	}
}

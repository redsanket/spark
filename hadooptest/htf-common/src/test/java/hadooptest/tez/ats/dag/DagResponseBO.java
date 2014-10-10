package hadooptest.tez.ats.dag;

import java.util.ArrayList;
import java.util.List;

public class DagResponseBO {
	List<DagEntity> entities;
	DagResponseBO(){
		this.entities = new ArrayList<DagEntity>();
	}
	
	public void dump(){
		for(DagEntity aDagEntity:entities){
			aDagEntity.dump();
		}
	}
}

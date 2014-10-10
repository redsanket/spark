package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagPrimaryFiltersBO {
	List<DagPrimaryFilterEntity> primaryFilters;
	DagPrimaryFiltersBO(){
		this.primaryFilters = new ArrayList<DagPrimaryFilterEntity>();
	}
	public void dump(){
		for (DagPrimaryFilterEntity aDagPrimaryFilterEntity:primaryFilters){
			aDagPrimaryFilterEntity.dump();
		}
	}
}

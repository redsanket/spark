package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagPrimaryFiltersBO {
	List<DagPrimaryFilterEntityBO> primaryFilters;
	DagPrimaryFiltersBO(){
		this.primaryFilters = new ArrayList<DagPrimaryFilterEntityBO>();
	}
	public void dump(){
		for (DagPrimaryFilterEntityBO aDagPrimaryFilterEntity:primaryFilters){
			aDagPrimaryFilterEntity.dump();
		}
	}
}

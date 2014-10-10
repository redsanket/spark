package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagPrimaryFilterEntity {
	String filterName;
	List<String> filterList;
	DagPrimaryFilterEntity(String filterName){
		this.filterName = filterName;
		filterList = new ArrayList<String>();
	}
	public void dump(){
		TestSession.logger.info("DUMPING PRIMARY-FILTER ENTITY");
		TestSession.logger.info("FilterName:" + filterName);
		for (String aFilter:filterList){
			TestSession.logger.info(aFilter);
		}
	}
}

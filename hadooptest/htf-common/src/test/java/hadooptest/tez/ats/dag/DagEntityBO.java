package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagEntityBO {
	String entityType;
	String entity;
	Long starttime;
	List<DagRelatedEntityBO>relatedentities;
	List<DagEventsEntityBO> events;
	List<DagPrimaryFiltersBO> primaryfilters;
	DagOtherInfoBO otherinfo;
	List<DagEdgeBO>edges;
	DagEntityBO(){
		this.events = new ArrayList<DagEventsEntityBO>();		
		this.primaryfilters = new ArrayList<DagPrimaryFiltersBO>();
		this.otherinfo = new DagOtherInfoBO();
		this.relatedentities = new ArrayList<DagRelatedEntityBO>();
		this.edges = new ArrayList<DagEdgeBO>();
	}
	
	public void dump(){
		TestSession.logger.info("entitytype:" + entityType);
		TestSession.logger.info("entity:" + entity);
		TestSession.logger.info("starttime:" + starttime);
		for (DagEventsEntityBO aDagEventsEntity:events){
			aDagEventsEntity.dump();
		}
		for (DagRelatedEntityBO aDagRelatedEntity:relatedentities){
			aDagRelatedEntity.dump();
		}
		for (DagPrimaryFiltersBO aDagPrimaryFiltersBO:primaryfilters){
			aDagPrimaryFiltersBO.dump();
		}
		for (DagEdgeBO aDagEdgeBO:edges){
			aDagEdgeBO.dump();
		}

		otherinfo.dump();
	}
}

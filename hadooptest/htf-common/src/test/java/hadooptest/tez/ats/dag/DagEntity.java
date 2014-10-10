package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagEntity {
	String entityType;
	String entity;
	Long starttime;
	List<DagRelatedEntity>relatedentities;
	List<DagEventsEntity> events;
	List<DagPrimaryFiltersBO> primaryfilters;
	DagOtherInfoBO otherinfo;
	DagEntity(){
		this.events = new ArrayList<DagEventsEntity>();
		
		this.primaryfilters = new ArrayList<DagPrimaryFiltersBO>();
		this.otherinfo = new DagOtherInfoBO();
		this.relatedentities = new ArrayList<DagRelatedEntity>();
	}
	public void dump(){
		TestSession.logger.info("entitytype:" + entityType);
		TestSession.logger.info("entity:" + entity);
		TestSession.logger.info("starttime:" + starttime);
		for (DagEventsEntity aDagEventsEntity:events){
			aDagEventsEntity.dump();
		}
		for (DagRelatedEntity aDagRelatedEntity:relatedentities){
			aDagRelatedEntity.dump();
		}
		for (DagPrimaryFiltersBO aDagPrimaryFiltersBO:primaryfilters){
			aDagPrimaryFiltersBO.dump();
		}

		otherinfo.dump();
	}
}

package hadooptest.tez.ats;

import hadooptest.TestSession;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EntityInGenericATSResponseBO {
	public List<ATSEventsEntityBO> events;
	public String entityType;
	public String entity;
	public Long starttime;
	public Map<String, List<String>>relatedentities;
	public Map<String, List<String>> primaryfilters;
	public ATSOtherInfoEntityBO otherinfo;

	public EntityInGenericATSResponseBO(EntityTypes entityType){
		this.events = new ArrayList<ATSEventsEntityBO>();
		//Seed the primary filters
		this.primaryfilters = new  HashMap<String, List<String>>();
		if (entityType == EntityTypes.TEZ_DAG_ID){
			this.primaryfilters.put("dagName", new ArrayList<String>());
			this.primaryfilters.put("user", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_APPLICATION_ATTEMPT){
			this.primaryfilters.put("user", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_VERTEX_ID){
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_TASK_ID){
			this.primaryfilters.put("TEZ_VERTEX_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_TASK_ATTEMPT_ID){
			this.primaryfilters.put("TEZ_VERTEX_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_TASK_ID", new ArrayList<String>());
		}else{
			//EntityTypes.TEZ_CONTAINER_ID
			this.primaryfilters = new HashMap<String, List<String>>();
		}
		
		//Seed the related entities
		this.relatedentities = new  HashMap<String, List<String>>();
		if (entityType == EntityTypes.TEZ_DAG_ID){
			this.relatedentities.put("TEZ_VERTEX_ID", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_APPLICATION_ATTEMPT){
			this.relatedentities.put("TEZ_CONTAINER_ID", new ArrayList<String>());
			this.relatedentities.put("TEZ_DAG_ID", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_VERTEX_ID){
			this.relatedentities.put("TEZ_TASK_ID", new ArrayList<String>());
		}else if(entityType == EntityTypes.TEZ_TASK_ID){
			this.relatedentities.put("TEZ_TASK_ATTEMPT_ID", new ArrayList<String>());
		}else{
			//EntityTypes.TEZ_CONTAINER_ID, or
			//EntityTypes.TEZ_TASK_ATTEMPT_ID
			this.relatedentities = new HashMap<String, List<String>>();
		}

		this.otherinfo = null;
	}
	
	public void dump(){
		TestSession.logger.info("entitytype:" + entityType);
		TestSession.logger.info("entity:" + entity);
		TestSession.logger.info("starttime:" + starttime);
		for (ATSEventsEntityBO aDagEventsEntity:events){
			aDagEventsEntity.dump();
		}
		TestSession.logger.info("DUMPING RELATED ENTITIES");
		for (String key:relatedentities.keySet()){
			TestSession.logger.info(key +":");
			TestSession.logger.info(relatedentities.get(key));
		}

		TestSession.logger.info("DUMPING PRIMARY FILTERS");
		for (String key:primaryfilters.keySet()){
			TestSession.logger.info(key +":");
			TestSession.logger.info(primaryfilters.get(key));
		}

		otherinfo.dump();
	}
}

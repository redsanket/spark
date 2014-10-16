package hadooptest.tez.ats;

import hadooptest.TestSession;
import hadooptest.tez.ats.ATSTestsBaseClass.EntityTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A REST call into a timelineserver returns either a single entity or a
 * JSONArray of entities. An entity has a set of common keys and a "otherinfo"
 * key that varies from call to call. Also within the common portion the
 * "primaryfilters" key can have different keys based on the REST call. To keep
 * the downstream code generic this class cares for seeding those keys, when it
 * is constructed, so that the clients are agnostic to the details and can work
 * off of keySet() contents. The otherInfo object is null, because it is an
 * abstract class here. Since the info varies drastically from call to call, the
 * {@code ATSUtils} class provides methods that provide the correct
 * implementation Object.
 * 
 * @author tiwari
 * 
 */
public class EntityInGenericATSResponseBO {
	public List<ATSEventsEntityBO> events;
	public String entityType;
	public String entity;
	public Long starttime;
	public Map<String, List<String>> relatedentities;
	public Map<String, List<String>> primaryfilters;
	public ATSOtherInfoEntityBO otherinfo;

	public EntityInGenericATSResponseBO(EntityTypes entityType) {
		this.events = new ArrayList<ATSEventsEntityBO>();

		// Seed the primary filters
		this.primaryfilters = new HashMap<String, List<String>>();
		if (entityType == EntityTypes.TEZ_DAG_ID) {
			this.primaryfilters.put("dagName", new ArrayList<String>());
			this.primaryfilters.put("user", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_APPLICATION_ATTEMPT) {
			this.primaryfilters.put("user", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_VERTEX_ID) {
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_TASK_ID) {
			this.primaryfilters.put("TEZ_VERTEX_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_TASK_ATTEMPT_ID) {
			this.primaryfilters.put("TEZ_VERTEX_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_DAG_ID", new ArrayList<String>());
			this.primaryfilters.put("TEZ_TASK_ID", new ArrayList<String>());
		} else {
			// EntityTypes.TEZ_CONTAINER_ID
			this.primaryfilters = new HashMap<String, List<String>>();
		}

		// Seed the related entities
		this.relatedentities = new HashMap<String, List<String>>();
		if (entityType == EntityTypes.TEZ_DAG_ID) {
			this.relatedentities.put("TEZ_VERTEX_ID", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_APPLICATION_ATTEMPT) {
			this.relatedentities.put("TEZ_CONTAINER_ID",
					new ArrayList<String>());
			this.relatedentities.put("TEZ_DAG_ID", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_VERTEX_ID) {
			this.relatedentities.put("TEZ_TASK_ID", new ArrayList<String>());
		} else if (entityType == EntityTypes.TEZ_TASK_ID) {
			this.relatedentities.put("TEZ_TASK_ATTEMPT_ID",
					new ArrayList<String>());
		} else {
			// EntityTypes.TEZ_CONTAINER_ID, or
			// EntityTypes.TEZ_TASK_ATTEMPT_ID
			this.relatedentities = new HashMap<String, List<String>>();
		}

		this.otherinfo = null;
	}

	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof EntityInGenericATSResponseBO)) {
			TestSession.logger
					.error("Not an instance of EntityInGenericATSResponseBO");
			return false;
		}
		EntityInGenericATSResponseBO other = (EntityInGenericATSResponseBO) arg;

		if (this.events.size() != other.events.size()
				|| this.relatedentities.size() != other.relatedentities.size()
				|| this.primaryfilters.size() != other.primaryfilters.size()) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}

		for (int xx = 0; xx < this.events.size(); xx++) {
			if (!(this.events.get(xx).equals(other.events.get(xx)))) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		for (String key : this.relatedentities.keySet()) {
			if (!other.relatedentities.containsKey(key)) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			if (!(this.relatedentities.get(key).equals(other.relatedentities
					.get(key)))) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		for (String key : this.primaryfilters.keySet()) {
			if (!other.primaryfilters.containsKey(key)) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			if (!(this.primaryfilters.get(key).equals(other.primaryfilters
					.get(key)))) {
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}

		if (!this.entity.equals(other.entity)
				|| !this.entityType.equals(other.entityType)
				|| !this.otherinfo.equals(other.otherinfo)
				|| this.starttime.longValue() != other.starttime.longValue()) {
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		return true;
	}

	public int hashCode() {
		int hash = 0;
		hash = hash * 37 + events.hashCode();
		hash = hash * 37 + entityType.hashCode();
		hash = hash * 37 + entity.hashCode();
		hash = hash * 37 + starttime.hashCode();
		hash = hash * 37 + relatedentities.hashCode();
		hash = hash * 37 + primaryfilters.hashCode();
		hash = hash * 37 + otherinfo.hashCode();

		return hash;
	}

	public void dump() {

		TestSession.logger.info("entitytype:" + entityType);
		TestSession.logger.info("entity:" + entity);
		TestSession.logger.info("starttime:" + starttime);
		if (events != null) {
			for (ATSEventsEntityBO aDagEventsEntity : events) {
				aDagEventsEntity.dump();
			}
		}
		if (relatedentities != null) {
			TestSession.logger.info("DUMPING RELATED ENTITIES");
			for (String key : relatedentities.keySet()) {
				TestSession.logger.info(key + ":");
				TestSession.logger.info(relatedentities.get(key));
			}
		}

		if (primaryfilters != null) {
			TestSession.logger.info("DUMPING PRIMARY FILTERS");
			for (String key : primaryfilters.keySet()) {
				TestSession.logger.info(key + ":");
				TestSession.logger.info(primaryfilters.get(key));
			}
		}
		if (otherinfo != null){
			otherinfo.dump();
		}
	}

}

package hadooptest.tez.ats;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

/**
 * The REST respone to calls to the timelineserver can return either a single
 * entity (for instance, a call to /TEZ_DAG_ID/dag_123) would return a single
 * response that is not a JSONArray. On the other hand a call to /TEZ_DAG_ID/
 * would return all the DAGs enclosed in a JSONArray
 * 
 * This class cares for that. This is what the tests would consume.
 * 
 * @author tiwari
 * 
 */
public class GenericATSResponseBO {
	public List<EntityInGenericATSResponseBO> entities;

	public GenericATSResponseBO() {
		entities = new ArrayList<EntityInGenericATSResponseBO>();
	}

	@Override
	public boolean equals(Object arg) {
		if (!(arg instanceof GenericATSResponseBO)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		GenericATSResponseBO other = (GenericATSResponseBO) arg;
		if (this.entities.size() != other.entities.size()){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		for (int xx = 0; xx < this.entities.size(); xx++) {
			if (!this.entities.get(xx).equals(other.entities.get(xx))){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		int hash = 0;
		hash = hash * 37 + entities.hashCode();
		return hash;
	}

	public void dump() {
		for (EntityInGenericATSResponseBO anEntity : entities) {
			anEntity.dump();
		}
	}
}

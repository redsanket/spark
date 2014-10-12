package hadooptest.tez.ats;

import java.util.ArrayList;
import java.util.List;

/**
 * The REST respone to calls to the timelineserver can return either
 * a single entity (for instance, a call to /TEZ_DAG_ID/dag_123) would
 * return a single response that is not a JSONArray. On the other hand
 * a call to /TEZ_DAG_ID/ would return all the DAGs enclosed in a JSONArray
 * 
 *  This class cares for that. This is what the tests would consume.
 *  
 * @author tiwari
 *
 */
public class GenericATSResponseBO {
	public List<EntityInGenericATSResponseBO> entities;
	public GenericATSResponseBO(){
		entities = new ArrayList<EntityInGenericATSResponseBO>();
	}
	public void dump(){
		for (EntityInGenericATSResponseBO anEntity:entities){
			anEntity.dump();
		}
	}
}

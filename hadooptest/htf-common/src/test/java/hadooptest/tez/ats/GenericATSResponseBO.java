package hadooptest.tez.ats;

import java.util.ArrayList;
import java.util.List;

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

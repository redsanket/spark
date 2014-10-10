package hadooptest.tez.ats.dag;

import hadooptest.TestSession;

import java.util.ArrayList;
import java.util.List;

public class DagRelatedEntityBO {
	String relatedEntityName;
	List<String> tezVertexIds;

	DagRelatedEntityBO(String relatedEntityName) {
		this.relatedEntityName = relatedEntityName;
		this.tezVertexIds = new ArrayList<String>();
	}
	public void dump(){
		TestSession.logger.info("DUMPING RELATED ENTITY NAME");
		TestSession.logger.info("relatedEntityName:" + relatedEntityName);
		for (String vertexId:tezVertexIds){
			TestSession.logger.info("VertexId:" + vertexId);
		}
	}

}

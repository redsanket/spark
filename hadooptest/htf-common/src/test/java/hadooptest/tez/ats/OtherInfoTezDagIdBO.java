package hadooptest.tez.ats;

import java.util.ArrayList;
import java.util.List;

import hadooptest.TestSession;

/**
 * This class provides the real Object to be used for the abstract
 * parent class {@code ATSOtherInfoEntityBO}. The object is instantiated
 * in individual methods in {@code ATSUtils} that consume the "otherinfo"
 * key in the response.
 * @author tiwari
 *
 */

public class OtherInfoTezDagIdBO extends ATSOtherInfoEntityBO {
	public Long startTime;
	public String status;
	public Long initTime;
	public Long timeTaken;
	public String applicationId;
	public DagPlanBO dagPlan;
	public Long endTime;
	public String diagnostics;
	public List<CounterGroup>counters;

	public OtherInfoTezDagIdBO(){
		this.dagPlan = new DagPlanBO();
		this.counters = new ArrayList<CounterGroup>();
	}


	@Override
	public boolean equals(Object arg){
		TestSession.logger.info("Comparing OtherInfoTezDagIdBO");
		if (!(arg instanceof OtherInfoTezDagIdBO)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		OtherInfoTezDagIdBO other = (OtherInfoTezDagIdBO) arg;
		if( this.startTime.longValue()!=other.startTime.longValue() ||
				!this.status.equals(other.status) ||
				this.initTime.longValue()!=other.initTime.longValue() ||
				this.timeTaken.longValue()!=other.timeTaken.longValue() ||
				!this.applicationId.equals(other.applicationId) ||
				this.endTime.longValue()!=other.endTime.longValue() ||
				!this.diagnostics.equals(other.diagnostics)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		
		
		if (!this.dagPlan.equals(other.dagPlan)){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		
		if (counters.size() != other.counters.size()){
			TestSession.logger.error("Equality failed here!");
			return false;
		}
		
		for (int xx=0;xx<counters.size();xx++){
			if(!counters.get(xx).equals(other.counters.get(xx))){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
		}
		
		return true;
	}
	
	@Override
	public int hashCode(){
		int hash = 0;
		hash = hash *37 + this.startTime.hashCode();
		hash = hash *37 + this.status.hashCode();
		hash = hash *37 + this.initTime.hashCode();
		hash = hash *37 + this.timeTaken.hashCode();
		hash = hash *37 + this.applicationId.hashCode();
		hash = hash *37 + this.dagPlan.hashCode();
		hash = hash *37 + this.endTime.hashCode();
		hash = hash *37 + this.diagnostics.hashCode();
		return hash;
	}
	
	public void dump() {
		TestSession.logger.info("DUMPING OTHER INFO");
		TestSession.logger.info("Starttime:" + startTime);
		TestSession.logger.info("status:" + status);
		TestSession.logger.info("initTime:" + initTime);
		TestSession.logger.info("timeTaken:" + timeTaken);
		TestSession.logger.info("applicationId:" + applicationId);
		TestSession.logger.info("endTime:" + endTime);
		TestSession.logger.info("diagnostics:" + diagnostics);
		dagPlan.dump();
		for(CounterGroup aCounterGroup:counters){
			aCounterGroup.dump();
		}
	}

	public static class DagPlanBO {
		public String dagName;
		public Long version;

		public List<DagPlanVertexBO> vertices;
		public List<DagPlanEdgeBO> edges;

		public DagPlanBO() {
			this.vertices = new ArrayList<DagPlanVertexBO>();
			this.edges = new ArrayList<DagPlanEdgeBO>();
		}
		
		@Override
		public boolean equals(Object arg){
			TestSession.logger.info("Comparing DagPlanBO");
			if (!(arg instanceof DagPlanBO)){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			DagPlanBO other = (DagPlanBO) arg;
			if (!this.dagName.equals(other.dagName) ||
				this.version.longValue()!=other.version.longValue()){
					TestSession.logger.error("Equality failed here!");
				return false;
			}
			if (this.vertices.size() != other.vertices.size()){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			if (this.edges.size() != other.edges.size()){
				TestSession.logger.error("Equality failed here!");
				return false;
			}
			
			for (int xx=0;xx<this.vertices.size();xx++){
				if(!this.vertices.get(xx).equals(other.vertices.get(xx))){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
			}
			for (int xx=0;xx<this.edges.size();xx++){
				if(!this.edges.get(xx).equals(other.edges.get(xx))){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
			}
			return true;
			
		}
		public void dump() {
			for (DagPlanVertexBO aVertex : vertices) {
				aVertex.dump();
			}
			for (DagPlanEdgeBO aDagEdgeBO : edges) {
				aDagEdgeBO.dump();
			}

		}

		static public  class DagPlanVertexBO {
			public String vertexName;
			public String processorClass;
			public List<String> inEdgeIds;
			public List<String> outEdgeIds;
			public List<DagVertexAdditionalInputBO> additionalInputs;

			public DagPlanVertexBO() {
				inEdgeIds = new ArrayList<String>();
				outEdgeIds = new ArrayList<String>();
				additionalInputs = new ArrayList<DagVertexAdditionalInputBO>();
			}
			
			@Override
			public boolean equals(Object arg){
				TestSession.logger.info("Comparing DagPlanVertexBO");
				if (!(arg instanceof DagPlanVertexBO))
					return false;
				
				DagPlanVertexBO other = (DagPlanVertexBO)arg;
				if (!this.vertexName.equals(other.vertexName) ||
				(!this.processorClass.equals(other.processorClass))){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				if (this.inEdgeIds.size() != other.inEdgeIds.size()){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				if (this.outEdgeIds.size()!=other.outEdgeIds.size()){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				for (int xx=0;xx<this.inEdgeIds.size();xx++){
					if (!(this.inEdgeIds.get(xx).equals(other.inEdgeIds.get(xx)))){
						TestSession.logger.error("Equality failed here!");
						return false;
					}
				}
				for (int xx=0;xx<this.outEdgeIds.size();xx++){
					if (!(this.outEdgeIds.get(xx).equals(other.outEdgeIds.get(xx)))){
						TestSession.logger.error("Equality failed here!");
						return false;
					}
				}
				
				if(this.additionalInputs.size()!=other.additionalInputs.size()){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				
				for(int xx=0;xx<this.additionalInputs.size();xx++){
					if(!(this.additionalInputs.get(xx).equals(other.additionalInputs.get(xx)))){
						TestSession.logger.error("Equality failed here!");
						return false;
					}
				}
				return true;

			}
			
			@Override
			public int hashCode(){
				int hash = 0;
				hash = hash * 37 + this.processorClass.hashCode();
				hash = hash * 37 + this.vertexName.hashCode();
				hash = hash * 37 + this.inEdgeIds.hashCode();
				hash = hash * 37 + this.outEdgeIds.hashCode();
				hash = hash * 37 + this.additionalInputs.hashCode();
				
				return hash;
			}
			
			static public class DagVertexAdditionalInputBO {
				public String name;
				public String clazz;
				public String initializer;
				
				@Override
				public boolean equals(Object arg){
					TestSession.logger.info("Comparing DagVertexAdditionalInputBO");
					if (!(arg instanceof DagVertexAdditionalInputBO))
						return false;
					DagVertexAdditionalInputBO other = (DagVertexAdditionalInputBO)arg;
					if((!this.name.equals(other.name) || 
							!this.clazz.equals(other.clazz) ||
							!this.initializer.equals(other.initializer))){
						TestSession.logger.error("Equality failed here!");
						return false;						
					}
					return true;
				}
				@Override
				public int hashCode(){
					int hash = 0;
					hash = hash * 37 + this.name.hashCode();
					hash = hash * 37 + this.clazz.hashCode();
					hash = hash * 37 + this.initializer.hashCode();
					
					return hash;
				}
				public void dump(){
					TestSession.logger.info("DUMPING ADDITIONAL INPUT");
					TestSession.logger.info("name:" + name);
					TestSession.logger.info("class:" + clazz);
					TestSession.logger.info("initializer:" + clazz);
				}
			}
			
			public void dump() {
				TestSession.logger.info("DAG INFO VERTEX PLAN ENTITY");
				TestSession.logger.info("VertexName:" + vertexName);
				TestSession.logger.info("processorClass:" + processorClass);
				for (String anInEdge : inEdgeIds) {
					TestSession.logger.info("inEdge:" + anInEdge);
				}
				for (String outEdge : outEdgeIds) {
					TestSession.logger.info("outEdge:" + outEdge);
				}
			}

		}

		static public class DagPlanEdgeBO {
			public String edgeId;
			public String inputVertexName;
			public String outputVertexName;
			public String dataMovementType;
			public String dataSourceType;
			public String schedulingType;
			public String edgeSourceClass;
			public String edgeDestinationClass;

			@Override
			public boolean equals(Object arg){
				TestSession.logger.info("Comparing DagPlanEdgeBO");
				if(!(arg instanceof DagPlanEdgeBO)){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				DagPlanEdgeBO other =(DagPlanEdgeBO)arg;				
				if (!this.edgeId.equals(other.edgeId) ||
						!this.inputVertexName.equals(other.inputVertexName) ||
						!this.outputVertexName.equals(other.outputVertexName) ||
						!this.dataMovementType.equals(other.dataMovementType) ||
						!this.dataSourceType.equals(other.dataSourceType) ||
						!this.schedulingType.equals(other.schedulingType) ||
						!this.edgeDestinationClass.equals(other.edgeDestinationClass)){
					TestSession.logger.error("Equality failed here!");
					return false;
				}
				return true;
			}
			
			@Override
			public int hashCode(){
				int hash = 0;
				hash = hash * 37 + this.edgeId.hashCode();
				hash = hash * 37 + this.inputVertexName.hashCode();
				hash = hash * 37 + this.outputVertexName.hashCode();
				hash = hash * 37 + this.dataMovementType.hashCode();
				hash = hash * 37 + this.dataSourceType.hashCode();
				hash = hash * 37 + this.schedulingType.hashCode();
				hash = hash * 37 + this.edgeSourceClass.hashCode();
				hash = hash * 37 + this.edgeDestinationClass.hashCode();
				return hash;
			}
			
			public void dump() {
				TestSession.logger.info("");
				TestSession.logger.info("DUMPING EDGE");
				TestSession.logger.info(" edgeId:" + edgeId);
				TestSession.logger.info(" inputVertexName:" + inputVertexName);
				TestSession.logger
						.info(" outputVertexName:" + outputVertexName);
				TestSession.logger
						.info(" dataMovementType:" + dataMovementType);
				TestSession.logger.info(" dataSourceType:" + dataSourceType);
				TestSession.logger.info(" schedulingType:" + schedulingType);
				TestSession.logger.info(" edgeSourceClass:" + edgeSourceClass);
				TestSession.logger.info(" edgeDestinationClass:"
						+ edgeDestinationClass);

			}
		}

	}

}

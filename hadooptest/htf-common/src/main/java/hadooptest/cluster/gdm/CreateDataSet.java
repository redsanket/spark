package hadooptest.cluster.gdm;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class CreateDataSet {
	private String dataSetName;
	private String descritionName;
	private String frequency;
	private String projectName;
	private String sourceCluster;
	private String uGIGroup;
	private String uGIOwner;
	private String uGIPermission;
	private String consumerContact;
	private String ownerContact;
	private String publisherContact;
	private String comments;
	private String doneFilePath;
	private JSONArray targets;
	private int targetsCount;
	private JSONArray sourcesPath;
	private JSONObject datasetRequest;
	private JSONObject newDataFeedRequest;

	public CreateDataSet() {
		this.targetsCount = 0;
		this.targets = new JSONArray();
		this.datasetRequest = new JSONObject();
		this.newDataFeedRequest = new JSONObject();
	}

	public CreateDataSet dataSetName(String dataSetName) {
		this.dataSetName = dataSetName;
		this.datasetRequest.put("DataSetName", this.dataSetName);
		return this;
	}

	public CreateDataSet description(String descritionName) {
		this.descritionName = descritionName;
		this.datasetRequest.put("Description", this.descritionName);
		return this;
	}

	public CreateDataSet frequency(String frequency) {
		this.frequency = frequency;
		this.datasetRequest.put("Frequency", this.frequency);
		return this;
	}

	public CreateDataSet projectName(String projectName) {
		this.projectName = projectName;
		this.datasetRequest.put("Project", this.projectName);
		return this;
	}

	public CreateDataSet sourceCluster(String sourceClusterName) {
		this.sourceCluster = sourceClusterName;
		this.datasetRequest.put("SourceCluster", this.sourceCluster);
		return this;
	}
	
	public CreateDataSet uGIGroup(String uGIGroup) {
		this.uGIGroup = uGIGroup;
		this.datasetRequest.put("UGIGroup", this.uGIGroup);
		return this;
	}
	
	public CreateDataSet uGIOwner(String uGIOwner) {
		this.uGIOwner = uGIOwner;
		this.datasetRequest.put("UGIOwner", this.uGIOwner);
		return this;
	}
	
	public CreateDataSet uGIPermission(String uGIPermission) {
		this.uGIPermission = uGIPermission;
		this.datasetRequest.put("UGIPermission", this.uGIPermission);
		return this;
	}
	
	public CreateDataSet consumerContact(String consumerContact) {
		this.consumerContact = consumerContact;
		this.datasetRequest.put("ConsumerContact", this.consumerContact);
		return this;
	}
	
	public CreateDataSet ownerContact(String ownerContact) {
		this.ownerContact = ownerContact;
		this.datasetRequest.put("OwnerContact", this.ownerContact);
		return this;
	}
	
	public CreateDataSet publisherContact(String publisherContact) {
		this.publisherContact = publisherContact ;
		this.datasetRequest.put("PublisherContact", this.publisherContact);
		return this;
	}
	
	public String getPublisherContact() {
		if (this.publisherContact == null) {
			try {
				throw new Exception("publisherContact is not specified.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.publisherContact;
	}
	
	public CreateDataSet comments(String comments) { 
		this.comments = comments;
		this.datasetRequest.put("Comments", this.comments);
		return this;
	}
	
	public CreateDataSet  doneFilePath(String doneFilePath){
		this.doneFilePath = doneFilePath;
		this.datasetRequest.put("DoneFilePath", this.doneFilePath);
		return this;
	}
	
	public CreateDataSet addTarget(Target target ) throws Exception {
		if (this.targets == null) {
			throw new Exception("Target is null");
		} else {
			this.targets.add(this.targetsCount, target.getTarget());
			this.targetsCount++;
		}
		return this;
	}

	public JSONArray getTargets() {
		if (this.targets == null) {
			try {
				throw new Exception("");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.targets;
	}

	public CreateDataSet addSourcePath(SourcePath sourcePath) throws Exception {
		this.sourcesPath = sourcePath.getSourcePath();
		this.datasetRequest.put("SourcePaths",this.sourcesPath.toString());
		return this;
	}

	public String toString() {
		String returnValue = null;
		if (this.datasetRequest == null) {
			try {
				throw new Exception("No value is set.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.datasetRequest.put("Targets", this.getTargets().toString());
		this.newDataFeedRequest.put("NewDataFeedRequest", this.datasetRequest);
		returnValue = this.newDataFeedRequest.toString();
		return returnValue;
	}


}

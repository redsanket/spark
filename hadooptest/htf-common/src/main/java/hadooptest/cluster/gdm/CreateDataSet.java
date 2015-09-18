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
	
	public String getDataSetName() {
		if (this.dataSetName == null) {
			try {
				throw new Exception("Dataset Name not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.dataSetName;
	}

	public CreateDataSet description(String descritionName) {
		this.descritionName = descritionName;
		this.datasetRequest.put("Description", this.descritionName);
		return this;
	}
	
	public String getDescription(){
		if (this.descritionName == null) {
			try {
				throw new Exception("Description not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.descritionName;
	}

	public CreateDataSet frequency(String frequency) {
		this.frequency = frequency;
		this.datasetRequest.put("Frequency", this.frequency);
		return this;
	}
	
	public String getFrequency() {
		if (this.frequency == null) {
			try {
				throw new Exception("Frequency not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.frequency;
	}

	public CreateDataSet projectName(String projectName) {
		this.projectName = projectName;
		this.datasetRequest.put("Project", this.projectName);
		return this;
	}
	
	public String getProjectName() {
		if(this.projectName == null) {
			try {
				throw new Exception("Project name not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.projectName;
	}

	public CreateDataSet sourceCluster(String sourceClusterName) {
		this.sourceCluster = sourceClusterName;
		this.datasetRequest.put("SourceCluster", this.sourceCluster);
		return this;
	}
	
	public String getSourceCluster() {
		if (this.sourceCluster == null) {
			try {
				throw new Exception("Source Cluster not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.sourceCluster;
	}
	
	public CreateDataSet uGIGroup(String uGIGroup) {
		this.uGIGroup = uGIGroup;
		this.datasetRequest.put("UGIGroup", this.uGIGroup);
		return this;
	}
	
	public String getuGIGroup() {
		if (this.uGIGroup == null) {
			try {
				throw new Exception("UGI group not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.uGIGroup;
	}
	
	public CreateDataSet uGIOwner(String uGIOwner) {
		this.uGIOwner = uGIOwner;
		this.datasetRequest.put("UGIOwner", this.uGIOwner);
		return this;
	}
	
	public String getUgiOwner() {
		if (this.uGIOwner == null) {
			try {
				throw new Exception("UGI owner not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.uGIOwner;
	}
	
	public CreateDataSet uGIPermission(String uGIPermission) {
		this.uGIPermission = uGIPermission;
		this.datasetRequest.put("UGIPermission", this.uGIPermission);
		return this;
	}
	
	public String getUgiPermission() {
		if (this.uGIPermission == null) {
			try {
				throw new Exception("UGI permission not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.uGIPermission;
	}
	
	public CreateDataSet consumerContact(String consumerContact) {
		this.consumerContact = consumerContact;
		this.datasetRequest.put("ConsumerContact", this.consumerContact);
		return this;
	}
	
	public String getConsumerContact() {
		if (this.consumerContact == null) {
			try {
				throw new Exception("consumer contact not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.consumerContact;
	}
	
	public CreateDataSet ownerContact(String ownerContact) {
		this.ownerContact = ownerContact;
		this.datasetRequest.put("OwnerContact", this.ownerContact);
		return this;
	}
	
	public String getOwnerContact() {
		if (this.ownerContact == null){
			try {
				throw new Exception("Owner Contact is not specified.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.ownerContact;
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
	
	public String getComments(){
		if (this.comments == null) {
			try {
				throw new Exception("comments not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.comments;
	}
	
	public CreateDataSet  doneFilePath(String doneFilePath){
		this.doneFilePath = doneFilePath;
		this.datasetRequest.put("DoneFilePath", this.doneFilePath);
		return this;
	}
	
	public String getDoneFilePath() {
		if (this.doneFilePath == null) {
			try {
				throw new Exception("Done file path not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.doneFilePath;
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
				throw new Exception("Targets not specified.");
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
	
	
	public JSONArray getSourcePath() {
		if (this.sourcesPath == null) {
			try {
				throw new Exception("Source Path not specified");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.sourcesPath;
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

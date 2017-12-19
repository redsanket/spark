package hadooptest.gdm.regression.stackIntegration.starling;

public class StarlingExecutionResult {

    private String logType;
    private String logDate;
    private String mrJobURL;
    private boolean isNewLog;
    private String partitionExists;
    private String partitionValue;
    private String results;
    private String comments;
    
    public String getLogType() {
        return logType;
    }
    
    public void setLogType(String logType) {
        this.logType = logType;
    }
    
    public String getLogDate() {
        return logDate;
    }
    
    public void setLogDate(String logDate) {
        this.logDate = logDate;
    }
    
    public String getMrJobURL() {
        return mrJobURL;
    }
    
    public void setMrJobURL(String mrJobURL) {
        this.mrJobURL = mrJobURL;
    }
    
    public String getPartitionExists() {
        return partitionExists;
    }
    
    public void setPartitionExists(String partitionExists) {
        this.partitionExists = partitionExists;
    }
    
    public String getResults() {
        return results;
    }
    
    public void setResults(String results) {
        this.results = results;
    }
    
    public String getComments() {
        return comments;
    }
    
    public void setComments(String comments) {
        this.comments = comments;
    }

    public boolean isNewLog() {
        return isNewLog;
    }

    public void setNewLog(boolean isNewLog) {
        this.isNewLog = isNewLog;
    }

    public String getPartitionValue() {
        return partitionValue;
    }

    public void setPartitionValue(String partitionValue) {
        this.partitionValue = partitionValue;
    }

    @Override
    public String toString() {
	return "StarlingExecutionResult [logType=" + logType + ", logDate=" + logDate + ", mrJobURL=" + mrJobURL
		+ ", isNewLog=" + isNewLog + ", partitionExists=" + partitionExists + ", partitionValue="
		+ partitionValue + ", results=" + results + ", comments=" + comments + "]";
    }
}

package hadooptest.gdm.regression.integration.metrics;

import hadooptest.gdm.regression.integration.DataBaseOperations;

import java.sql.Connection;
import java.sql.SQLException;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class NameNodeDFSMemoryJob implements Job{
	private NameNodeDFSMemoryInfo nameNodeMemoryInfoObject;
	private DataBaseOperations dbOperations;
	private Connection dbCon;

	public NameNodeDFSMemoryJob() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.nameNodeMemoryInfoObject = new NameNodeDFSMemoryInfo();
		this.dbOperations = new DataBaseOperations();
		this.dbCon = this.dbOperations.getConnection();
	}

	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		this.nameNodeMemoryInfoObject.getNameNodeDFSMemoryInfo();
		try {
			this.dbOperations.insertNameNodeDFSMemoryInfoRecord(dbCon , this.nameNodeMemoryInfoObject.getNameNodeName(), this.nameNodeMemoryInfoObject.getHadoopVersion(), this.nameNodeMemoryInfoObject.getCurrentTimeStamp(), 
					this.nameNodeMemoryInfoObject.getTotalMemoryCapacity() , this.nameNodeMemoryInfoObject.getUsedMemoryCapacity() , this.nameNodeMemoryInfoObject.getRemainingMemoryCapacity(), this.nameNodeMemoryInfoObject.getMissingBlocks() );
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
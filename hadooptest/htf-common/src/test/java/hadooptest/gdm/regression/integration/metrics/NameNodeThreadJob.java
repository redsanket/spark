package hadooptest.gdm.regression.integration.metrics;

import java.sql.SQLException;
import java.sql.Connection;

import hadooptest.gdm.regression.integration.DataBaseOperations;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class NameNodeThreadJob implements Job{
	private NameNodeThreadInfo nameNodeThreadInfoObject;
	private DataBaseOperations dbOperations;
	private Connection dbCon;

	public NameNodeThreadJob() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		this.nameNodeThreadInfoObject = new NameNodeThreadInfo();
		this.dbOperations = new DataBaseOperations();
		this.dbCon = this.dbOperations.getConnection();
	}

	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		this.nameNodeThreadInfoObject.getNameNodeThreadInfo();
		try {
			this.dbOperations.insertNameNodeThreadInfoRecord(dbCon , this.nameNodeThreadInfoObject.getNameNodeName(), this.nameNodeThreadInfoObject.getHadoopVersion(), this.nameNodeThreadInfoObject.getCurrentTimeStamp(), this.nameNodeThreadInfoObject.getNewThread() , this.nameNodeThreadInfoObject.getRunnableThread() , this.nameNodeThreadInfoObject.getBlockedThread(), this.nameNodeThreadInfoObject.getWaitingThread() , this.nameNodeThreadInfoObject.getTimedWaitingThread() , this.nameNodeThreadInfoObject.getTerminatedThread() );
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

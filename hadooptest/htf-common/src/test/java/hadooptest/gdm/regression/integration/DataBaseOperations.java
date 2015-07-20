package hadooptest.gdm.regression.integration;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class DataBaseOperations {

	private final static String DRIVER = "com.mysql.jdbc.Driver";

	public DataBaseOperations() { }

	/**
	 * Create database if not exists.
	 * @param con
	 * @throws SQLException
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public void createDB() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		Class.forName(DRIVER).newInstance();
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" ,"root","");
		if (con != null ) {
			Statement stmt = con.createStatement();
			stmt.executeUpdate(DBCommands.CREATE_DB); 
			System.out.println("Database created successfully...");
			stmt.close();	
			con.close();
		} else {
			System.out.println("Failed to open the connection to database.");
		}
	}

	/**
	 * Get the connection object
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public Connection getConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName(DRIVER).newInstance();
		Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" + DBCommands.TABLE_NAME  ,"root","");
		//Connection con = DriverManager.getConnection("jdbc:mysql://dense34.blue.ygrid.yahoo.com/" + DBCommands.TABLE_NAME  ,"root","");
		if (con != null ) {
			return con;
		} else {
			System.out.println("Failed to open  the connection.");
			return con;
		}
	}

	/**
	 * Create the table, if not exists
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */
	public void createTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		Connection con = this.getConnection();
		if (con != null) {
			System.out.println("Connected to database con = " + con.toString());
			Statement stmt = con.createStatement();
			stmt.execute(DBCommands.CREATE_TABLE);
			System.out.println("Table created successfully...");
			stmt.close();	
			con.close();
		} else {
			System.out.println("Failed to connect database..!");
		}
	}
	
	public void createNameNodeThreadInfoTable() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.getConnection();
		if (con != null) {
			System.out.println("Connected to database con = " + con.toString());
			Statement stmt = con.createStatement();
			stmt.execute(DBCommands.CREATE_NAME_NODE_THREAD_INFO_TABLE);
			System.out.println("Table created successfully...");
			stmt.close();	
			con.close();
		} else {
			System.out.println("Failed to connect database..!");
		}
	}
	
	
	public void createNameNodeMemoryInfoTable() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.getConnection();
		if (con != null) {
			System.out.println("Connected to database con = " + con.toString());
			Statement stmt = con.createStatement();
			stmt.execute(DBCommands.CREATE_NAME_NODE_MEMORY_INFO_TABLE);
			System.out.println("Table created successfully...");
			stmt.close();	
			con.close();
		} else {
			System.out.println("Failed to connect database..!");
		}
	}
	

	/**
	 * Insert the record into the table
	 * @param dataSetName
	 * @param currentFrequency
	 * @param startTime
	 * @param endTime
	 * @param steps
	 * @param currentStep
	 * @param result
	 * @throws SQLException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 */	
	public void insertRecord(String dataSetName, String  currentFrequency, String jobStarted ,  String  startTime, String currentStep , String hadoopVersion , String pigVersion) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.getConnection();
		if (con != null) {
			PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_ROW);
			preparedStatement.setString(1, dataSetName);
			preparedStatement.setString(2, currentFrequency);
			preparedStatement.setString(3, jobStarted);
			preparedStatement.setString(4, startTime);
			preparedStatement.setString(5, currentStep);
			preparedStatement.setString(6, hadoopVersion);
			preparedStatement.setString(7, pigVersion);
			boolean isRecordInserted = preparedStatement.execute();
			assertTrue("Failed to insert record for " + dataSetName , isRecordInserted != true);

			con.close();
		} else {
			System.out.println("Failed to connect database.");
		}
	}
	
	/**
	 * Insert record into CREATE_NAME_NODE_THREAD_INFO_TABLE 
	 * @param con
	 * @param NameNode_Name
	 * @param HadoopVersion
	 * @param TimeStamp
	 * @param ThreadsNew
	 * @param ThreadsRunnable
	 * @param ThreadsBlocked
	 * @param ThreadsWaiting
	 * @param ThreadsTimedWaiting
	 * @param ThreadsTerminated
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void insertNameNodeThreadInfoRecord(Connection con, String NameNode_Name ,String  HadoopVersion ,String  TimeStamp , String ThreadsNew , String ThreadsRunnable , String ThreadsBlocked ,  String ThreadsWaiting , String ThreadsTimedWaiting , String ThreadsTerminated) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		if (con != null) {
			PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_NAME_NODE_THREAD_INFO_ROW);
			preparedStatement.setString(1, NameNode_Name);
			preparedStatement.setString(2, HadoopVersion);
			preparedStatement.setString(3, TimeStamp);
			preparedStatement.setString(4, ThreadsNew);
			preparedStatement.setString(5, ThreadsRunnable);
			preparedStatement.setString(6, ThreadsBlocked);
			preparedStatement.setString(7, ThreadsWaiting);
			preparedStatement.setString(8, ThreadsTimedWaiting);
			preparedStatement.setString(9, ThreadsTerminated);
			boolean isRecordInserted = preparedStatement.execute();
			assertTrue("Failed to insert record " + DBCommands.INSERT_NAME_NODE_THREAD_INFO_ROW  + "   for " + TimeStamp + "  time stamp." , isRecordInserted != true);
		} else {
			System.out.println("Failed to connect database.");
		}
	}
	
	/**
	 * Insert record into NAME_NODE_MEMORY_INFO_TABLE
	 * @param con
	 * @param nameNode_Name
	 * @param hadoopVersion
	 * @param timeStamp
	 * @param totalMemoryCapacity
	 * @param usedMemoryCapacity
	 * @param remainingMemoryCapacity
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void insertNameNodeDFSMemoryInfoRecord(Connection con, String nameNodeName ,String  hadoopVersion ,String  timeStamp , String totalMemoryCapacity , String usedMemoryCapacity , String remainingMemoryCapacity , String missingBlocks) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		if (con != null) {
			PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_NAME_NODE_DFS_MEMORY_ROW);
			preparedStatement.setString(1, nameNodeName);
			preparedStatement.setString(2, hadoopVersion);
			preparedStatement.setString(3, timeStamp);
			preparedStatement.setString(4, totalMemoryCapacity);
			preparedStatement.setString(5, usedMemoryCapacity);
			preparedStatement.setString(6, remainingMemoryCapacity);
			preparedStatement.setString(7, missingBlocks);
			boolean isRecordInserted = preparedStatement.execute();
			assertTrue("Failed to insert record " + DBCommands.NAME_NODE_DFS_MEMORY_INFO_TABLE  + "   for " + timeStamp + "  time stamp." , isRecordInserted != true);
		} else {
			System.out.println("Failed to connect database.");
		}
	}

	/**
	 * update the specified column value.
	 * @param columnName
	 * @param columnValue
	 * @param dataSetName
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public void updateRecord(Connection con , String... args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		System.out.println("******************************************************************************************************************");
		String dataSetName = args[args.length - 1];
		System.out.println("dataSetName = " + dataSetName);
		List<String> list = Arrays.asList(args);
		List<String> subList = list.subList(0, list.size() - 1);
		int i = 0;
		StringBuffer tempeStr= new StringBuffer();
		while(i < subList.size()) {
			String col = subList.get(i);
			i++;
			String val  = subList.get(i);
			i++;
			String temp = col + "=\""  + val + "\" ,";
			tempeStr.append(temp);
		}
		System.out.println("updateStr = " + tempeStr);

		// remove the extra , character 
		String temp = tempeStr.toString();
		String updateStr = temp.substring(0, temp.length() - 2 );
		String UPDATE_RECORD = "UPDATE " + DBCommands.TABLE_NAME + "  SET  " +  updateStr.toString() +  "  where dataSetName = \"" + dataSetName + "\"";
		System.out.println("UPDATE_RECORD  = " + UPDATE_RECORD);
		if (con != null) {
			Statement stmt = con.createStatement();
			stmt.execute(UPDATE_RECORD);
			System.out.println("Record updated successfully..!");
			stmt.close();
		}
	}

	public synchronized String getRecord(Connection con , String...  args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		String dataSetName = args[args.length - 1];
		List<String> list = Arrays.asList(args);
		List<String> subList = list.subList(0, list.size() - 1);

		// collect the column name
		List<String> columnNameList = new ArrayList<String>();
		StringBuilder columns = new StringBuilder();
		for ( String columnName : subList) {
			columnNameList.add(columnName);
			columns.append(columnName).append(" , ");
		}

		// remove the extra , character 
		String temp = columns.toString();
		String selectQureyStr = temp.substring(0, temp.length() - 2 );
		StringBuffer states = new StringBuffer();

		if (con != null) {
			String SELECT_QUERY = "SELECT " + selectQureyStr + "  from " + DBCommands.TABLE_NAME  + "   where dataSetName =  \"" + dataSetName + "\"";
			Statement stmt = con.createStatement();
			ResultSet resultSet = stmt.executeQuery(SELECT_QUERY);
			if (resultSet != null) {
				while (resultSet.next()) {
					int rowNo = resultSet.getRow();
					System.out.println("rowNo = " + rowNo);
					String value = resultSet.getString(columnNameList.get(0));
					states.append(value).append(",");
				}
			} else {
				System.out.println("Failed to execute " + SELECT_QUERY + " query.");
			}
		}
		return states.toString();
	}


}

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
	public void insertRecord(String dataSetName, String  currentFrequency, String  startTime, String  endTime, String  steps, String  currentStep, String result) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		Connection con = this.getConnection();
		if (con != null) {
			PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_ROW);
			preparedStatement.setString(1, dataSetName);
			preparedStatement.setString(2, currentFrequency);
			preparedStatement.setString(3, startTime);
			preparedStatement.setString(4, endTime);
			preparedStatement.setString(5, steps);
			preparedStatement.setString(6, currentStep);
			preparedStatement.setString(7, result);
			boolean isRecordInserted = preparedStatement.execute();
			assertTrue("Failed to insert record for " + dataSetName , isRecordInserted != true);

			con.close();
		} else {
			System.out.println("Failed to connect database.");
		}
	}
	
	
	public void insertRecord(String dataSetName, String  currentFrequency, String jobStarted ,  String  startTime, String currentStep) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.getConnection();
		if (con != null) {
			PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_ROW);
			preparedStatement.setString(1, dataSetName);
			preparedStatement.setString(2, currentFrequency);
			preparedStatement.setString(3, jobStarted);
			preparedStatement.setString(4, startTime);
			preparedStatement.setString(5, currentStep);
			boolean isRecordInserted = preparedStatement.execute();
			assertTrue("Failed to insert record for " + dataSetName , isRecordInserted != true);

			con.close();
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
	/*public void updateRecord(String columnName , String columnValue , String dataSetName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Connection con = this.getConnection();
		String UPDATE_RECORD = "UPDATE " + DBCommands.TABLE_NAME + "  SET " + columnName + " = \"" + columnValue  + "\" where dataSetName = \"" + dataSetName + "\"";
		if (con != null) {
			Statement stmt = con.createStatement();
			stmt.execute(UPDATE_RECORD);
			System.out.println("Record updated successfully..!");
			stmt.close();
			con.close();
		}
	}*/

	public void updateRecord(Connection con , String... args) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		System.out.println("******************************************************************************************************************");
		//Connection con = this.getConnection();
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
		//Connection con1  = this.getConnection();
		if (con != null) {
			Statement stmt = con.createStatement();
			stmt.execute(UPDATE_RECORD);
			System.out.println("Record updated successfully..!");
			stmt.close();
			//con.close();
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

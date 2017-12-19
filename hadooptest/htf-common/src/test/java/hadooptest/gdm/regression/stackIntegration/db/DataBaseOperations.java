// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.stackIntegration.db;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import java.io.*;
import java.util.*;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import hadooptest.TestSession;

public class DataBaseOperations {
    private final static String DRIVER = "com.mysql.jdbc.Driver";
    private final static String DB_USER_NAME = "hadoopqa"; 

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

        // gridci-1973, get local mysql DB passwd from keydb
        String passwd="";
        try {
                ProcessBuilder pb = new ProcessBuilder("/home/y/bin64/keydbgetkey", "mysqlroot");
                Process p = pb.start();
                BufferedReader stdout = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
                int exitStatus = p.waitFor();
                String line;
                while((line = stdout.readLine())!=null)
                {
                        passwd+=line;
                } 
                stdout.close();
                //TestSession.logger.info("Keydb fetch returns passwd: " + passwd);
                TestSession.logger.info("Keydb local mysql passwd fetch return value is: " + exitStatus);
        } catch (IOException ioe) {
                TestSession.logger.error("Mysql local DB passwd fetch IO exception, is the keydb mysqlroot file readable? Exception: " + ioe);
        } catch (IllegalThreadStateException itse) {
                TestSession.logger.error("Mysql local DB passwd fetch got illegal thread state ex: " + itse);
        } catch (InterruptedException ie) {
                TestSession.logger.error("Mysql local DB passwd fetch got interrupted ex: " + ie);
        }

        Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" ,"root", passwd);
        if (con != null ) {
            Statement stmt = con.createStatement();
            stmt.executeUpdate(DBCommands.CREATE_DB); 
            TestSession.logger.info("Database created successfully...");
            stmt.close();   
            con.close();
        } else {
            TestSession.logger.info("Failed to open the connection to database.");
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

        // gridci-1973, get local mysql DB passwd from keydb
        String passwd="";
        try {
                ProcessBuilder pb = new ProcessBuilder("/home/y/bin64/keydbgetkey", "mysqlroot");
                Process p = pb.start();
                BufferedReader stdout = new BufferedReader(new InputStreamReader(new BufferedInputStream(p.getInputStream())));
                int exitStatus = p.waitFor();
                String line;
                while((line = stdout.readLine())!=null)
                {
                        passwd+=line;
                } 
                stdout.close();
                //TestSession.logger.info("Keydb fetch returns passwd: " + passwd);
                TestSession.logger.info("Keydb local mysql passwd fetch return value is: " + exitStatus);
        } catch (IOException ioe) {
                TestSession.logger.error("Mysql local DB passwd fetch IO exception, is the keydb mysqlroot file readable? Exception: " + ioe);
        } catch (IllegalThreadStateException itse) {
                TestSession.logger.error("Mysql local DB passwd fetch got illegal thread state ex: " + itse);
        } catch (InterruptedException ie) {
                TestSession.logger.error("Mysql local DB passwd fetch got interrupted ex: " + ie);
        }

        Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" + DBCommands.DB_NAME ,"root", passwd);
        if (con != null ) {
            return con;
        } else {
            TestSession.logger.info("Failed to open  the connection.");
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
    public void createIntegrationResultTable() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        String tableName = DBCommands.CREATE_INTEGRATION_TABLE;
        tableName = tableName.replaceAll("TB_NAME", DBCommands.TABLE_NAME);
        createTable(tableName);
    }
    
    public void createTable(final String TABLE_NAME) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        Connection con = this.getConnection();
        if (con != null) {
            TestSession.logger.info("Connected to database con = " + con.toString());
            Statement stmt = con.createStatement();
            stmt.execute(TABLE_NAME);
            TestSession.logger.info("Table created successfully...");
            stmt.close();
            con.close();
        } else {
            TestSession.logger.info("Failed to connect database..!");
        }
    }
    
    public void insertDataSetName(String dataSetName , String currentDate ) {
        Connection con = null;

        /**
         * TODO : Add code to check whether already record exists.
         */

        try {
            con = this.getConnection();
            String INSERT_ROW = "INSERT INTO " + DBCommands.TABLE_NAME + " (dataSetName, date)  "  + "  values (?,?) ";
            PreparedStatement preparedStatement = con.prepareCall(INSERT_ROW);
            preparedStatement.setString(1, dataSetName);
            preparedStatement.setString(2, currentDate);
            boolean isRecordInserted = preparedStatement.execute();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            TestSession.logger.error("Failed to insert the current dataset name in to the database." + e);
            e.printStackTrace();
        }finally{
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    TestSession.logger.error("Failed to close the connection.");
                    e.printStackTrace();
                }
            }
        }
    }
    
    
    public boolean checkRecordAlreadyExists(String dataSetName, String currentDate) {
	Connection con = null;
	boolean flag = false;
	try {
	    con = this.getConnection();
	    String tableName = DBCommands.DB_NAME + "." + DBCommands.TABLE_NAME;
	    String selectQuery = "select dataSetName, date from " + tableName + "  where dataSetName = ? and date = ?";
	    TestSession.logger.info("checkRecordAlreadyExists - selectQuery - "  + selectQuery);
	    PreparedStatement pStmt = con.prepareStatement(selectQuery);
	    pStmt.setString(1, dataSetName);
	    pStmt.setString(2, currentDate);
	    ResultSet resultSet = pStmt.executeQuery();
	  
	    if ( resultSet != null ) {
		while ( resultSet.next() ) {
		    if ( resultSet.getString("dataSetName") != null  && resultSet.getString("date") != null) {
			if ( resultSet.getString("dataSetName").equalsIgnoreCase(dataSetName) && resultSet.getString("date").equalsIgnoreCase(currentDate)) {
			    flag = true;
			    break;
			}
		    }
		}
	    } else {
		TestSession.logger.error("Failed to execute " + selectQuery);
		throw new SQLException("Failed to execute " + selectQuery + "   -   dataSetName - " + dataSetName + "     date - " + currentDate );
	    }
	} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
	    TestSession.logger.error("Failed to check for record already exist in database." + e);
	    e.printStackTrace();
	}finally{
	    if (con != null) {
		try {
		    con.close();
		} catch (SQLException e) {
		    TestSession.logger.error("Failed to close the connection.");
		    e.printStackTrace();
		}
	    }
	}
	return flag;
    }

    public synchronized void insertComponentTestResult(String dataSetName , String columnName , String columnValue) {
        TestSession.logger.info("dataSetName  = " + dataSetName  + "   columnName  = " + columnName  + "   columnValue = " + columnValue);
        Connection con = null;
        try {
            con = this.getConnection();
            String  UPDATE_RECORD = "update " + DBCommands.TABLE_NAME + "  set " + columnName.trim() + "=\""  + columnValue + "\"" + "  where dataSetName=\"" + dataSetName + "\""    ;
            TestSession.logger.info("UPDATE_RECORD  = " + UPDATE_RECORD);
            if (con != null) {
                Statement stmt = con.createStatement();
                stmt.execute(UPDATE_RECORD);
                TestSession.logger.info("Record updated successfully..!");
                stmt.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            TestSession.logger.error("Failed to update " + columnName + "  result into DB." + e);
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    TestSession.logger.error("Failed to close db connection." + e);
                    e.printStackTrace();
                }
            }   
        }
    }

    // gridci-1667, update final table
    public synchronized void insertComponentTestResult(Boolean isFinalTable, String dataSetName , String columnName , String columnValue) {
        TestSession.logger.info("table name = FINAL_RESULT_TABLE_NAME, dataSetName  = " + dataSetName  + ", columnName  = " + columnName  + ", columnValue = " + columnValue);
        Connection con = null;
        try {
            con = this.getConnection();
            String  UPDATE_RECORD = "update " + DBCommands.FINAL_RESULT_TABLE_NAME + "  set " + columnName.trim() + "=\""  + columnValue + "\"" + "  where dataSetName=\"" + dataSetName + "\""    ;
            TestSession.logger.info("UPDATE_RECORD  = " + UPDATE_RECORD);
            if (con != null) {
                Statement stmt = con.createStatement();
                stmt.execute(UPDATE_RECORD);
                TestSession.logger.info("Record updated successfully..!");
                stmt.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            TestSession.logger.error("Failed to update " + columnName + "  result into DB." + e);
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    TestSession.logger.error("Failed to close db connection." + e);
                    e.printStackTrace();
                }
            }  
        }
    }

    public synchronized void insertComponentTestResult(String dataSetName , String testIteration, String columnName , String columnValue) {
        TestSession.logger.info("dataSetName  = " + dataSetName  + "   columnName  = " + columnName  + "   columnValue = " + columnValue);
        Connection con = null;
        try {
            con = this.getConnection();
            String  UPDATE_RECORD = "update " + DBCommands.TABLE_NAME + "  set " + columnName.trim() + "=\""  + columnValue + "\"" + "  where dataSetName=\"" + dataSetName + "\""  + "  and testIteration=\"" + testIteration +  "\""   ;
            TestSession.logger.info("UPDATE_RECORD  = " + UPDATE_RECORD);
            if (con != null) {
                Statement stmt = con.createStatement();
                stmt.execute(UPDATE_RECORD);
                TestSession.logger.info("Record updated successfully..!");
                stmt.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            TestSession.logger.error("Failed to update " + columnName + "  result into DB." + e);
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    TestSession.logger.error("Failed to close db connection." + e);
                    e.printStackTrace();
                }
            }   
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
        TestSession.logger.info("******************************************************************************************************************");
        String dataSetName = args[args.length - 1];
        TestSession.logger.info("dataSetName = " + dataSetName);
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
        TestSession.logger.info("updateStr = " + tempeStr);

        // remove the extra , character 
        String temp = tempeStr.toString();
        String updateStr = temp.substring(0, temp.length() - 2 );
        String UPDATE_RECORD = "UPDATE " + DBCommands.FINAL_RESULT_TABLE_NAME + "  SET  " +  updateStr.toString() +  "  where dataSetName = \"" + dataSetName + "\"";
        TestSession.logger.info("UPDATE_RECORD  = " + UPDATE_RECORD);
        if (con != null) {
            Statement stmt = con.createStatement();
            stmt.execute(UPDATE_RECORD);
            TestSession.logger.info("Record updated successfully..!");
            stmt.close();
        }
    }
    
    public void getTodayResult() {
        java.text.SimpleDateFormat simpleDateFormat = new java.text.SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        String currentDate = simpleDateFormat.format(calendar.getTime());
        String QUERY = "select * from integration where date like "  + "\""  + currentDate + "\"";
        Table<String, String, String> todayIntResultTable = HashBasedTable.create();
        try {
            java.sql.Connection connection = this.getConnection();
            Statement statement = connection.createStatement();
            if (statement != null) {
                ResultSet resultSet = statement.executeQuery(QUERY);
                if (resultSet != null) {
                    while (resultSet.next()) {
                        
                    }
                }   
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
            TestSession.logger.error("failed to get the connection - " +  e);
            e.printStackTrace();
        }
    }
    
    public List<String> getDataSetNames(String tableName , String date) {
	List<String> dataSetNames = new ArrayList<String>();
	String selectQuery = "select dataSetName from " + tableName + "  where date = ?";
	Connection con = null;
	try {
	    con = this.getConnection();
	    PreparedStatement pStmt = con.prepareStatement(selectQuery);
	    pStmt.setString(1, date);
	    ResultSet resultSet = pStmt.executeQuery();
	    if ( resultSet != null ) {
		while ( resultSet.next() ) {
		    dataSetNames.add(resultSet.getString("dataSetName"));
		}
	    } else {
		TestSession.logger.error("Failed to execute " + selectQuery);
		throw new SQLException("Failed to execute " + selectQuery );
	    }
	} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
	    TestSession.logger.error("Failed to check for record already exist in database." + e);
	    e.printStackTrace();
	}finally{
	    if (con != null) {
		try {
		    con.close();
		} catch (SQLException e) {
		    TestSession.logger.error("Failed to close the connection.");
		    e.printStackTrace();
		}
	    }
	}
	return dataSetNames;
    }

    /**
     * Update starling results to final table.
     * @param date date for which the results needs tobe inserted.
     */
    public void updateStarlingExecutionResult(String date) {
	String tableName = DBCommands.DB_NAME + "." + DBCommands.TABLE_NAME;
	String 	QUERY = "SELECT starlingVersion,starlingResult,starlingComments,starlingJSONResults from "  +  tableName + " where date=\"" + date + "\"";
	Connection con = null;

	class StarlingResult {
	    private String starlingVersion;
	    private String starlingResult;
	    private String starlingComments;
	    private String starlingJSONResults;

	    public String getStarlingVersion() {
		return starlingVersion;
	    }

	    public void setStarlingVersion(String starlingVersion) {
		this.starlingVersion = starlingVersion;
	    }

	    public String getStarlingResult() {
		return starlingResult;
	    }

	    public void setStarlingResult(String starlingResult) {
		this.starlingResult = starlingResult;
	    }

	    public String getStarlingComments() {
		return starlingComments;
	    }

	    public void setStarlingComments(String starlingComments) {
		this.starlingComments = starlingComments;
	    }

	    public String getStarlingJSONResults() {
		return starlingJSONResults;
	    }

	    public void setStarlingJSONResults(String starlingJSONResults) {
		this.starlingJSONResults = starlingJSONResults;
	    }
	}

	StarlingResult starlingResultObject = new StarlingResult();
	try {
	    con = this.getConnection();
	    TestSession.logger.info("QUERY = " + QUERY);
	    Statement stmt = con.createStatement();
	    ResultSet resultSet = stmt.executeQuery(QUERY);
	    boolean flag = false;
	    while ( resultSet != null ) {
		starlingResultObject.setStarlingVersion(resultSet.getString("starlingVersion"));
		starlingResultObject.setStarlingComments(resultSet.getString("starlingComments"));
		starlingResultObject.setStarlingJSONResults(resultSet.getString("starlingJSONResults"));
		String result = resultSet.getString("starlingResult");
		if (result.indexOf("fail") > -1) {
		    starlingResultObject.setStarlingResult(result);
		    flag = true;
		    break;
		}
	    }

	    //if ( flag ) {
	    // update the result;
	    String finalTable = DBCommands.DB_NAME + "." + DBCommands.FINAL_RESULT_TABLE_NAME;
	    String UPDATE_QUERY = "UPDATE " + finalTable   + "  set starlingVersion=" +  starlingResultObject.getStarlingVersion() +
		    "  starlingResult=" + starlingResultObject.getStarlingResult() +
		    "  starlingComments=" + starlingResultObject.getStarlingComments() +
		    "  starlingJSONResults=" + starlingResultObject.getStarlingJSONResults() +
		    "  where date=" + date;
	    if (stmt.execute(UPDATE_QUERY) ) {
		TestSession.logger.info("update starling successfully to final table");
	    } else {
		TestSession.logger.error("Fail to update starling result to final table");
	    }
	    /*} else {
		TestSession.logger.error("There is no record existing in " + DBCommands.DB_NAME + "." + DBCommands.FINAL_RESULT_TABLE_NAME  + "  table for date = " + date);
	    }*/
	} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
	    TestSession.logger.error("Failed to check for record already exist in database." + e);
	    e.printStackTrace();
	}finally{
	    if (con != null) {
		try {
		    con.close();
		} catch (SQLException e) {
		    TestSession.logger.error("Failed to close the connection.");
		    e.printStackTrace();
		}
	    }
	}
    }
}

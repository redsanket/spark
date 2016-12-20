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
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" ,"root","");
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
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" + DBCommands.DB_NAME ,"root","");
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
    public synchronized void insertComponentTestResult(Boolean finalTable, String dataSetName , String columnName , String columnValue) {
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
    
}

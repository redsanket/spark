// Copyright 2016, Yahoo Inc.
package hadooptest.gdm.regression.integration;

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
import java.util.Date;
import java.util.List;

import hadooptest.TestSession;


public class DataBaseOperations {

    private final static String DRIVER = "com.mysql.jdbc.Driver";
    private final static String DB_USER_NAME = "hadoopqa";
    private final static String DB_PASS_WORD = "*38480E511C26DD7BDEEB8FFB196B9D82A11C212E";
    private final static String DB_HOST_NAME = "dev-corp-rw.yds.corp.yahoodns.net"; 
    private final static String DB_NAME = "hadoop_stack_integration";

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

        Connection con = DriverManager.getConnection("jdbc:mysql://localhost/" + DBCommands.DB_NAME  ,"root", passwd);
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
        createTable(DBCommands.CREATE_TABLE);
    }
    
    public void createNameNodeThreadInfoTable() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        createTable(DBCommands.CREATE_NAME_NODE_THREAD_INFO_TABLE);
    }
    
    public void createNameNodeMemoryInfoTable() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        createTable(DBCommands.CREATE_NAME_NODE_MEMORY_INFO_TABLE);
    }
    
    public void createHealthCheckupTable() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        createTable(DBCommands.CREATE_HEALTH_CHECKUP_TABLE);
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
    public void insertRecord(String dataSetName, String testType, String  currentFrequency, String jobStarted ,  String  startTime, String currentStep , String hadoopVersion , String pigVersion ,
            String oozieVersion , String hbaseVersion , String tezVersion , String hiveVersion , String hcatVersion, String gdmVersion) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
        Connection con = this.getConnection();
        if (con != null) {
            PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_ROW);
            preparedStatement.setString(1, dataSetName);
            preparedStatement.setString(2, testType);
            preparedStatement.setString(3, currentFrequency);
            preparedStatement.setString(4, jobStarted);
            preparedStatement.setString(5, startTime);
            preparedStatement.setString(6, currentStep);
            preparedStatement.setString(7, hadoopVersion);
            preparedStatement.setString(8, pigVersion);
            preparedStatement.setString(9, oozieVersion);
            preparedStatement.setString(10, hbaseVersion);
            preparedStatement.setString(11, tezVersion);
            preparedStatement.setString(12, hiveVersion);
            preparedStatement.setString(13, hcatVersion);
            preparedStatement.setString(14, gdmVersion);
            boolean isRecordInserted = preparedStatement.execute();
            assertTrue("Failed to insert record for " + dataSetName , isRecordInserted != true);

            con.close();
        } else {
            TestSession.logger.info("Failed to connect database.");
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
            TestSession.logger.info("Failed to connect database.");
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
            TestSession.logger.info("Failed to connect database.");
        }
    }
    
    
    /**
     * Insert the record into health checkup table
     * @param con db connection
     * @param date  current date
     * @param clusterState cluster name
     * @throws SQLException
     */
    public void insertHealthCheckInfoRecord(Connection con , String date , String clusterState , String oozieState , String pigState, String hbaseState , String tezState , String hiveState , String hcatState , String gdmState) throws SQLException {
        if (con != null) {
            PreparedStatement preparedStatement = con.prepareCall(DBCommands.INSERT_HEALTH_CHECKUP_INFO_ROW);
            preparedStatement.setString(1, date);
            preparedStatement.setString(2, clusterState);
            preparedStatement.setString(3, oozieState);
            preparedStatement.setString(4, pigState);
            preparedStatement.setString(5, hbaseState);
            preparedStatement.setString(6, tezState);
            preparedStatement.setString(7, hiveState);
            preparedStatement.setString(8, hcatState);
            preparedStatement.setString(9, gdmState);
            boolean isRecoredInserted = preparedStatement.execute();
            TestSession.logger.info("isRecoredInserted  = " + isRecoredInserted);
            assertTrue("Failed to insert record "  + DBCommands.INSERT_HEALTH_CHECKUP_INFO_ROW  , isRecoredInserted != true);
        }
    }
    
    /**
     * Check whether current day record exists in health checkup table.
     * @return
     * @throws SQLException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws ClassNotFoundException
     */
    public int isHealthCheckupRecordExits() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        int recordCount = -1;
        Connection con = this.getConnection();
        assertTrue("Failed to open database connection in isHealthCheckupRecordExits . "  , con != null );
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
        Date date = new Date();
        String dt = dateFormat.format(date);
        final String SELECT_QUERY= "SELECT * FROM " + DBCommands.HEALTH_CHECKUP_UP_TABLE + " where date=\"" + dt + "\"";
        TestSession.logger.info("SELECT_QUERY = " + SELECT_QUERY);
        Statement stmt = con.createStatement();
        ResultSet resultSet = stmt.executeQuery(SELECT_QUERY);
        if ( resultSet != null ) {
            recordCount = resultSet.getRow();
            TestSession.logger.info("Row count in " + DBCommands.HEALTH_CHECKUP_UP_TABLE  + "   = " + recordCount);
        }
        return recordCount;
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
        String UPDATE_RECORD = "UPDATE " + DBCommands.TABLE_NAME + "  SET  " +  updateStr.toString() +  "  where dataSetName = \"" + dataSetName + "\"";
        TestSession.logger.info("UPDATE_RECORD  = " + UPDATE_RECORD);
        if (con != null) {
            Statement stmt = con.createStatement();
            stmt.execute(UPDATE_RECORD);
            TestSession.logger.info("Record updated successfully..!");
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
                    TestSession.logger.info("rowNo = " + rowNo);
                    String value = resultSet.getString(columnNameList.get(0));
                    states.append(value).append(",");
                }
            } else {
                TestSession.logger.info("Failed to execute " + SELECT_QUERY + " query.");
            }
        }
        return states.toString();
    }
}

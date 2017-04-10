<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat,java.io.BufferedReader,java.io.InputStreamReader"%>
<%
	SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
	Calendar currentTimeStampCal = Calendar.getInstance();
	String currentHrFrequency = "Integration_Testing_DS_" + feed_sdf.format(currentTimeStampCal.getTime()) + "00";
%>
<%
	Connection con = null;
	ResultSet resultSet = null;
	try {
		String password = "";
		String[] command = { "/home/y/bin64/keydbgetkey", "mysqlroot" };
		Process process = Runtime.getRuntime().exec(command);
		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String pswd;
		while ((pswd = reader.readLine()) != null) {
		    password = password.concat(pswd);
		}
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		con = DriverManager.getConnection("jdbc:mysql://localhost/stackIntegrationTestDB", "root", password);
%>
<%
	} catch (Exception e) {
%>
		<%@ include file="JDBCErrorPage.jsp"%>
<%
	}
%>

<%
	if (con != null) {
%>

<%
			String currentDataSet = request.getParameter("currentDataSetName");
%>

		<%
			String SELECT_QUERY = "SELECT * from integration_test where dataSetName=\"" + currentDataSet.trim() + "\"";
			try {
		%>
				<%
					Statement stmt = con.createStatement();
					resultSet = stmt.executeQuery(SELECT_QUERY);
					int exectionResultCount = resultSet.getRow();
				%>
				<%= exectionResultCount %>
		<%
			}catch (Exception e) {
		%>
			 <center>
			  	 	 	<h1> Failed :  <%= e.getMessage() %></h1>
	  	 	 </center>
		<%
			}
			if (resultSet != null)  {
		%>		
			<div class="container">
				<div class="panel panel-default">
					<div class="panel-heading">
						<h1 class="panel-title">HBase TestCase & Result</h1>
					</div>
					<div style="cursor: pointer" class="panel-body">
					<div id="minHadoopContainer">
						<table class="table">
							<thead>
								<tr>
									<th class="text-center">Slno</th>
									<th class="text-center">TestCase</th>
									<th class="text-center">Status</th>
									<th class="text-center">MR Job Link</th>
									<th class="text-center">Comment</th>
								</tr>
							</thead>
							<tbody>
		
				<%
					while (resultSet.next()) {
				%>
				
						<%
							String hbaseCreateTable = resultSet.getString("hbaseCreateTable");
							String hbaseCreateTableCurrentState = resultSet.getString("hbaseCreateTableCurrentState");
							String hbaseCreateTableComment = resultSet.getString("hbaseCreateTableComment");
							String hbaseInsertTableCurrentState = resultSet.getString("hbaseInsertTableCurrentState");
							String hbaseInsertRecordTable = resultSet.getString("hbaseInsertRecordTable");
							String hbaseInsertRecordTableComment = resultSet.getString("hbaseInsertRecordTableComment");
							String hbaseScanRecordTableCurrentState = resultSet.getString("hbaseScanRecordTableCurrentState");
							String hbaseScanRecordTable = resultSet.getString("hbaseScanRecordTable");
							String hbaseScanRecordTableComment = resultSet.getString("hbaseScanRecordTableComment");
							
							String hbaseDeleteTableCurrentState = resultSet.getString("hbaseDeleteTableCurrentState");
							String hbaseDeleteTable = resultSet.getString("hbaseDeleteTable");
							String hbaseDeleteTableComment = resultSet.getString("hbaseDeleteTableComment");
							
							
						%>
				<!--  hive drop table testcases -->
								<tr>
									<td class="text-center">
										1.
									</td>
									<td class="text-center">
										Create table
									</td>
									<td class="text-center">
										<%
											if (hbaseCreateTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hbaseCreateTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hbaseCreateTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hbaseCreateTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hbaseCreateTableCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
									<td class="text-center">
										<%
											if (hbaseCreateTableComment != null) {
										%>
											<a><%= hbaseCreateTableComment %></a>	
										<%
											}  
										%>
									</td>
								</tr>
								
								
							<!--  hive create table testcases -->
								<tr>
									<td class="text-center">
										2.
									</td>
									<td class="text-center">
										Insert Record into table
									</td>
									<td class="text-center">
										<%
											if (hbaseInsertTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hbaseInsertTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hbaseInsertRecordTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hbaseInsertRecordTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hbaseInsertTableCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
									<td class="text-center">
										<%
											if (hbaseInsertRecordTableComment != null) {
										%>
											<a><%= hbaseInsertRecordTableComment %></a>	
										<%
											}  
										%>
									</td>

								</tr>
								
								
							<!--  Load data to hive location -->
								<tr>
									<td class="text-center">
										3.
									</td>
									<td class="text-center">
											Scan Table
									</td>
									<td class="text-center">
										<%
											if (hbaseScanRecordTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hbaseScanRecordTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hbaseScanRecordTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hbaseScanRecordTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hbaseScanRecordTableCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
									<td class="text-center">
										<%
											if (hbaseScanRecordTableComment != null) {
										%>
											<a><%= hbaseScanRecordTableComment %></a>	
										<%
											}  
										%>
									</td>
								</tr>
								
								
								<!--  Load data into table testcases -->
								<tr>
									<td class="text-center">
										4.
									</td>
									<td class="text-center">
										Delete table
									</td>
									<td class="text-center">
										<%
											if (hbaseDeleteTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hbaseDeleteTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hbaseDeleteTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hbaseDeleteTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hbaseDeleteTableCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									
									<td class="text-center">
										
									</td>
									<td class="text-center">
										<%
											if (hbaseDeleteTableComment != null) {
										%>
											<a><%= hbaseDeleteTableComment %></a>	
										<%
											}  
										%>
										
									</td>
								</tr>
								
								
				<%
					}// end while
				%>
							</tbody>
						</table>
					</div>
				</div>
			</div>
		</div>
		<%
			} // endresultSet !=null 
		%>
<%
	} // end con != null
%>
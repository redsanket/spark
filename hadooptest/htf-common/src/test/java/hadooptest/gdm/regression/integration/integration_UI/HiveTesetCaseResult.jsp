<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat"%>
<%
	SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
	Calendar currentTimeStampCal = Calendar.getInstance();
	String currentHrFrequency = "Integration_Testing_DS_" + feed_sdf.format(currentTimeStampCal.getTime()) + "00";
%>
<%
	Connection con = null;
	ResultSet resultSet = null;
	try {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		con = DriverManager.getConnection("jdbc:mysql://localhost/stackIntegrationTestDB", "root", "");
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
						<h1 class="panel-title">Hive TestCase & Result</h1>
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
							String hiveDropTableResult = resultSet.getString("hiveDropTable");
							String hiveDropTableCurrentState = resultSet.getString("hiveDropTableCurrentState");
							String hiveDropTableComment = resultSet.getString("hiveDropTableComment");
							
							String hiveCreateTableCurrentState = resultSet.getString("hiveCreateTableCurrentState");
							String hiveCreateTable = resultSet.getString("hiveCreateTable");
							String hiveCreateTableComment = resultSet.getString("hiveCreateTableComment");
							
							String hiveCopyDataToHiveCurrentState = resultSet.getString("hiveCopyDataToHiveCurrentState");
							String hiveCopyDataToHive = resultSet.getString("hiveCopyDataToHive");
							String hiveCopyDataToHiveComment = resultSet.getString("hiveCopyDataToHiveComment");
							
							String hiveLoadDataToTableCurrentState = resultSet.getString("hiveLoadDataToTableCurrentState");
							String hiveLoadDataToTable = resultSet.getString("hiveLoadDataToTable");
							String hiveCopyDataToHiveMRJobURL = resultSet.getString("hiveCopyDataToHiveMRJobURL");
							String hiveLoadDataToTableComment = resultSet.getString("hiveLoadDataToTableComment");
							
							
						%>
				<!--  hive drop table testcases -->
								<tr>
									<td class="text-center">
										1.
									</td>
									<td class="text-center">
										Drop table
									</td>
									<td class="text-center">
										<%
											if (hiveDropTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hiveDropTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hiveDropTableResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hiveDropTableResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hiveDropTableCurrentState.equals("UNKNOWN")) {
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
											if (hiveCreateTableComment != null) {
										%>
											<a><%= hiveCreateTableComment %></a>	
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
										Create table
									</td>
									<td class="text-center">
										<%
											if (hiveCreateTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hiveCreateTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hiveCreateTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hiveCreateTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hiveCreateTableCurrentState.equals("UNKNOWN")) {
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
											if (hiveDropTableComment != null) {
										%>
											<a><%= hiveDropTableComment %></a>	
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
											Copy data to Hive location
									</td>
									<td class="text-center">
										<%
											if (hiveCopyDataToHiveCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hiveCopyDataToHiveCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hiveCopyDataToHive.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hiveCopyDataToHive.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hiveCopyDataToHiveCurrentState.equals("UNKNOWN")) {
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
											if (hiveCopyDataToHiveComment != null) {
										%>
											<a><%= hiveCopyDataToHiveComment %></a>	
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
										Load data into Hive table
									</td>
									<td class="text-center">
										<%
											if (hiveLoadDataToTableCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hiveLoadDataToTableCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hiveLoadDataToTable.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hiveLoadDataToTable.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hiveLoadDataToTableCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									
									<td class="text-center">
											<%
											if (hiveCopyDataToHiveMRJobURL != null) {
										%>
											 <a href="<%= hiveCopyDataToHiveMRJobURL %>" target="_blank">MR Job</a> 	
										<%
											}  
										%>
									</td>
									<td class="text-center">
										<%
											if (hiveLoadDataToTableComment != null) {
										%>
											<a><%= hiveLoadDataToTableComment %></a>	
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
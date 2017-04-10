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
						<h1 class="panel-title">Oozie TestCase & Result</h1>
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
							String cleanup_outputResult = resultSet.getString("cleanup_outputResult");
							String cleanup_outputCurrentState = resultSet.getString("cleanup_outputCurrentState");
							String cleanup_outputMRJobURL = resultSet.getString("cleanup_outputMRJobURL");
							String cleanup_outputComments = resultSet.getString("cleanup_outputComments");
							
							String check_inputResult = resultSet.getString("check_inputResult");
							String check_inputCurrentState = resultSet.getString("check_inputCurrentState");
							String check_inputMRJobURL = resultSet.getString("check_inputMRJobURL");
							String check_inputComments = resultSet.getString("check_inputComments");
							
							String pig_abf_input_PageValidNewsResult = resultSet.getString("pig_abf_input_PageValidNewsResult");
							String pig_abf_input_PageValidNewsCurrentState = resultSet.getString("pig_abf_input_PageValidNewsCurrentState");
							String pig_abf_input_PageValidNewsMRJobURL = resultSet.getString("pig_abf_input_PageValidNewsMRJobURL");
							String pig_abf_input_PageValidNewsComments = resultSet.getString("pig_abf_input_PageValidNewsComments");
							
							String hive_storageResult = resultSet.getString("hive_storageResult");
							String hive_storageCurrentState = resultSet.getString("hive_storageCurrentState");
							String hive_storageMRJobURL = resultSet.getString("hive_storageMRJobURL");
							String hive_storageComments = resultSet.getString("hive_storageComments");
							
							String hive_verifyResult = resultSet.getString("hive_verifyResult");
							String hive_verifyCurrentState = resultSet.getString("hive_verifyCurrentState");
							String hive_verifyMRJobURL = resultSet.getString("hive_verifyMRJobURL");
							String hive_verifyComments = resultSet.getString("hive_verifyComments");
						%>
				<!--  hive drop table testcases -->
								<tr>
									<td class="text-center">
										1.
									</td>
									<td class="text-center">
										Cleanup the out folder on HDFS ( rm -rf <path>)
									</td>
									<td class="text-center">
										<%
											if (cleanup_outputCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="cleanup_output testcase is running" />
										<%
											} else if (cleanup_outputCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (cleanup_outputResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="cleanup_output test failed" />
			 								<%
			 									} else if (cleanup_outputResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="cleanup_output test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (cleanup_outputCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="cleanup_output testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
									<%
										if ( (cleanup_outputMRJobURL != null) && !(cleanup_outputMRJobURL.indexOf("UNKNOWN") > -1))  {
									%>	
											 <a href="<%= cleanup_outputMRJobURL %>" target="_blank">MR Job</a> 
									<%
										}
									%>
										
									</td>
									<td class="text-center">
										<%
											if (cleanup_outputComments != null) {
										%>
											<a><%= cleanup_outputComments %></a>	
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
										Check whether instance files for the current hour exists on HDFS
									</td>
									<td class="text-center">
										<%
											if (check_inputCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="check_input testcase is running" />
										<%
											} else if (check_inputCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (check_inputResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="check_input test failed" />
			 								<%
			 									} else if (check_inputResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="check_input test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (check_inputCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="check_input testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										<%
											if ( (check_inputMRJobURL != null) && !(check_inputMRJobURL.indexOf("UNKNOWN") > -1) ) {
										%>	
												 <a href="<%= check_inputMRJobURL %>" target="_blank">MR Job</a> 
										<%
											}
										%>
									</td>
									<td class="text-center">
										<%
											if (check_inputComments != null) {
										%>
											<a><%= check_inputComments %></a>	
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
											Execute Pig script to load ,filter and store the data 
									</td>
									<td class="text-center">
										<%
											if (pig_abf_input_PageValidNewsCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="pig_abf_input_PageValidNews testcase is running" />
										<%
											} else if (pig_abf_input_PageValidNewsCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (pig_abf_input_PageValidNewsResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="pig_abf_input_PageValidNews test failed" />
			 								<%
			 									} else if (pig_abf_input_PageValidNewsResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="pig_abf_input_PageValidNews test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (pig_abf_input_PageValidNewsCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="pig_abf_input_PageValidNews testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										<%
											if ( (pig_abf_input_PageValidNewsMRJobURL != null)  && !(pig_abf_input_PageValidNewsMRJobURL.indexOf("UNKNOWN") > -1) ) {
										%>	
												 <a href="<%= pig_abf_input_PageValidNewsMRJobURL %>" target="_blank">MR Job</a> 
										<%
											}
										%>
									</td>
									<td class="text-center">
										<%
											if (pig_abf_input_PageValidNewsComments != null) {
										%>
											<a><%= pig_abf_input_PageValidNewsComments %></a>	
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
										Create database and external table in hive and Load data into Hive table
									</td>
									<td class="text-center">
										<%
											if (hive_storageCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hive_storage testcase is running" />
										<%
											} else if (hive_storageCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (pig_abf_input_PageValidNewsResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hive_storage test failed" />
			 								<%
			 									} else if (pig_abf_input_PageValidNewsResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hive_storage test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hive_storageCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hive_storage testcase has not yet started" />
										<%
											}
										%>
									</td>
									
									<td class="text-center">
										<%
											if ( (hive_storageMRJobURL != null)  && !(hive_storageMRJobURL.indexOf("UNKNOWN") > -1) ) {
										%>
											 <a href="<%= hive_storageMRJobURL %>" target="_blank">MR Job</a> 	
										<%
											}  
										%>
									</td>
									<td class="text-center">
										<%
											if (hive_storageComments != null) {
										%>
											<a><%= hive_storageComments %></a>	
										<%
											}  
										%>
										
									</td>
								</tr>
								 
								<tr>
									<td class="text-center">
										5.
									</td>
									<td class="text-center">
										Verify whether database, table exists and data is loaded into the table 
									</td>
									<td class="text-center">
										<%
											if (hive_verifyCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hive_verify testcase is running" />
										<%
											} else if (hive_verifyCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hive_verifyResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hive_verify test failed" />
			 								<%
			 									} else if (hive_verifyResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hive_verify test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hive_verifyCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hive_verify testcase has not yet started" />
										<%
											}
										%>
									</td>
									
									<td class="text-center">
											<%
											if( (hive_verifyMRJobURL != null) && !(hive_verifyMRJobURL.indexOf("UNKNOWN") > -1) ) {
										%>
											 <a href="<%= hive_verifyMRJobURL %>" target="_blank">MR Job</a> 	
										<%
											}  
										%>
									</td>
									<td class="text-center">
										<%
											if (hive_verifyComments != null) {
										%>
											<a><%= hive_verifyComments %></a>	
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
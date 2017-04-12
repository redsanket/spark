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
			String SELECT_QUERY = "SELECT * from integration_test where dataSetName=\"" + currentDataSet + "\"";
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
						<h1 class="panel-title">Tez TestCase & Result</h1>
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
							String tezVersion = resultSet.getString("tezVersion");
							String tezCurrentState = resultSet.getString("tezCurrentState");
							String tezResult = resultSet.getString("tezResult");
							String tezMRJobURL = resultSet.getString("tezMRJobURL");
							String tezComments = resultSet.getString("tezComments");
						%>
				<!--  Hadoop -->
								<tr>
									<td class="text-center">
										1.
									</td>
									<td class="text-center">
										Verify whether tez is able to load , transform and store the data
									</td>
									<td class="text-center">
										<%
											if (tezCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (tezCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (tezResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (tezResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (tezCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										<%
											if (tezMRJobURL != null) {
										%>
											 <a href="<%= tezMRJobURL %>" target="_blank">MR Job</a> 
										<%
											} else {
										%>
												<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
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
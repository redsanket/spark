<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">PipeLine-2</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li><a href="RunningJobStatus.jsp">Running Job</a></li>
        <li  class="active"><a href="CurrentHRResult.jsp">New Running Job</a></li>
        <li><a href="./NewCompletedJob.jsp">Completed Job</a></li>
         <li><a href="Release.jsp">Release</a></li>
          <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Build History<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="HealthCheckup.jsp">Current Day HealthCheckUp</a></li>
          	<li><a href="HealthCheckUpHistory.jsp">HealthCheckup history</a></li>
          </ul>
        </li>
      </ul>
      <ul class="nav navbar-nav navbar-right">

        <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Pipe lines<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="http://openqe48blue-n9.blue.ygrid.yahoo.com:8080/RunningJobStatus.jsp">Hadoop Pipeline</a></li>
            <li><a href="http://openqe53blue-n9.blue.ygrid.yahoo.com:8080/RunningJobStatus.jsp">Pineline-2</a></li>
          </ul>
        </li>

      </ul>
    </div>
    </div>
  </nav>
<%@ include file="header.jsp" %>
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
			String SELECT_QUERY = "SELECT * from integration_test where dataSetName=\"" + currentHrFrequency.trim() + "\"";
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
		<%
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy:MM:dd-HH");
			String currentHR = simpleDateFormat.format(currentTimeStampCal.getTime());
		%>
		<center><h3>Test executing for : <%= currentHR + "00" %> hour </h3> </center>
			<div class="container">
				<div class="panel panel-default">
					<div class="panel-heading">
						<h1 class="panel-title">pipeline</h1>
					</div>
					<div style="cursor: pointer" class="panel-body">
					<div id="minHadoopContainer" style="width: 50%;">
						<table class="table">
							<thead>
								<tr>
									<th class="text-center">Stack Component</th>
									<th class="text-center">Version</th>
									<th class="text-center">Status</th>
									<th class="text-center">Comment</th>
								</tr>
							</thead>
							<tbody>
		
				<%
					while (resultSet.next()) {
				%>
				
						<%
							String dataSetName = resultSet.getString("dataSetName");
							String hadoopVersion = resultSet.getString("hadoopVersion");
							String hadoopCurrentState = resultSet.getString("hadoopCurrentState");
							String hadoopResult = resultSet.getString("hadoopResult");
							String gdmVersion = resultSet.getString("gdmVersion");
							String gdmCurrentState = resultSet.getString("gdmCurrentState");
							String gdmResult = resultSet.getString("gdmResult");
							String tezVersion = resultSet.getString("tezVersion");
							String tezCurrentState = resultSet.getString("tezCurrentState");
							String tezResult = resultSet.getString("tezResult");
							String hiveVersion = resultSet.getString("hiveVersion");
							String hiveCurrentState = resultSet.getString("hiveCurrentState");
							String hiveResult = resultSet.getString("hiveResult");
							String hcatVersion = resultSet.getString("hcatVersion");
							String hcatCurrentState = resultSet.getString("hcatCurrentState");
							String hcatResult = resultSet.getString("hcatResult");
							String hbaseVersion = resultSet.getString("hbaseVersion");
							String hbaseCurrentState = resultSet.getString("hbaseCurrentState");
							String hbaseResult = resultSet.getString("hbaseResult");
						%>
				<!--  Hadoop -->
								<tr>
									<td class="text-center">
										hadoop
									</td>
									<td class="text-center">
										<%= hadoopVersion %>
									</td>
									<td class="text-center">
										<%
											if (hadoopCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hadoop testcase is running" />
										<%
											} else if (hadoopCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hadoopResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hadoopResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hadoop test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hadoopCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hadoop testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								
					<!--  gdm  -->
				
				
						<tr>
									<td class="text-center">
										GDM
									</td>
									<td class="text-center">
										<%= gdmVersion %>
									</td>
									<td class="text-center">
										<%
											if (gdmCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="gdm testcase is running" />
										<%
											} else if (gdmCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (gdmResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="gdm test failed" />
			 								<%
			 									} else if (gdmResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="gdm test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (gdmCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="gdm testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
					
				
				
				<!-- tez -->
									<tr>
									<td class="text-center">
										tez
									</td>
									<td class="text-center">
										<%= tezVersion %>
									</td>
									<td class="text-center">
										<%
											if (tezCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="tez testcase is running" />
										<%
											} else if (tezCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (tezResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="tez test failed" />
			 								<%
			 									} else if (tezResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="tez test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (tezCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="tez testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								
								<!-- hive -->
								
								<tr>
									<td class="text-center">
										hive
									</td>
									<td class="text-center">
										<%= hiveVersion %>
									</td>
									<td class="text-center">
										<%
											if (hiveCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hive testcase is running" />
										<%
											} else if (hiveCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hiveResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hive test failed" />
			 								<%
			 									} else if (hiveResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hive test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hiveCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hive testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								
								
								<!--  hcat -->
								<tr>
									<td class="text-center">
										hcatalog
									</td>
									<td class="text-center">
										<%= hcatVersion %>
									</td>
									<td class="text-center">
										<%
											if (hcatCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hcat testcase is running" />
										<%
											} else if (hcatCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hcatResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hcat test failed" />
			 								<%
			 									} else if (hcatResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hcat test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hcatCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hcat testcase has not yet started" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								
								<!--  hbase -->
									<tr>
									<td class="text-center">
										HBase
									</td>
									<td class="text-center">
										<%= hbaseVersion %>
									</td>
									<td class="text-center">
										<%
											if (hbaseCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="hbase testcase is running" />
										<%
											} else if (hbaseCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (hbaseResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hbase test failed" />
			 								<%
			 									} else if (hbaseResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="hbase test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (hbaseCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hbase testcase has not yet started" />
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
					<b></b>
					 <a href="DetailedTestCaseExecutionResult.jsp" target="_blank">Detailed View</a> 
				</div>
			</div>
		</div>
		<%
			} // endresultSet !=null 
		%>
<%
	} // end con != null
%>

</body>
<html>
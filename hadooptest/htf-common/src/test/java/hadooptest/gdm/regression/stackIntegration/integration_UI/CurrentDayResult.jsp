<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">PipeLine-2</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li><a href="RunningJobStatus.jsp">Running Job</a></li>
        <li  class="active"><a href="CurrentDayResult.jsp">New Running Job</a></li>
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
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat,java.io.BufferedReader,java.io.InputStreamReader"%>
<%
	SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMdd");
	Calendar currentTimeStampCal = Calendar.getInstance();
	String currentDayFrequency = feed_sdf.format(currentTimeStampCal.getTime());
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
			String SELECT_QUERY = "select * from integration_test  where date like  "  + "\"" + currentDayFrequency +  "\"";
			try {
		%>
		
	<%-- <%=  SELECT_QUERY %> --%>
				<%
					Statement stmt = con.createStatement();
					resultSet = stmt.executeQuery(SELECT_QUERY);
					int exectionResultCount = resultSet.getRow();
				%>
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
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
			String currentHR = simpleDateFormat.format(currentTimeStampCal.getTime());
		%>
		<center><h3>Stack Component test execution result for  : <%= currentHR  %> </h3> </center>
			<div class="container">
				<div class="panel panel-default">
					<div class="panel-heading">
						<h1 class="panel-title">Execution Result </h1>
					</div>
					<div style="cursor: pointer" class="panel-body">
					<div id="minHadoopContainer" style="width: 50%;">
						<table class="table">
							<thead>
								<tr>
									<th class="text-center">Dataset Iteration</th>
									<th class="text-center">Hadoop</th>
									<th class="text-center">Gdm</th>
									<th class="text-center">Tez</th>
									 <th class="text-center">Hive</th>
									<th class="text-center">HCatalog</th>
									<th class="text-center">HBase</th> 
									<!-- <th class="text-center">Oozie</th> -->
									<th class="text-center">Comment</th>
									
								</tr>
							</thead>
							<tbody>
		
				<%
					while (resultSet.next()) {
				%>
				
					<%
						String dataSetName = resultSet.getString("dataSetName");
					
						String hadoopCurrentState = resultSet.getString("hadoopCurrentState");
						String hadoopResult = resultSet.getString("hadoopResult");
						
					 	String gdmCurrentState = resultSet.getString("gdmCurrentState");
						String gdmResult = resultSet.getString("gdmResult");
						
						String tezCurrentState = resultSet.getString("tezCurrentState");
						String tezResult = resultSet.getString("tezResult");
						
						String hiveCurrentState = resultSet.getString("hiveCurrentState");
						String hiveResult = resultSet.getString("hiveResult");
						
						String hcatCurrentState = resultSet.getString("hcatCurrentState");
						String hcatResult = resultSet.getString("hcatResult");
						
						String hbaseCurrentState = resultSet.getString("hbaseCurrentState");
						String hbaseResult = resultSet.getString("hbaseResult"); 
					%>
					<tr>
					<td class="text-center">
						 <a href="DetailedTestCaseExecutionResult.jsp?currentDataSetName=<%= dataSetName %>" target="_blank"><%= dataSetName.trim() %></a> 	
					</td>
					<!-- hadoop -->
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
						
						<!-- gdm -->
						
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
						
						<!--  tez -->
						
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
						
				 	<!--  hive -->
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
						
						
			 				<!--  hcat -->
						
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
						
						<!-- hbase -->
						
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
						<!-- oozie -->
					<!-- 	<td class="text-center">
										
						</td> -->
						
						<!-- comment -->
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

</body>
<html>
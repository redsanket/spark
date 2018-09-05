<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">PipeLine-2</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li><a href="RunningJobStatus.jsp">Running Job</a></li>
        <li class="active"><a href="./NewCompletedJob.jsp">Completed Job</a></li>
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
            <li><a href="http://openqe48blue-n9.blue.ygrid.yahoo.com:8080/NewCompletedJob.jsp">Hadoop Pipeline</a></li>
            <li><a href="http://openqe53blue-n9.blue.ygrid.yahoo.com:8080/NewCompletedJob.jsp">Pineline-2</a></li>
          </ul>
        </li>
      </ul>
    </div>
    </div>
  </nav>
<%@ include file="header.jsp" %>
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.util.Date,java.io.BufferedReader,java.io.InputStreamReader"%>
   <%
  	String SELECT_QUERY = null;
  	String currentDayHadoopVersion = request.getParameter("build");
  	if (currentDayHadoopVersion != null) {
  %>
  	<%
  		SELECT_QUERY = "select * from integrationFinalResult  where hadoopVersion like  "  + "\"" + currentDayHadoopVersion +  "\"";
  	%>
  <%
  	} else {
  %>
  	<%
  		currentDayHadoopVersion = "tested hadoop versions";
  		SELECT_QUERY = "select * from integrationFinalResult";
  	%>
  <%
  	}
  %>	
		<br>
		 <script type="text/javascript">
		 	$(document).ready(function() {
     			var table = $('#job-completed-table').dataTable({
     				 "iDisplayLength": <%= currentDayHadoopVersion %>
     			});
     			<%
     				if (currentDayHadoopVersion != null) {
     			%>
     					$('input[type=search]').val('<%=currentDayHadoopVersion %>').trigger($.Event("keyup", { keyCode: 13 }));
     			<%
     				}
     			%>
			} );
		 </script>
		<br>
		<%
			Connection con = null;
			try {
		%>
		<%
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
			  } catch(Exception e) {
				  
		%>
		<%@ include file="JDBCErrorPage.jsp" %>
		<%
			  }
				if (con != null) {
		%>
			<div>
			<%	
					Statement stmt = con.createStatement();
					ResultSet resultSet = stmt.executeQuery(SELECT_QUERY);
					int recordCount = resultSet.getRow();
					if (resultSet != null) {
			%>
				<div class="container-fluid">
				<table id="job-completed-table" class="table table-bordered table-hover table-striped " >
				<thead>
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
				</thead>
				<tbody>
				
					<%
						while (resultSet.next()) {
					%>
					<%
							String dataSetName = resultSet.getString("dataSetName");
							
							String hadoopCurrentState = resultSet.getString("hadoopCurrentState");
							String hadoopVersion = resultSet.getString("hadoopVersion");
							String hadoopResult = resultSet.getString("hadoopResult");
							
						 	String gdmCurrentState = resultSet.getString("gdmCurrentState");
						 	String gdmVersion = resultSet.getString("gdmVersion");
							String gdmResult = resultSet.getString("gdmResult");
							
							String tezCurrentState = resultSet.getString("tezCurrentState");
							String tezVersion = resultSet.getString("tezVersion");
							String tezResult = resultSet.getString("tezResult");
							
							String hiveCurrentState = resultSet.getString("hiveCurrentState");
							String hiveVersion = resultSet.getString("hiveVersion");
							String hiveResult = resultSet.getString("hiveResult");
							
							String hcatCurrentState = resultSet.getString("hcatCurrentState");
							String hcatVersion = resultSet.getString("hcatVersion");
							String hcatResult = resultSet.getString("hcatResult");
							
							String hbaseCurrentState = resultSet.getString("hbaseCurrentState");
							String hbaseVersion = resultSet.getString("hbaseVersion");
							String hbaseResult = resultSet.getString("hbaseResult"); 
							
							/* String oozieCurrentState = resultSet.getString("oozieCurrentState");
							String oozieResult = resultSet.getString("oozieResult"); */
							recordCount++;
				 %>
				 <tr>
				 
				 <td class="text-center">
						 <%-- <a href="DetailedTestCaseExecutionResult.jsp?currentDataSetName=<%= dataSetName %>" target="_blank"><%= dataSetName.trim() %></a> --%>
						 <a href="CurrentExecutionResult.jsp?dataSetName=<%= dataSetName %>" target="_blank"><%= dataSetName.trim() %></a>
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
			 										<%= hadoopVersion %>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
			 								<%
			 									} else if (hadoopResult.equals("PASS")) {
			 								%>
			 								<%= hadoopVersion %>
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
 									<%=gdmVersion %>
 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="gdm test failed" />
 								<%
 									} else if (gdmResult.equals("PASS")) {
 								%>
 									<%=gdmVersion %>
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
			 									<%= tezVersion %>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="tez test failed" />
			 								<%
			 									} else if (tezResult.equals("PASS")) {
			 								%>
			 									<%= tezVersion %>
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
			 									<%= hiveVersion %>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hive test failed" />
			 								<%
			 									} else if (hiveResult.equals("PASS")) {
			 								%>
			 									<%= hiveVersion %>
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
			 									<%= hcatVersion %>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hcat test failed" />
			 								<%
			 									} else if (hcatResult.equals("PASS")) {
			 								%>
			 									<%= hcatVersion %>
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
			 									<%= hbaseVersion %>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hbase test failed" />
			 								<%
			 									} else if (hbaseResult.equals("PASS")) {
			 								%>
			 									<%= hbaseVersion %>
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
					<%-- <td class="text-center">
								<%
											if (oozieCurrentState.equals("RUNNING")) {
										%>
											<img src="./images/Running.gif" title="oozie testcase is running" />
										<%
											} else if (oozieCurrentState.equals("COMPLETED")) {
										%>
			 								<%
			 									if (oozieResult.equals("FAIL")) {
			 								%>
			 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="oozie test failed" />
			 								<%
			 									} else if (oozieResult.equals("PASS")) {
			 								%>
			 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="oozie test passed" />
			 								<%
			 									}
			 								%>
										<%
											} else if (oozieCurrentState.equals("UNKNOWN")) {
										%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="oozie testcase has not yet started" />
										<%
											}
										%>			
						</td> --%>
						
						<!-- comment -->
						<td class="text-center">
							 		
						</td>
				 		
			 		 		
				 </tr>
	
		<%
						} // end of while loop
		%>
				</tbody>
			</table>
			</div>
				
		<%
					resultSet.close();
					stmt.close();
					
				}
		%>		
		 	</div>
		 	<br>
			<br> 
		<%
				con.close();
			} // end of con
		%>					 
	</body>
</html>
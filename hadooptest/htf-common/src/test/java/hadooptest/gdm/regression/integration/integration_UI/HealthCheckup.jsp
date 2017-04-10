<%@ include file="header.jsp" %>
<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">Integration Testing</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li><a href="RunningJobStatus.jsp">Running Job</a></li>
        <li><a href="./NewCompletedJob.jsp">Completed Job</a></li>
         <li><a href="Release.jsp">Release</a></li>
         <li class="active" class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Build History<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="HealthCheckup.jsp">Current Day HealthCheck</a></li>
          <!--   <li><a href="./OverAllPerformance.jsp">Compare Hadoop Performance for Difference Release</a></li> -->
          	<li><a href="HealthCheckUpHistory.jsp">HealthCheck history</a></li>
          </ul>
        </li>
      </ul>
      
  <!--     <ul class="nav navbar-nav navbar-right">
        <li><a href="#"><span class="glyphicon glyphicon-log-in"></span>Login</a></li>
      </ul> -->
      
        <ul class="nav navbar-nav navbar-right">
        
        <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Pipe lines<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="http://openqe48blue-n9.blue.ygrid.yahoo.com:8080/HealthCheckup.jsp">Hadoop Pipeline</a></li>
            <li><a href="http://openqe53blue-n9.blue.ygrid.yahoo.com:8080/HealthCheckup.jsp">Pineline-2</a></li>
          </ul>
        </li>

      </ul>
      
      
      
      
    </div>
    </div>
  </nav>
<body>
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat,java.io.BufferedReader,java.io.InputStreamReader"%>
<%
	ResultSet resultSet = null;
%>
		<br>
		 <script type="text/javascript">
		 	$(document).ready(function() {
     			var table = $('#job-completed-table').dataTable();
			} );
		 </script>
		<br>
		<%
			Connection con = null;
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
				con = DriverManager.getConnection("jdbc:mysql://localhost/integration_test","root", password);
		%>
		<% 
			  } catch(Exception e) {
		%>
				<!-- redirect the page to jdbc error page, if connection to db creation is failed. -->
				<%@ include file="JDBCErrorPage.jsp" %>
		<%
			  }
				if (con != null) {
		%>
				<%
					// get today's date
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
					java.util.Date date = new java.util.Date();
				    String todayDate = dateFormat.format(date);
				    int exectionResultCount = 0;
				    
				  	// create the statement
					final String SELECT_QUERY = "select distinct * from health_checkup where date=\""+  todayDate + "\"";
				  	try {
				  %>
				  <%
					Statement stmt = con.createStatement();
					resultSet = stmt.executeQuery(SELECT_QUERY);
					exectionResultCount = resultSet.getRow();
				  %>
				  <%
				  	} catch ( Exception e) {
		  		 %>
			  	 	 <center>
			  	 	 	<h1> Failed :  <%= e.getMessage() %></h1>
			  	 	 </center>
			  	 <%
				  	}
					if (resultSet != null)  {
				%>	
				<%-- <%
				
					 if (exectionResultCount > 0) {
				%> --%>
								<!--  if we get resultset then we are creating the table -->
							<div class="container-fluid">
							<table id="healthCheckUpTable" class="table table-bordered table-hover table-striped">
							<thead>
								<tr>
									<th class="text-center">Date</th>
									<th class="text-center">Cluster State</th>
									<th class="text-center">Oozie State</th>
								    <th class="text-center">Pig State</th>
									<th class="text-center">Hive State</th>
									<th class="text-center">HCat State</th>
									<th class="text-center">HBase State</th>
									<th class="text-center">Tez State</th>
									<th class="text-center">GDM State</th>
									
									<!--  TODO, this is intentionally commented out, the following column will be shown when tez is integrated. -->
									<!-- <th class="text-center">Tez State</th>  -->
								 	
								</tr>
							</thead>
							<tbody>
							<%
								while (resultSet.next()) {
							%>
									<%
										String todaysDateValue = resultSet.getString("date");
										String clusterState = resultSet.getString("Cluster_State");
										String oozieState = resultSet.getString("Oozie_State");
										String pigState = resultSet.getString("Pig_State");
										String hiveState = resultSet.getString("Hive_State");
										String hcatState = resultSet.getString("Hcat_State");
										String hbaseState = resultSet.getString("Hbase_State");
										String tezState = resultSet.getString("Tez_State");
										String gdmState = resultSet.getString("Gdm_State"); 
										
										/*
											TODO, this is intentionally commented out, the following column will be shown when tez is integrated.
											String tez_State =  resultSet.getString("Tez_State");
										*/
										 
									%>
								 	<tr>
				 
				 						<td class="text-center">
				 							<%= todaysDateValue %>
				 						</td>
				 						<td class="text-center">
				 							<%
				 								if (clusterState.indexOf("~") > 0) {
				 							%>
			 									<%
			 										List<String> clusterStateList = Arrays.asList(clusterState.split("~"));
			 										if (clusterStateList.get(0).equals("active")) { 
			 									%>
			 										<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="Cluster is active" /> <%= clusterStateList.get(1) %></span>
			 									<%
			 										} else if (clusterStateList.get(0).equals("down")) {
			 									%>	
			 											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Cluster is down or not reachable" />
			 									<%
			 										}
			 									%>
				 							<%
				 								} else {
				 							%>
		 											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Cluster is down or not reachable" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						<td class="text-center">
					 						<%
					 							if ( oozieState.indexOf("~") > 0) {
					 						%>
					 							<%
					 								List<String> oozieStateList = Arrays.asList(oozieState.split("~"));
					 								if (oozieStateList.get(0).equals("active")) {
					 							%>
					 								<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="oozie is active" /> <%= oozieStateList.get(1) %></span>
					 							<%
					 								} else if (oozieStateList.get(0).equals("down")) {
					 							%>
					 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Oozie is down or not reachable" />
					 							<%
					 								}
					 							%>
					 						<%	
					 							} else {
					 						%>
					 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Oozie is down or not reachable" />
					 						<%
					 							}
					 						%>
				 						</td>
				 						<td class="text-center">
				 							<%
				 								if (pigState.indexOf("~") > 0) {
				 							%>	
				 								<%
				 									List<String> pigStateList = Arrays.asList(pigState.split("~"));
				 									if (pigStateList.get(0).equals("active")) {
				 								%>
			 											<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="pig is active" /> <%= pigStateList.get(1) %></span>
				 								<%
				 									} else if (pigStateList.get(0).equals("down")) {
				 								%>
				 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="pig is not installed" />
				 								<%
				 									}
				 								%>
				 							<%
				 								} else {
				 							%>
				 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="pig is not installed" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						<td class="text-center">
				 							<%
				 								if (hiveState.indexOf("~") > 0) {
				 							%>	
				 								<%
				 									List<String> hiveStateList = Arrays.asList(hiveState.split("~"));
				 									if (hiveStateList.get(0).equals("active")) {
				 								%>
			 											<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="Hive is active" /> <%= hiveStateList.get(1) %></span>
				 								<%
				 									} else  if (hiveStateList.get(0).equals("down")) {
				 								%>
				 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Hive is not installed" />
				 								<%
				 									}
				 								%>
				 							<%
				 								} else {
				 							%>
				 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Hive is not installed" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						<td class="text-center">
				 							<%
				 								if (hcatState.indexOf("~") > 0) {
				 							%>	
				 								<%
				 									List<String> hcatStateList = Arrays.asList(hcatState.split("~"));
				 									if (hcatStateList.get(0).equals("active")) {
				 								%>
			 											<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="Hcat is active" /> <%= hcatStateList.get(1) %></span>
				 								<%
				 									} else  if (hcatStateList.get(0).equals("down")) {
				 								%>
				 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Hcat is not installed" />
				 								<%
				 									}
				 								%>
				 							<%
				 								} else {
				 							%>
				 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="Hcat is not installed" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						<td class="text-center">
				 							<%
				 								if (hbaseState.indexOf("~") > 0) {
				 							%>	
				 								<%
				 									List<String> hbaseStateList = Arrays.asList(hbaseState.split("~"));
				 									if (hbaseStateList.get(0).equals("active")) {
				 								%>
			 											<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="Hbase is active" /> <%= hbaseStateList.get(1) %></span>
				 								<%
				 									} else if (hbaseStateList.get(0).equals("down")) {
				 								%>
				 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="HBase is not installed" />
				 								<%
				 									}
				 								%>
				 							<%
				 								} else {
				 							%>
				 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="HBase is not installed" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						
				 						<td class="text-center">
				 							<%
				 								if (tezState.indexOf("~") > 0) {
				 							%>	
				 								<%
				 									List<String> tezStateStateList = Arrays.asList(tezState.split("~"));
				 									if (tezStateStateList.get(0).equals("active")) {
				 								%>
			 											<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="Hbase is active" /> <%= tezStateStateList.get(1) %></span>
				 								<%
				 									} else if (tezStateStateList.get(0).equals("down")) {
				 								%>
				 										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="HBase is not installed" />
				 								<%
				 									}
				 								%>
				 							<%
				 								} else {
				 							%>
				 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="HBase is not installed" />
				 							<%
				 								}
				 							%>
				 						</td>
				 						<td class="text-center">
				 						
				 							<%
				 								if (gdmState.indexOf("~") > 0) {
		 									%>
		 									 <%
		 									 	List<String> gdmStateList = Arrays.asList(gdmState.split("~"));
		 									 	if (gdmStateList.get(0).equals("active")) {
		 									 %>
		 									 	<span>	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" title="GDM is active" /> <%= gdmStateList.get(1) %></span>
		 									 <%
		 									 	} else if (gdmStateList.get(1).equals("down")) {
		 									 %>
		 									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="GDM version is not installed" />
		 									 <%
		 									 	}  else {
		 									 %>
		 									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="20" title="GDM healthcheckup is not tested" />
		 									 <%
		 									 		}
				 								}
		 									 %>
				 						</td>
				 						
			 						</tr>
							<% 
								} // end while resultSet 
							%>
							</tbody>
						</table>
		<%-- 			<%
						} // end else row count	
					%>  --%>
				<%		
					}
				%>
				</div>	
		<%
				}
		%>
		
		</body>
		</html>
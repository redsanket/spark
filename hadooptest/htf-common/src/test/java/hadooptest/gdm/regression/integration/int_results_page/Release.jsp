<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat"%>
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
         <li class="active"><a href="Release.jsp">Release</a></li>
         <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Build History<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="HealthCheckup.jsp">Current Day HealthCheckUp</a></li>
          	<li><a href="HealthCheckUpHistory.jsp">HealthCheckup history</a></li>
          </ul>
        </li>
        </li>
      </ul>
      <!-- <ul class="nav navbar-nav navbar-right">
          <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Pipe lines<span class="caret"></span></a>
          <ul class="dropdown-menu">
             <li><a href="HealthCheckup.jsp">Hadoop Pipeline</a></li>
          	<li><a href="HealthCheckUpHistory.jsp">Pineline-2</a></li>
          </ul>
        </li>
      </ul> -->
      
      
        <ul class="nav navbar-nav navbar-right">

        <li class="dropdown">
          <a href="#" class="dropdown-toggle" data-toggle="dropdown">Pipe lines<span class="caret"></span></a>
          <ul class="dropdown-menu">
            <li><a href="http://openqe48blue-n9.blue.ygrid.yahoo.com:8080/Release.jsp">Hadoop Pipeline</a></li>
            <li><a href="http://openqe53blue-n9.blue.ygrid.yahoo.com:8080/Release.jsp">Pineline-2</a></li>
          </ul>
        </li>

      </ul>
    </div>
    </div>
  </nav>
<body>
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
				Class.forName("com.mysql.jdbc.Driver").newInstance();
				con = DriverManager.getConnection("jdbc:mysql://localhost/integration_test","root", "");
		%>
		<% 
			  } catch(Exception e) {
		%>
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
				    List<String> resultList = null;
					List<String> hcatResultList = null;
				    
					final String SELECT_QUERY = "select distinct * from integration_test where testType=\"RELEASE\"";
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
							<table id="job-completed-table" class="table table-bordered table-hover table-striped">
							<thead>
								<tr>
									<th class="text-center">Date</th>
									<th class="text-center">Hadoop Build #</th>
<!-- 									<th class="text-center">DataSet Name</th>
 -->									<!-- <th class="text-center">jobStarted</th> -->
									<th class="text-center">GDM</th>
								   <!--  <th class="text-center">Oozie Job Started</th>
									<th class="text-center">cleanUp Output</th>
									<th class="text-center">check Input</th>
									<th class="text-center">pig Raw Processor</th>
									<th class="text-center">Hive Storage</th>
								 	<th class="text-center">Hive Verify</th>
									<th class="text-center">Oozie Job Completed</th> -->
									<th class="text-center">HBase Table Created</th>
									<th class="text-center">HBase Insert</th>
									<th class="text-center">HBase Scan</th>
									<th class="text-center">HBase Table Deleted</th>
									<th class="text-center">Tez</th>
									<th class="text-center">Hive Table Deleted</th>
									<th class="text-center">Hive Table Created</th>
									<th class="text-center">Hive Table Loaded</th>
									<th class="text-center">HCatalog</th>
							</tr>
							</thead>
							<tbody>
							<%
								while (resultSet.next()) {
							%>
									<%
										/* String startTime = resultSet.getString("startTime");
										String hadoopVersion = resultSet.getString("hadoopVersion"); */
										String startTime = resultSet.getString("startTime");
										String dataSetName = resultSet.getString("dataSetName");
										String coreBuildNumber = resultSet.getString("hadoopVersion");
										String jobStarted = resultSet.getString("jobStarted");
										String dataAvailable = resultSet.getString("dataAvailable");
										String oozieJobStarted = resultSet.getString("oozieJobStarted");
										String cleanUpOutput =  resultSet.getString("cleanUpOutput");
										String checkInput =  resultSet.getString("checkInput");
										String pigRawProcessor =  resultSet.getString("pigRawProcessor");
										String hiveStorage =  resultSet.getString("hiveStorage");
										String hiveVerify =  resultSet.getString("hiveVerify");
										String currentStep = resultSet.getString("currentStep");
										String status = resultSet.getString("status");
										String oozieJobCompleted = resultSet.getString("oozieJobCompleted");
										String hbaseCreateTable = resultSet.getString("hbaseCreateTable");
										String hbaseInsert = resultSet.getString("hbaseInsert");
										String hbaseScan = resultSet.getString("hbaseScan");
										String hbaseDeleteTable = resultSet.getString("hbaseDeleteTable");
										String tez = resultSet.getString("tez");
										String hiveTableDeleted = resultSet.getString("hiveTableDeleted");
										String hiveTableCreate = resultSet.getString("hiveTableCreate");
										String hiveLoadData = resultSet.getString("hiveLoadData");
										String hcat = resultSet.getString("hcat");
										
										
									%>
								 	<!-- <tr data-toggle="modal" data-target="#myModal"> -->
<%--								 	<tr>
				 
				 						 <td class="text-center">
				 							<%= startTime %>
				 						</td>
				 						<td class="text-center">
				 							<!-- <a href="url"> --><%= hadoopVersion %><!-- </a> -->
				 						</td> 
				 						
			 						</tr>
			 						--%>
			 						
			 						
			 						 <tr>
			 						  <td class="text-center">
				 							<%= startTime %>
				 						</td>
				 						
							 			<td class="text-center">				 		
							 			<%= coreBuildNumber  %>
							 			</td>
				 
								 	<%-- 	<td class="text-center">				 		
								 			<%= dataSetName  %>   
								 		
								 		</td> --%>
				 		
				<%--  		<td class="text-center">
				 			<%
				 				if (jobStarted.equals("STARTED")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Job Started" />
				 			<%
				 				}
				 			%>
				 		</td> --%>
				 		<td class="text-center">
				 			<%
				 				if (dataAvailable.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="Polling for data" />
				 			<%
				 				} else if  (dataAvailable.equals("POLLING") || dataAvailable.equals("INCOMPLETE") ) {
				 			%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="polling for data or incomplete" />
				 			<%
				 				} else if (dataAvailable.equals("MISSED_SLA")) {
				 			%>
			 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Missed SLA" />
				 			<%
				 				} else if (dataAvailable.equals("AVAILABLE")) {
				 			%>
			 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Data Available on grid" />
				 				<%
				 				}
				 			%>
				 		</td>
				 <%-- 		<td class="text-center">
				 			<%
				 				if ( oozieJobStarted.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="oozie job not yet started." />
				 			<%
				 				} else if ( oozieJobStarted.equals("STARTED")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="oozie job started" />
				 			<%
				 				} else if ( oozieJobStarted.equals("FAILED") || oozieJobStarted.equals("KILLED")) { 
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Missed SLA" />
				 			<%
				 				} 
				 			%>
				 		</td>
				 		<td class="text-center">
				 		
				 		
				 		<%
					 			if (cleanUpOutput.indexOf("~") > 0) {	
					 		%>	
					 		<%
					 				List<String> cleanUpOutputList = Arrays.asList(cleanUpOutput.split("~"));
					 				String cleanUpOutputResult = cleanUpOutputList.get(0).trim();
					 				String cleanUpOutputResultMRJobURL = cleanUpOutputList.get(1).trim();
					 				if (cleanUpOutputResult.equals("UNKNOWN")) {
					 		%>
					 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="CleanUpOutput is not yet started." />
					 		<%		
					 				}  else if (cleanUpOutputResult.equals("RUNNING")) {
					 		%>
					 					<a id="cleanUpOutputRunning" target="_blank" href="<%= cleanUpOutputResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="CleanUpOutput is running" />
					 					</a>
					 		<%
					 				} else if (cleanUpOutputResult.equals("SUCCEEDED") || cleanUpOutputResult.equals("OK")) {
					 		%>
					 					<a id="cleanUpOutputSuccessed" target="_blank" href="<%= cleanUpOutputResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="CleanUpOutput is completed" />
				 						</a>
					 		<%
					 				} else if (cleanUpOutputResult.equals("FAILED") || cleanUpOutputResult.equals("KILLED")) { 
					 		%>
					 					<a id="cleanUpOutputFailed" target="_blank" href="<%= cleanUpOutputResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="CleanUpOutput Failed" />
				 						</a>
					 		<%
					 				}
					 			} else  {
					 		%>			 		
					 			<%
					 				if (cleanUpOutput.equals("UNKNOWN")) {
					 			%>
					 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="CleanUpOutput is not yet started." />
					 			<%
					 				} else if (cleanUpOutput.equals("RUNNING")) {
								%>
										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="CleanUpOutput is running" />
								<%
									} else if (cleanUpOutput.equals("SUCCEEDED") || cleanUpOutput.equals("OK")) {
								%>
										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="CleanUpOutput is completed" />
								<%
									} else if (cleanUpOutput.equals("FAILED") || cleanUpOutput.equals("KILLED")) {
								%>
										<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="CleanUpOutput Failed" />
								<%
									}
					 			%>
					 		<%
					 			}
					 		%>
				 		</td>
				 		<td class="text-center">
				 		
				 			<%
				 				if (checkInput.indexOf("~") > 0) {
				 			%>
				 			<%		List<String> checkInputList = Arrays.asList(checkInput.split("~"));
				 					String checkInputResult = checkInputList.get(0);
				 					String checkInputResultMRJobURL = checkInputList.get(1).trim();
				 					if (checkInputResult.equals("UNKNOWN")) {
				 			%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="checkInput is not yet started." />
				 			<%	
				 					}  else if (checkInputResult.equals("RUNNING")) {
				 			%>
				 						<a id="checkInputResultFailed" target="_blank" href="<%= checkInputResultMRJobURL %>">
				 							<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="checkInput is running" />
			 							</a>
				 			<%
				 					} else if (checkInputResult.equals("SUCCEEDED") || checkInputResult.equals("OK")) {
				 			%>
					 					<a id="checkInputResultSucessed" target="_blank" href="<%= checkInputResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="checkInput is completed" />
				 						</a>	
				 			<%
				 					}  else if (checkInputResult.equals("FAILED") || checkInputResult.equals("KILLED")) {
				 			%>
					 					<a id="checkInputResultFailed" target="_blank" href="<%= checkInputResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="checkInput Failed" />
				 						</a>
				 			<%
				 					}
				 				} else {
				 			%>
					 				<%
						 				
						 				if (checkInput.equals("UNKNOWN")) {
									%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="checkInput is not yet started." />
									<%
										} else if (checkInput.equals("RUNNING")) {
									%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="checkInput is running" />
									<%
										} else if (checkInput.equals("SUCCEEDED") || checkInput.equals("OK")) {
									%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="checkInput is completed" />
									<%
										} else if (checkInput.equals("FAILED") || checkInput.equals("KILLED")) {
									%>
											<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="checkInput Failed" />
									<%
										}
						 			%>
				 			<%
				 				}
				 			%>
				 		</td>
				 		<td class="text-center">
				 			<%
				 				if (pigRawProcessor.indexOf("~") > 0) {
				 					
				 			%>
		 					<%		List<String> pigRawProcessorList = Arrays.asList(pigRawProcessor.split("~"));
		 							String pigRawProcessorResult = pigRawProcessorList.get(0).trim();
		 							String pigRawProcessorResultMRJobURL = pigRawProcessorList.get(1).trim();
		 							if (pigRawProcessorResult.contains("UNKNOWN")) {
		 					%>
		 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="pigRawProcessor is not yet started." />
		 					<%
		 							} else if (pigRawProcessorResult.contains("RUNNING")) {
		 					%>
	 								<a id="pigRawProcessorRunning" target="_blank" href="<%= pigRawProcessorResultMRJobURL %>">
	 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="pigRawProcessor is running" />
 									</a>
		 					<%
		 							} else if (pigRawProcessorResult.contains("SUCCEEDED") || pigRawProcessorResult.contains("OK")) {
		 					%>
		 							<a id="pigRawProcessorSuccessed" target="_blank" href="<%= pigRawProcessorResultMRJobURL %>">
		 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="pigRawProcessor is completed" />
	 								</a>
		 					<%
		 							}  else if (pigRawProcessorResult.contains("FAILED") || pigRawProcessorResult.contains("KILLED")  || pigRawProcessorResult.contains("ERROR") ) {
		 					%>
		 								<a id="pigRawProcessorFailed" target="_blank" href="<%= pigRawProcessorResultMRJobURL %>">
		 									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="pigRawProcessor Failed" />
	 									</a>
		 					<%
		 							}
				 				} else if (pigRawProcessor.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="pigRawProcessor is not yet started." />
				 			<%
				 				}
				 			%>
				 		</td>
				 		<td class="text-center">
				 			<%
				 				if (hiveStorage.indexOf("~") > 0) {
				 			%> 
				 					<%
					 					List<String> hiveStorageList = Arrays.asList(hiveStorage.split("~"));
					 					String hiveStorageResult = hiveStorageList.get(0).trim();
					 					String hiveStorageResultMRJobURL = hiveStorageList.get(1).trim();
					 					if (hiveStorageResult.equals("ERROR")) {
				 					%>
				 							<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="hiveStorage is not yet started." />
				 					<%
					 					} else if ( hiveStorageResult.equals("RUNNING")) {
				 					%>
				 							<a id="hiveStorageRunning" target="_blank" href="<%= hiveStorageResultMRJobURL %>">
				 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="hiveStorage is running" />
				 							</a>
				 					<%
					 					} else if ( hiveStorageResult.equals("SUCCEEDED") || hiveStorageResult.equals("OK")) {
				 					%>
				 							<a id="hiveStorageSuccessed" target="_blank" href="<%= hiveStorageResultMRJobURL %>">
				 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="hiveStorage is completed" />
			 								</a>	
				 					<%
					 					} else if (hiveStorageResult.contains("FAILED") || hiveStorageResult.contains("KILLED")  || hiveStorageResult.contains("ERROR") ) {
				 					%>
				 							<a id="hiveStorageFailed" target="_blank" href="<%= hiveStorageResultMRJobURL %>">
				 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="pigRawProcessor Failed" />
			 								</a>
				 					<%
					 					}
				 					%>
				 			<%
				 				} else if (hiveStorage.equals("UNKNOWN")) {
				 			%>
				 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="hiveStorage is not yet started." />
				 			<%
				 				}
				 			%>
				 		</td>
				 		<td class="text-center">
				 		<%
					 			if  (hiveVerify.indexOf("~") > 0) {
					 		%>
					 		<%
					 				List<String> hiveVerifyList = Arrays.asList(hiveVerify.split("~"));
					 				String hiveVerifyResult = hiveVerifyList.get(0).trim();
					 				String hiveVerifyResultMRJobURL = hiveVerifyList.get(1).trim();
					 				if  (hiveVerifyResult.equals("UNKNOWN")) {
					 		%>
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="hiveVerify is not yet started." />
					 		<%
					 				} else if (hiveVerifyResult.equals("RUNNING")) {
					 		%>
					 					<a id="hiveVerifyRunning" target="_blank" href="<%= hiveVerifyResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="hiveVerify is running" />
				 						</a>
					 		<%
					 				} else if (hiveVerifyResult.equals("SUCCEEDED") || hiveVerifyResult.equals("OK")) {
					 		%>
					 					<a id="hiveVerifySuccessed" target="_blank" href="<%= hiveVerifyResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="hiveVerify is completed" />
				 						</a>
					 		<%
					 				} else if (hiveVerifyResult.equals("FAILED") || hiveVerifyResult.equals("KILLED") || hiveVerifyResult.equals("ERROR")) {
					 		%>
					 					<a id="hiveVerifyFailed" target="_blank" href="<%= hiveVerifyResultMRJobURL %>">
					 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="hiveVerify Failed" />
				 						</a>
					 		<%
					 				}
					 			} else if (hiveVerify.equals("UNKNOWN")) {
					 		%>
					 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="hiveVerify is not yet started." />
					 		<%
					 			}
					 		%>
				 		</td>
				 		<td class="text-center">
				 			<%
				 				if  (oozieJobCompleted.indexOf("~") > 0) {
							%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="oozieJobCompleted is not yet started." />
							<%
								}else if (oozieJobCompleted.equals("RUNNING")) {
							%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="25" title="oozieJobCompleted is running" />
							<%
								} else if (oozieJobCompleted.equals("SUCCEEDED") || oozieJobCompleted.equals("OK")) {
							%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="oozieJobCompleted is completed" />
							<%
								} else if (oozieJobCompleted.equals("FAILED") || oozieJobCompleted.equals("KILLED") || oozieJobCompleted.equals("ERROR")) {
							%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="oozieJobCompleted Failed" />
							<%
								} else if ( oozieJobCompleted.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="oozieJobCompleted is not yet started." />
				 			<%
								}
				 			%>
				 		</td> --%>
				 		
			 			<td class="text-center">
				 				<%
				 					if (hbaseCreateTable.equals("PASS") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="HBase Table created successfully" />
				 				<%
				 					} else if (hbaseCreateTable.equals("FAIL") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="HBase Table creation Failed" />
				 				<%
				 					} else {
				 				%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="HBase Table creation job did not executed" />
				 				<%
				 					}
				 				%>
				 		</td>
				 		
				 		<td class="text-center">
				 		<%
				 			if (hbaseInsert.indexOf("~") > 0) { 
				 		%>
				 				<%
				 					List<String> hbaseInsertList = Arrays.asList(hbaseInsert.split("~"));
				 					if (hbaseInsertList.get(0).equals("PASS") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="HBase Insert is completed" />
				 				<%
				 					} else if (hbaseInsertList.get(0).equals("FAIL") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="HBase Insert is Failed" />
				 				<%
				 					}
				 				%>
				 		<%
				 			} else {
				 		%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="HBase Insert has not yet started." />
				 		<%
				 			}
				 		%>
				 		</td>
				 		<td class="text-center">
				 		<%
				 			if (hbaseScan.indexOf("~") > 0) { 
				 		%>
				 				<%
				 					List<String> hbaseScanList = Arrays.asList(hbaseScan.split("~"));
				 					if (hbaseScanList.get(0).equals("PASS") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="HBase Scan is completed" />
				 				<%
				 					} else if (hbaseScanList.get(0).equals("FAIL") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="HBase Scan is Failed" />
				 				<%
				 					}
				 				%>
				 		<%
				 			} else {
				 		%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="HBase Scan has not yet started." />
				 		<%
				 			}
				 		%>
				 		</td>
				 		
				 		<td class="text-center">
		 				<%
		 					if (hbaseDeleteTable.equals("PASS") == true) {
		 				%>
		 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="HBase table deleted successfully" />
		 				<%
		 					} else if (hbaseDeleteTable.equals("FAIL") == true) {
		 				%>
		 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="HBase table deletion Failed" />
		 				<%
		 					} else {
		 				%>
		 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="HBase table deletion job dn't executed" />
		 				<%
		 					}
		 				%>
			 		</td>
				 		
				 		<td class="text-center">
				 		<%
				 			if (tez.indexOf("~") > 0) {
				 		%>
				 				<%
				 					List<String> tezList = Arrays.asList(tez.split("~"));
				 					if (tezList.get(0).equals("PASS") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Tez job is successful" />
				 				<%
				 					} else if (tezList.get(0).equals("FAIL") == true) {
				 				%>
				 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Tez job is Failed" />
				 				<%
				 					}
				 				%>
				 		<%
				 			} else {
				 		%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="Tez has not yet started." />
				 		<%
				 			}
				 		%>
			 		</td>
			 		<td class="text-center">
			 		<%
			 			if (hiveTableDeleted.equals("UNKNOWN") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="Hive table deletion is not yet started." />
			 		<%
			 			} else if (hiveTableDeleted.equals("FAIL") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Hive table deletion is failed." />
			 		<%
			 			} else if (hiveTableDeleted.equals("PASS") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Hive table deletion is successful" />
			 		<%
			 			}
			 		%>
			 		</td>

			 		<td class="text-center">
			 		<%
			 			if (hiveTableCreate.equals("UNKNOWN") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="Hive table creation is not yet started." />
			 		<%
			 			} else if (hiveTableCreate.equals("FAIL") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Hive table creation is failed." />
			 		<%
			 			} else if (hiveTableCreate.equals("PASS") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Hive table creation is successful" />
			 		<%
			 			}
			 		%>
			 		</td>		
			 		
			 		<td class="text-center">
			 		<%
			 			if (hiveLoadData.equals("UNKNOWN") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="Hive table load data is not yet started." />
			 		<%
			 			} else if (hiveLoadData.equals("FAIL") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="Hive table load data is failed." />
			 		<%
			 			} else if (hiveLoadData.equals("PASS") == true) {
			 		%>
			 				<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="Hive table load data is successful" />
			 		<%
			 			}
			 		%>
			 		</td>	
			 		
			 		<td class="text-center">
			 			<%
			 				int value = hcat.indexOf("~");
			 			//	out.println("value - " + value);
			 				if (hcat.equals("UNKNOWN") == true) {
			 			%>
			 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="25" title="HCatalog reading the hive table is not yet started." />
			 			<%
			 				} if (hcat.indexOf("~") > -1) {
			 			%>
			 				<%
		 						hcatResultList = Arrays.asList(hcat.split("~"));
		 						if (hcatResultList.get(0).trim().equals("FAIL") == true) {
			 				%>
		 							<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="25" title="HCatalog reading the hive table is failed." />
			 				<%
		 						} else if (hcatResultList.get(0).trim().equals("PASS") == true) {
			 				%>
		 							<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="25" title="HCatalog reading the hive table is successful" />
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
				<%		
					}
				%>
				</div>	
		<%
				}
		%>
		
  <div class="modal fade" id="myModal" role="dialog">
    <div class="modal-dialog modal-lg">
      <div class="modal-content">
        <div class="modal-header">
          <button type="button" class="close" data-dismiss="modal">&times;</button>
          <h4 class="modal-title">Modal Header</h4>
        </div>
        <div class="modal-body">
          <p>This is a large modal.</p>
        </div>
        <div class="modal-footer">
          <button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
        </div>
      </div>
    </div>
 </div>
		
<!-- modal -->
</body>
</html>
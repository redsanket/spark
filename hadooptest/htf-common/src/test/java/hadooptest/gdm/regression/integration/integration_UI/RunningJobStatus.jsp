<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">Integration Testing</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li  class="active"><a href="RunningJobStatus.jsp">Running Job</a></li>
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
<%@ include file="header.jsp"%>
<%@ page
	import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*"%>
<%
	SimpleDateFormat feed_sdf = new SimpleDateFormat("yyyyMMddHH");
	Calendar currentTimeStampCal = Calendar.getInstance();
	String currentHrFrequency = "Integration_Testing_DS_" + feed_sdf.format(currentTimeStampCal.getTime()) + "00";
	List<String> resultList = null;
	List<String> hourList = null;
	List<String> totalExecutionTimeList = null;
	HashMap<String,String> resultMap = new HashMap<String,String>();
	List<String> tezExecutionResultList = null;
	List<String> hbaseScanExecutionResultList = null;
	List<String> hcatExecutionResultList = null;
	List<String> pigExecutionResultList = null;
	List<String> cleanUpOutputResultList = null;
%>
<%
	Connection con = null;
	try {
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		con = DriverManager.getConnection("jdbc:mysql://localhost/integration_test", "root", "");
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
		<div>
			<%
		//	currentHrFrequency = "Integration_Testing_DS_201511162100";
				String SELECT_QUERY = "SELECT distinct * from integration_test where dataSetName=\"" + currentHrFrequency.trim() + "\"";
				//out.println("SELECT_QUERY   = " + SELECT_QUERY);
				Statement stmt = con.createStatement();
				ResultSet resultSet = stmt.executeQuery(SELECT_QUERY);
				int recordCount = resultSet.getRow();
			//	out.println("recordCount  = " + recordCount);
				if (resultSet != null) {
			%>
					
					
					<div class="container">
					<h3>Current Frequency : <%=  currentHrFrequency %> </h3><br>
					<table class="table table-bordered table-hover table-striped">
					<thead>
						<tr>
							<th class="text-center">Component Name</th>
							<th class="text-center">Version</th>
							<th class="text-center">Result</th>
					        <th class="text-center">Comments</th>
						</tr>
					</thead>
					<tbody>
			<%
					while (resultSet.next()) {
			%>
					<%
						String jobStarted = resultSet.getString("jobStarted");
						String dataAvailable = resultSet.getString("dataAvailable");
						String hbaseCreateTable = resultSet.getString("hbaseCreateTable");
						String hbaseInsert = resultSet.getString("hbaseInsert");
						String hbaseScan = resultSet.getString("hbaseScan");
						String hbaseDeleteTable = resultSet.getString("hbaseDeleteTable");
						String tez = resultSet.getString("tez").trim();

						String hiveTableDeleted = resultSet.getString("hiveTableDeleted").trim();
						String hiveTableCreate = resultSet.getString("hiveTableCreate").trim();
						String hiveLoadData = resultSet.getString("hiveLoadData").trim();
						String hcat = resultSet.getString("hcat");

						String hadoopVersion = resultSet.getString("hadoopVersion");
						String tezVersion = resultSet.getString("tezVersion");
						String hbaseVersion = resultSet.getString("hbaseVersion");
						String hiveVersion = resultSet.getString("hiveVersion");
						String hcatVersion = resultSet.getString("hcatVersion");
						String pigVersion = resultSet.getString("pigVersion");
						String oozieVersion = resultSet.getString("oozieVersion");

						// oozie
						String oozieJobStarted = resultSet.getString("oozieJobStarted");
						String cleanUpOutput = resultSet.getString("cleanUpOutput");
						String checkInput = resultSet.getString("checkInput");
						String pigRawProcessor = resultSet.getString("pigRawProcessor");
						String hiveStorage = resultSet.getString("hiveStorage");
						String hiveVerify = resultSet.getString("hiveVerify");
						String oozieJobCompleted = resultSet.getString("oozieJobCompleted");
						
						
					%>
						<!-- hadoop  -->
						<tr>
							<td class="text-center">
								Hadoop
							</td>
				 			<td class="text-center">
				 				<%= hadoopVersion %>
				 			</td>
							<td class="text-center">
				 			<%
				 				if (dataAvailable.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Polling for data" />
				 			<%
				 				} else if  (dataAvailable.equals("POLLING") || dataAvailable.equals("INCOMPLETE") ) {
				 			%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="15" title="polling for data or incomplete" />
				 			<%
				 				} else if (dataAvailable.equals("MISSED_SLA")) {
				 			%>
			 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Missed SLA" />
				 			<%
				 				} else if (dataAvailable.equals("AVAILABLE")) {
				 			%>
			 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Data Available on grid" />
				 			<%
				 				}
				 			%>
				 			</td>
				 			<td class="text-center">
				 			</td>
						</tr>

						<!-- GDM -->
						<tr>
							<td class="text-center">
								GDM
							</td>
							<td class="text-center">
				 				6.9
				 			</td>
							<td class="text-center">
				 			<%
				 				if (dataAvailable.equals("UNKNOWN")) {
				 			%>
				 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Polling for data" />
				 			<%
				 				} else if  (dataAvailable.equals("POLLING") || dataAvailable.equals("INCOMPLETE") ) {
				 			%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_yellow.png" width="15" title="polling for data or incomplete" />
				 			<%
				 				} else if (dataAvailable.equals("MISSED_SLA")) {
				 			%>
			 						<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Missed SLA" />
				 			<%
				 				} else if (dataAvailable.equals("AVAILABLE")) {
				 			%>
			 					<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Data Available on grid" />
				 			<%
				 				}
				 			%>
				 			</td>
				 			<td class="text-center">
				 			</td>
						</tr>

						<!-- Tez  -->
						<tr>
							<td class="text-center">
								Tez
							</td>
							<td class="text-center">
							<%= tezVersion %>
							</td>
							<td class="text-center">
								<%
									if (tez.equals("UNKNOWN")) {
								%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Tez is not tested yet." />
								<%
									} else if (tez.indexOf("~") > -1) {
								%>
										<%
											tezExecutionResultList = Arrays.asList(tez.split("~"));
											if (tezExecutionResultList.get(0).equals("FAIL")) {
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Tez job failed." />
										<%
											} else if (tezExecutionResultList.get(0).equals("PASS")) {
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Tez job is passed." />
										<%
											}
											%>
								<%
									}
								%>
							</td>
							<td class="text-center">
								<%
									if (  tezExecutionResultList != null) {
								%>
										<a id="tezMRJobLink" target="_blank" href="<%= tezExecutionResultList.get(1) %>" >
											Tez MR Job
										</a>
								<%
									} else if (tez.equals("UNKNOWN")) {
								%>
								 	Tez job has not started
								<%
									}
								%>
				 			</td>
						</tr>
						<tr>
						<td class="text-center">
							HBase
						</td>
						<td class="text-center">
							<%= hbaseVersion %>
						</td>
						<td class="text-center">

							<table class="table table-bordered table-hover table-striped">
							<thead>
								<tr>
									<th class="text-center">Table deleted</th>
									<th class="text-center">Table Created</th>
									<th class="text-center">Table Scan</th>
								</tr>
							</thead>
							<tbody>
							<tr>
								<td class="text-center">
									 <%
									 	if (hbaseDeleteTable.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Tez is not tested yet." />
									 <%
									 	} else if (hbaseDeleteTable.equals("PASS")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Tez is not tested yet." />
									 <%
									 	}
									 %>
								</td>
								<td class="text-center">
									<%
									 	if (hbaseCreateTable.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Tez is not tested yet." />
									 <%
									 	} else if (hbaseCreateTable.equals("PASS")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Tez is not tested yet." />
									 <%
									 	}
									 %>
								</td>
								<td class="text-center">
									<%
									 	if (hbaseScan.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Tez is not tested yet." />
									 <%
									 	} else if (hbaseScan.indexOf("~") > -1) {
									 %>
								 	<%
								 			hbaseScanExecutionResultList = Arrays.asList(hbaseScan.split("~"));
								 			if (hbaseScanExecutionResultList.get(0).trim().equals("PASS")) {
								 	%>
								 			<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Tez is not tested yet." />
								 	<%
									 		}
									 	}
									%>
								</td>
							</tr>
							</tbody>
							</table>
							</td>
							<td class="text-center">
									<%
										if (hbaseScanExecutionResultList != null) {
									%>
										<a id="tezMRJobLink" target="_blank" href="<%= hbaseScanExecutionResultList.get(1).trim() %>" >
											HBase Table Scan MR Job
										</a>	
									<%
										}
									%>
									
								</td>
							</tr>

							<!-- hive -->
							<tr>
								<td class="text-center">
									Hive
								</td>
								<td class="text-center">
								<%
									if (hiveVersion.indexOf("~") > -1) {
								%>
									<%
										List<String> hiveVersionList = Arrays.asList(hiveVersion.split("~"));
										if (hiveVersionList != null) {
									%>
											<%=  hiveVersionList.get(1) %>
									<%
										}
									%>
								<%
									}
								%>
									
								</td>

								<td class="text-center">
									<!--  hive table -->
									<table class="table table-bordered table-hover table-striped">
							<thead>
								<tr>
									<th class="text-center">Hive Table deleted</th>
									<th class="text-center">Hive Table Created</th>
									<th class="text-center">Load Data to Table</th>
								</tr>
							</thead>
							<tbody>
							<tr>
								<td class="text-center">
									 <%
									 	if (hiveTableDeleted.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Hive table deletion is not tested yet." />
									 <%
									 	} else if (hiveTableDeleted.equals("PASS")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Hive table deletion is successful" />
									 <%
									 	} else if (hiveTableDeleted.equals("FAIL")) {
									 %>
									 	<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Hive table deletion failed" />
									 <%
									 	}
									 %>
								</td>
								<td class="text-center">
									<%
									 	if (hiveTableCreate.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Hive table creation is not tested yet." />
									 <%
									 	} else if (hiveTableCreate.equals("PASS")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Hive table creation is successful" />
									 <%
									 	} else if (hiveTableDeleted.equals("FAIL")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Hive table creation failed" />
									 <%
									 	}
									 %>
								</td>
								<td class="text-center">
									<%
										 	if (hiveLoadData.equals("UNKNOWN")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Hive table Scan using hcat is not tested yet." />
									 <%
									 	} else if (hiveLoadData.equals("PASS")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Hive table Scan using hcat is successfull." />
									 <%
									 	}  else if (hiveLoadData.equals("FAIL")) {
									 %>
									 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Hive table Scan using hcat is failed" />
									 <%
									 	}
									 %>
								</td>
							</tr>
							</tbody>
							</table>
						</td>
						<td class="text-center">
						</td>
						</tr>
						<tr>
								<td class="text-center">
									HCatalog
								</td>
								<td class="text-center">
								<%
									if (hcatVersion.indexOf("~") > -1) {
								%>
									<%
										List<String> hcatVersionList = Arrays.asList(hcatVersion.split("~"));
										if (hcatVersionList != null) {
									%>
											<%=  hcatVersionList.get(1) %>
									<%
										}
									%>
								<%
									}
								%>
								</td>
								<td class="text-center">
								 <%
								 	if (hcat.equals("UNKNOWN")) {
								 %>
								 			<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="HCatalog Job is not yet started." />
								 <%
								 	} else if (hcat.indexOf("~") > -1) {
								 %>
								 	 <%
								 		hcatExecutionResultList = Arrays.asList(hcat.split("~"));
								 	 	if ( hcatExecutionResultList.get(0).trim().equals("PASS")) {
								 	 %>
		 	 								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="HCat Job is successfull." />
								 	 <%
								 	 	} else if ( hcatExecutionResultList.get(0).trim().equals("PASS")) {
								 	 %>
								 	 		<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="HCat Job got failed" />
								 	 <%
								 	 	}
								 	 %>
								 <%
								 	}
								 %>
								</td>
								<td class="text-center">
								<%
								 	if (hcatExecutionResultList != null ) {
								%>
								 	<a id="hcatalogMRJobLink" target="_blank" href="<%= hcatExecutionResultList.get(1).trim() %>" >
											HCatalog MR Job
									</a>
								<%
								 	}
								%>
								
								
								</td>
							</tr>
							<tr>
							<td class="text-center">
								Pig
							</td>
							<td class="text-center">
							
							
							<%
									if (pigVersion.indexOf("~") > -1) {
								%>
									<%
										List<String> pigVersionList = Arrays.asList(pigVersion.split("~"));
										if (pigVersionList != null) {
									%>
											<%=  pigVersionList.get(1) %>
									<%
										}
									%>
								<%
									}
								%>
							</td>
							<td class="text-center">
							<%
									if (tez.equals("UNKNOWN")) {
								%>
									<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="Tez is not tested yet." />
								<%
									} else if (tez.indexOf("~") > -1) {
								%>
										<%
											pigExecutionResultList = Arrays.asList(tez.split("~"));
											if (pigExecutionResultList.get(0).equals("FAIL")) {
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="Tez job failed." />
										<%
											} else if (pigExecutionResultList.get(0).equals("PASS")) {
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="Tez job is passed." />
										<%
											}
										%>
								<%
									}
								%>
							</td>
							<td class="text-center">
							</td>
							</tr>		
							<tr>
								<td class="text-center">
									Oozie
								</td>
								<td class="text-center">
								<%= oozieVersion %>	
								</td>
								<td class="text-center">

							<table class="table table-bordered table-hover table-striped">
							 <thead>
								<tr>
									<th class="text-center">Test</th>
									<th class="text-center">Result</th>
									<th class="text-center">Comment</th>
								</tr>
							</thead> 
							<tbody>
								<tr>
									<td class="text-center">
										<b>oozieJobStarted</b>
									</td>
									<td class="text-center">
										<%
											if (oozieJobStarted.equals("UNKNOWN") == true) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="oozieJobStarted is not tested yet." />
										<%
											} else if (oozieJobStarted.equals("STARTED") == true) {
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_green.png" width="15" title="oozieJobStarted on grid" />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>cleanUpOutput</b>
									</td>
									<td class="text-center">
									<%= cleanUpOutput %>
										<%
											if (cleanUpOutput.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="cleanUpOutput is not tested yet." />
										<%
											} else if (cleanUpOutput.indexOf("~") > -1) {
										%>
											<%
												cleanUpOutputResultList = Arrays.asList(cleanUpOutput.split("~"));
												if (cleanUpOutputResultList.get(0).equals("FAILED")) {
											%>
													<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_red.png" width="15" title="cleanUpOutput test failed." />
											<%
												}
											%>
										<%
											}
										%>
									</td>
									<td class="text-center">
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>checkInput</b>
									</td>
									<td class="text-center">
										<%
											if (checkInput.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="checkInput is not tested yet." />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>pigRawProcessor</b>
									</td>
									<td class="text-center">
										<%
											if (pigRawProcessor.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="pigRawProcessor is not tested yet." />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>hiveStorage</b>
									</td>
									<td class="text-center">
										<%
											if (hiveStorage.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hiveStorage is not tested yet." />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>hiveVerify</b>
									</td>
									<td class="text-center">
										<%
											if (hiveVerify.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="hiveVerify is not tested yet." />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
								<tr>
									<td class="text-center">
										<b>oozieJobCompleted</b>
									</td>
									<td class="text-center">
										<%
											if (oozieJobCompleted.equals("UNKNOWN")) { 
										%>
												<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/circle_grey.png" width="15" title="oozieJobCompleted is not tested yet." />
										<%
											}
										%>
									</td>
									<td class="text-center">
										
									</td>
								</tr>
							</tbody>
							</table>
							</td>
								<td class="text-center">
									
								</td>
							</tr>				
						</tbody>
						</table>
			<%
					}
			%>
			
			
			<%
				}else if (resultSet == null) {
			%>
				<div>
					<h1>Could able to fetch the query.</h1>
				</div>
			<%
				}
			%>
		</div>
<%
				}
%>
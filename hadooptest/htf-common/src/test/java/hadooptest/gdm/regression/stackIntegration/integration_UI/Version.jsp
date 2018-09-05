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
		String SELECT_QUERY = "select dataSetName,date,hadoopVersion,tezVersion,gdmVersion,pigVersion,hiveVersion,hcatVersion,hbaseVersion,oozieVersion from integration_test  where date like  " 
		+ "\"" + currentDayFrequency +  "\""  +"   limit 1";
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
		<center><h3>Stack Component Version : <%= currentHR  %> hour </h3> </center>
			<div class="container">
				<div class="panel panel-default">
					<div class="panel-heading">
						<h1 class="panel-title"> Stack Component Version : <%= currentHR  %> </h1>
					</div>
					<div style="cursor: pointer" class="panel-body">
					<div id="minHadoopContainer" style="width: 50%;">
						<table class="table">
							<thead>
								<tr>
									<th class="text-center">Hadoop</th>
									<th class="text-center">Gdm</th>
									<th class="text-center">Tez</th>
									<th class="text-center">Hive</th>
									<th class="text-center">HCatalog</th>
									<th class="text-center">HBase</th>
									<th class="text-center">Oozie</th>
								</tr>
							</thead>
							<tbody>
		
				<%
					while (resultSet.next()) {
				%>
				
						<%
							String hadoopVersion = resultSet.getString("hadoopVersion");
							String gdmVersion = resultSet.getString("gdmVersion");
							String tezVersion = resultSet.getString("tezVersion");
							String hiveVersion = resultSet.getString("hiveVersion");
							String hcatVersion = resultSet.getString("hcatVersion");
							String hbaseVersion = resultSet.getString("hbaseVersion");
							String oozieVersion = resultSet.getString("oozieVersion");
						%>
					<tr>
						<td class="text-center">
						
						<%
							if (! (hadoopVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= hadoopVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((hadoopVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (gdmVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= gdmVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((gdmVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (tezVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= tezVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((tezVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (hiveVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= hiveVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((hiveVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (hcatVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= hcatVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((hcatVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (hbaseVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= hbaseVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((hbaseVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
						<%
							}
						%>
						</td>
						<td class="text-center">
						<%
							if (! (oozieVersion.indexOf("UNKNOWN") > -1)) {
						%>
								<span><%= oozieVersion %> <img style="float: center; margin: 0px 0px 10px 10px;" src="./images/check.png" width="20" /></span>
						<%
							} else if ((oozieVersion.indexOf("0.0") > -1)) {
						%>
								<img style="float: center; margin: 0px 0px 10px 10px;" src="./images/cross.png" width="20" title="hadoop test failed" />
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
					<b></b>
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
<%@ include file="header.jsp" %>
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat,java.io.BufferedReader,java.io.InputStreamReader"%>
<%
	SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyyMMdd");
	Calendar calendar1 = Calendar.getInstance();
    calendar1.setTimeZone(TimeZone.getTimeZone("UTC"));
    String currentDate1 = simpleDateFormat1.format(calendar1.getTime());
%>
<nav class="navbar navbar-inverse">
    <div class="container-fluid">
    <div class="navbar-header">
      <a class="navbar-brand">Integration Testing</a>
    </div>
    <div>
      <ul class="nav navbar-nav">
        <li  class="active"><a href="CurrentExecutionResult.jsp">Current Running Job</a></li>
         <li><a href="DetailedFinalResult.jsp?date=<%= currentDate1 %>">Today's Result</a></li>
         <li><a href="BuildHistory.jsp">Previous Test Result</a></li>
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
<body>
<form action="UpdateComment.jsp" method="POST">
  <%
  	String QUERY = null;
	String currentdataSetName = request.getParameter("dataSetName");
	if ( currentdataSetName != null) {
  %>
  	<%
  		currentdataSetName = currentdataSetName.substring(0, currentdataSetName.length() - 2);
  		QUERY = "select * from integrationFinalResult  where dataSetName like  "  + "\"" + currentdataSetName +  "%\"";
  	%>
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
			try {
		%>
		
	<%-- <%=  SELECT_QUERY %> --%>
				<%
					Statement stmt = con.createStatement();
					resultSet = stmt.executeQuery(QUERY);
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
									<th class="text-center">Comment</th>
									
								</tr>
							</thead>
							<tbody>
		
				<%
					while (resultSet.next()) {
				%>
				
					<%
						String dataSetName = resultSet.getString("dataSetName");
						String comments = resultSet.getString("comments"); 
						String date = resultSet.getString("date");
					%>
					<tr>
					<td class="text-center">
						<%= dataSetName.trim() %>
					</td>
						<!-- comment -->
						<td class="text-center">
								<% 
									if (comments != null) {
								%>	 
										<input type="hidden" name="dataSetName" value=<%= dataSetName %>>
										<input type="hidden" name="date" value=<%= date %>>
										<textarea name="commentTextArea" class="form-control" rows="3"> <%= comments %></textarea>
								<%
									}
								%>
							 		
						</td>
						<td>
							<input type="submit" value="Submit" />
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
  	
  	
  <%
	} else {
  %>
  <h1><center>Result not available..!</center></h1>
  <%
	}
  %>
</form>
</body>
<html>
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
<%
	String dataSetName = request.getParameter("dataSetName");
	String newCommentValue = request.getParameter("commentTextArea");
	String date = request.getParameter("date");
	String UPDATE_RECORD = "update integrationFinalResult "  + "  set  comments  "  + "=\"" + newCommentValue + "\"" + "  where dataSetName=\"" + dataSetName + "\"" ;
	Connection con = null;
	ResultSet resultSet = null;
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
		<%
				Statement stmt = con.createStatement();
		 		stmt.execute(UPDATE_RECORD);
		 		response.sendRedirect("DetailedFinalResult.jsp?date=" + date);
		 		
		 		
		} catch(Exception e) {
	%>
		 <center>
			  	 	 	<h1> Failed :  <%= e.getMessage() %></h1>
	  	 </center>
	<%
		}
	%>

<%
	} else {
%>
	<h1>Failed to get connection to db</h1>
<%
	}
%>





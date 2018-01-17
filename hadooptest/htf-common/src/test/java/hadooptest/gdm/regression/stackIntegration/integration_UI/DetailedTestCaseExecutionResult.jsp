<%@ include file="header.jsp" %>
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat"%>
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
        <li  class="active"><a href="CurrentExecutionResult.jsp">Detailed Component Testcases</a></li>
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
<%@ include file="header.jsp" %>
<%@ page import="java.util.Calendar,java.text.SimpleDateFormat,java.sql.*,java.util.*,java.text.DateFormat"%>

<script>
    $(document).ready(
            function() {
                setInterval(function() {
                    $('#tez').load();
                    $('#hive').load();
                    $('#hcat').load();
                    $('#hbase').load();
                }, 3000);
            });
</script>

		<%
			Calendar currentTimeStampCal = Calendar.getInstance();
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy:MM:dd-HH");
			String currentHR = simpleDateFormat.format(currentTimeStampCal.getTime());
		%>
		
		<%
			String currentDataSet = request.getParameter("currentDataSetName");
		%>
		<center><h3>Test executing for : <%= currentDataSet %> hour </h3> </center>

	<div  id="tez">
		<jsp:include page="TezTestCaseResult.jsp" />
	</div>
	
	<div  id="hive">
		<jsp:include page="HiveTesetCaseResult.jsp" /> 
	</div>

	<div  id="hcat">
		<jsp:include page="HCatalogTestCasesResult.jsp" />
	</div>

	<div  id="hbase">
		<jsp:include page="HBaseTestCaseResult.jsp" />
	</div>

	<div id="oozie">
		<jsp:include page="OozieTestCaseResult.jsp" />
	</div>

</body>
</html>
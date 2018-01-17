<%@ include file="header.jsp" %>
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

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

<div  id="core">
		<jsp:include page="openqe48blue-n9.blue.ygrid.yahoo.com:8080/CurrentHRResult.jsp">
			<jsp:param name="pipelineName" value="Core Hadoop PipeLine" />  
		</jsp:include> 
	</div>
	
	<div  id="pipeline1">
		<jsp:include page="openqe55blue-n9.blue.ygrid.yahoo.com:8080/CurrentHRResult.jsp">
			<jsp:param name="pipelineName" value="PipeLine - 1" />  
		</jsp:include> 
	</div>

	<!-- <div  id="core">
		<c:import url="openqe48blue-n9.blue.ygrid.yahoo.com:8080/CurrentHRResult.jsp"> </c:import> 
	</div> -->
<%-- 
	<div  id="pipeline1">
		<jsp:include flush="true" page="CurrentHRResult.jsp">
			<jsp:param name="pipelineName" value="PipeLine - 1" />  
		</jsp:include>  --%>

</body>
</html>
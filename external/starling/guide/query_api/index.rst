=========
Query API
=========

Overview
========

The Query API 

Base URI
--------

Requests
--------

Responses
---------

Authorization
-------------


Write Results to HDFS
=====================

Name of the query. This name will be the HDFS Folder to which results will be written to. 
If directory is already present the content is overwritten.The output directory would be 
/user/$username/startling-query-results/Name

Endpoint
--------

queryserver/api/query/

HTTP Methods Supported
----------------------

POST

Parameters
----------

query
#####

The parametrised hive query that is to be submitted. The parameter are marked by $ in the beginning.

queue
#####

The queue to which query has to be submitted.

parammap
########

"This is a JSON string containing parameter name and parameter value. 


Example {
  ""user"": ""user1"",
  ""cluster"":""cluster1"",
  ""dummy"":""cluster11111""
} and the parameter which are in the map are replaced in the query by the value of the property in the JSON object"


Response
--------

oozie Job Id
############

Oozie job id of the query which we submit. Would be failed in the case of failure in submission

Result location
###############

This will be the result location where the final output will be. 
It will be of patter /user/${username}/starling-results/${queryname}.


Oozie Job Status
================

Returns the status of the given oozie job for which we submitted hive query. Should be Running/Finished/Killed/Failed

Endpoint
--------

queryserver/api/query/${oozieJobId}

HTTP Methods Supported
----------------------

GET

Parameters
##########

Status
******


Response
--------



Available Starling Tables 
=========================

This will be list of tables in Starling database which the user can look at.

Endpoint
--------

queryserver/api/tsd/tables

HTTP Methods Supported
----------------------

GET

Return Value
------------

Table List


Table Information
=================

Would be an array of columns which are present in the table. Each of the 
Column will have three fields, name, type and comments.

Endpoint
--------

queryserver/api/tsd/tables






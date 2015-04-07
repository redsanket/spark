Workflows
=========

Overview
--------


Oozie workflows can be parameterized (using variables like ${inputDir} within the 
workflow definition). When submitting a workflow job values for the parameters must 
be provided. If properly parameterized (i.e. using different output directories) 
several identical workflow jobs can concurrently.

Transitions
-----------



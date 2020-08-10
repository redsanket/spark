*****
Pipes
*****

What is Hadoop pipes?
=====================

Hadoop pipes allows users to use the `C++` language for MapReduce programming. The main method it takes is to put the `C++` code of the application logic in a separate process, and then let the Java code communicate with `C++` code through the socket. To a large extent, this approach is similar to Hadoop streaming, where communication differs: one is the standard input output and the other is the socket.


Whick API should I use for `Non-java` applications?
===================================================

Refer to Apache Hadoop :hadoop_rel_doc:`pipes package <api/org/apache/hadoop/mapred/pipes/package-summary.html>`

How to submit the script?
=========================

A quick way to submit the debug script is to set values for the properties ``mapreduce.map.debug.script`` and ``mapreduce.reduce.debug.script``, for debugging map and reduce tasks respectively. These properties can also be set by using APIs ``Configuration.set(MRJobConfig.MAP_DEBUG_SCRIPT, String)`` and ``Configuration.set(MRJobConfig.REDUCE_DEBUG_SCRIPT, String)``. |br|
**Note:** In streaming mode, a debug script can be submitted with the command-line options ``-mapdebug`` and ``-reducedebug``, for debugging map and reduce tasks respectively.

How to Run `C++` programs on Hadoop?
====================================

See the dated tutorial `Hadoop Tutorial 2.2 -- Running C++ Programs on Hadoop <http://www.science.smith.edu/dftwiki/index.php/Hadoop_Tutorial_2.2_--_Running_C++_Programs_on_Hadoop>`_
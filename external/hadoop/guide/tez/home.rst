..  _tez_home:

********
Overview
********

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

Introduction
============

The Apache TEZ project is aimed at building an application framework which
allows for a complex directed-acyclic-graph of tasks for processing data
:cite:`Bikas:2015`. |br|
It is currently built atop :ref:`Apache Hadoop YARN <yarn_home>`.

The 2 main design themes for Tez are:

* Empowering end users by:
  
  * Expressive dataflow definition APIs
  * Flexible Input-Processor-Output runtime model
  * Data type agnostic
  * Simplifying deployment

* Execution Performance
  
  * Performance gains over Map Reduce
  * Optimal resource management
  * Plan reconfiguration at runtime
  * Dynamic physical data flow decisions


Resources
=========

.. include:: /common/tez/tez-reading-resources.rst


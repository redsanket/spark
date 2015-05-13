Bundles
=======

.. 04/16/15: Rewrote.
.. TBD: Provide annotations for diagrams.
.. 05/12/15: Edited.

Overview
--------

Bundle is a higher-level Oozie abstraction that batches a set of Coordinator 
applications. The user can start, stop, suspend, resume, and rerun in the 
Bundle-level, making it easier to control the operation. 

More specifically, the Oozie Bundle system allows the user to define and execute 
a bunch of Coordinator applications often called a data pipeline. No explicit 
dependency exists among the Coordinator applications in a Bundle. A user, however, could 
use the data dependency of Coordinator applications to create an implicit data 
application pipeline. See also the `Bundle Bundle Specification <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/BundleFunctionalSpec.html>`_ 
for more details.

The diagram below shows how the Oozie Client submits Bundles throught the Web service to the Oozie
Server, which then executes Coordinators either based on an interval or data availability. 

.. image:: images/coord_pipeline.jpg
   :height: 334px
   :width: 720 px
   :scale: 95 %
   :alt: Oozie Coordinator Pipeline
   :align: left

Bundle Example
--------------

As we do not have any Bundle examples, see the
following topics:

- `Bundle Definitions <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/BundleFunctionalSpec.html#a2._Definitions>`_
- `Expression Language for Parameterization <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/BundleFunctionalSpec.html#a3._Expression_Language_for_Parameterization>`_
- `Bundle Job <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/BundleFunctionalSpec.html#a4._Bundle_Job>`_ 
- `Oozie Bundle XML-Schema <https://kryptonitered-oozie.red.ygrid.yahoo.com:4443/oozie/docs/BundleFunctionalSpec.html#Appendix_A_Oozie_Bundle_XML-Schema>`_



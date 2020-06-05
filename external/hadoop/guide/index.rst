##################
Hadoop User Guide
##################

This document is **Verizonmedia Proprietary/Confidential**.
Do not release the contents of this document outside the company.

.. toctree::
  :maxdepth: 3
  :numbered:

  user_guide/hadoop/index
  user_guide/tez/index
  hadoop_team/index
  tez_team/index

*****
About
*****

Apache Hadoop is a collection of open-source software utilities that
facilitate using a network of many computers to solve problems involving
massive amounts of data and computation. It provides a software
framework for distributed storage and processing of big data using the
MapReduce programming model.


Contributing to the Doc
========================

This guide is built using `Sphinx-doc <https://www.sphinx-doc.org/en/master/index.html>`_

Installation
-------------

See `installing sphinx page <https://www.sphinx-doc.org/en/master/usage/installation.html>`_

- **prerequisite**: Install ``python`` and ``pip``.
- **Linux**: ``apt-get install python-sphinx``
- **OS X**:
  - ``brew install sphinx-doc``

Then, you may need to install virtualenv using pip: ``pip install virtualenv``.

Guide
-----
- `CheatSheet <http://openalea.gforge.inria.fr/doc/openalea/doc/_build/html/source/sphinx/rest_syntax.html>`_
- `Configuration <https://www.sphinx-doc.org/en/master/usage/configuration.html>`_
- `Online demo <https://sphinx-rtd-theme.readthedocs.io/en/stable/demo/demo.html>`_
- `Using inline labels <https://docs.typo3.org/m/typo3/docs-how-to-document/master/en-us/WritingReST/InlineCode.html>`_
- `TOC guide <https://docutils.sourceforge.io/docs/ref/rst/directives.html#table-of-contents>`_
- Headers style:

  .. code-block:: python

    As an example:

    ##################
    H1: document title
    ##################

    Introduction text.
    *********

    Sample H2
    *********

    Sample content.
    **********

    Another H2
    **********

    Sample H3
    =========

    Sample H4
    ---------

    Sample H5
    ^^^^^^^^^

    Sample H6
    """""""""

    And some text.


  .. code-block:: python

    ReStructuredText Text Roles.
    The ReStructuredText Interpreted Text Roles are valid both for reST and Sphinx processing.
    They are: :emphasis:, :strong:, :literal:, :code:, :math:, :pep-reference:,
             :rfc-reference:, :subscript:, :superscript:, :title-reference:, :raw:.
    The first three are seldom used because we prefer the shortcuts provided by previous reST inline markup.


**Current Team members:**

-  Ahmed Hussein
-  Daryn Sharp
-  Eric Badger
-  Eric Payne
-  Jim Brennan
-  Jon Eagles
-  Kihwal Lee
-  Mark Holderbaugh
-  Nathan Roberts
-  Richard Ross
-  Wayne Badger
-  Zehao Chen

.. _developersguide:

****************
Developers-Guide
****************


.. _internalhadoop:

Internal Hadoop
===============

For new hires, visit the :ref:`New Members page <hadoop_team_getting_started_onboarding>` to get all the information
about verizonmedia environment and the initial steps to set up your
development machine.

To start on yHadoop, visit :ref:`On-Boarding page <hadoop_team_getting_started_development>` and
`Grid Onboarding Guide <https://git.ouroath.com/pages/developer/Bdml-guide/Onboarding_to_the_Grid/>`_.


.. _community_hadoop:

Community Hadoop
================

To start contributing to the Apache Hadoop, follow the wiki page on `community confluence page <https://cwiki.apache.org/confluence/display/HADOOP/How+To+Contribute>`_
that explains in details:

- Dev Environment Setup
- Making Changes
- Contributing your work
- Jira Guidelines

For ideas about what you might contribute, please see the `Project Suggestions <https://cwiki.apache.org/confluence/display/HADOOP2/ProjectSuggestions>`_  page.

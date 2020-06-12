##################
Hadoop User Guide
##################

.. toctree::
  :maxdepth: 4
  :hidden:

  user_guide/hadoop/index
  user_guide/tez/index
  hadoop_team/index
  tez_team/index

-----------

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

This document is **Verizonmedia Proprietary/Confidential**.
Do not release the contents of this document outside the company.


***************
About The guide
***************

Apache Hadoop is a collection of open-source software utilities that
facilitate using a network of many computers to solve problems involving
massive amounts of data and computation. It provides a software
framework for distributed storage and processing of big data using the
MapReduce programming model.


Contributing to the Doc
=======================


This guide is built using `Sphinx-doc <https://www.sphinx-doc.org/en/master/index.html/>`_

Installation
------------

See `installing sphinx page <https://www.sphinx-doc.org/en/master/usage/installation.html/>`_

- **prerequisite**: Install ``python`` and ``pip``.
- **Linux**: ``apt-get install python-sphinx``
- **OS X**:
  - ``brew install sphinx-doc``

Then, you may need to install virtualenv using pip: ``pip install virtualenv``.

re-structuredtext resources
---------------------------

Tools
^^^^^

Useful Tools for documentation:

Sublime
  sublime is a rich editor available on linux and OS X.
  * Install Sublime:
  
     * OS X: Using `homebrew Formulae <https://formulae.brew.sh/cask/sublime-text>`_
     * Linux: Packages are provided for `most of the major distributions <https://www.sublimetext.com/docs/3/linux_repositories.html>`_
  
  * Install sphinx Sublime Packages (see the `guide <https://sublime-and-sphinx-guide.readthedocs.io/en/latest/packages.html>`_)
     
     * Use `RestructuredText Improved` to set highlighting of RST syntax in Sublime.
     * `Sublime RST Completion` is a group of snippets and commands to facilitate writing restructuredText with SublimeText. 

Code Helpers
  * `Online RST Editor <http://rst.ninjs.org>`_
  * Use an online table generator to build tables fast (see `Tables Generator <https://www.tablesgenerator.com/text_tables>`_).
  
  * **Reuse Content:** Sphinx supports several ways to reuse content within and across projects.
     
     * `Use a Substitution <https://sublime-and-sphinx-guide.readthedocs.io/en/latest/reuse.html#use-a-substitution>`_ to reuse short, inline content.
     * `Include a Shared File <https://sublime-and-sphinx-guide.readthedocs.io/en/latest/reuse.html#include-a-shared-file>`_ to reuse longer, more complex content.
     * Uee Iframe to embed external documents into sphinx. For example, the following code sample will embed GDoc into your page:
     
        .. code-block:: rst

          <iframe width="560" height="315" src="https://docs.google.com/document/d/1nleU1sSm7p4Ulp-7KzLcLBh0znHLf_MOklcl8jieEec/edit?usp=sharing" frameborder="0" allowfullscreen></iframe>

Guides
^^^^^^

- `CheatSheet <http://openalea.gforge.inria.fr/doc/openalea/doc/_build/html/source/sphinx/rest_syntax.html/>`_
- `Configuration <https://www.sphinx-doc.org/en/master/usage/configuration.html/>`_
- `Online demo <https://sphinx-rtd-theme.readthedocs.io/en/stable/demo/demo.html/>`_
- `Using inline labels <https://docs.typo3.org/m/typo3/docs-how-to-document/master/en-us/WritingReST/InlineCode.html/>`_
- `TOC guide <https://docutils.sourceforge.io/docs/ref/rst/directives.html#table-of-contents/>`_
- `Writing Code Blocks <https://docs.typo3.org/m/typo3/docs-how-to-document/master/en-us/WritingReST/Codeblocks.html>`_
- `Restructured Text and Sphinx CheatSheet <https://thomas-cokelaer.info/tutorials/sphinx/rest_syntax.html#inline-markup-and-special-characters-e-g-bold-italic-verbatim/>`_
- `Roles guide <https://www.sphinx-doc.org/en/master/usage/restructuredtext/roles.html>`_
- `Directives Guide <https://www.sphinx-doc.org/en/master/usage/restructuredtext/directives.html>`_
- Headers style:

  .. code-block:: rst

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


  ReStructuredText Text Roles
    are valid both for reST and Sphinx processing.

    They are: ``:emphasis:``, ``:strong:``, ``:literal:``, ``:code:``, ``:math:``, ``:pep-reference:``, ``:rfc-reference:``, ``:subscript:``, ``:superscript:``, ``:title-reference:``, ``:raw:``.

    The first three are seldom used because we prefer the shortcuts provided by previous `reST` inline markup.

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


Documents and Resources
-----------------------

Internal Docs
^^^^^^^^^^^^^

* Grid Cookbook

  * `Google Docs: <https://docs.google.com/document/d/1SqTMLgzBuoGoHFf7dU3y4G9eZxzpkAkfT0qIuSlWJdA>`_
  * `Twiki: <https://archives.ouroath.com/twiki/twiki.corp.yahoo.com/view/Grid/CookBook.html>`_

Community Resources
^^^^^^^^^^^^^^^^^^^

YARN + MR Community Sync Up
  `Google Doc <https://docs.google.com/document/d/1GY55sXrekVd-aDyRY7uzaX0hMDPyh3T-AL1kUY2TI5M/edit#heading=h.6wgz1xgh0qde>`_ with meeting minutes and general information.

.. bibliography:: resources/refs.bib
   :cited:
   :all:


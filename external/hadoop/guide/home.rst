***********************
Contributing to the Doc
***********************

This guide (|version|) is built using `Sphinx-doc <https://www.sphinx-doc.org/en/master/index.html/>`_

Setting-up Environment
======================

See `installing sphinx page <https://www.sphinx-doc.org/en/master/usage/installation.html/>`_

- **prerequisite**: Install ``python`` and ``pip``.
- **Linux**: ``apt-get install python-sphinx``
- **OS X**:
  - ``brew install sphinx-doc``

Then, you may need to install virtualenv using pip: ``pip install virtualenv``.


To test the generated code locally, you can simply run the following command from the project root directory:

.. code-block:: bash

    # clean the old build
    make clean
    # to generate only hadoop/docs
    make hadoop
    # OR, generate the whole repository
    make

Then, you open ``docs/hadoop/index.html``


.. note:: Make sure that no ``WARNING`` messages while building hadoop ``rst`` files before you commit your changes.

reStructuredText Resources
==========================

Tools
-----

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

          .. raw:: html

            <iframe width="560" height="315" src="https://docs.google.com/document/d/1nleU1sSm7p4Ulp-7KzLcLBh0znHLf_MOklcl8jieEec/edit?usp=sharing" frameborder="0" allowfullscreen></iframe>

Sphinx Guides
-------------

- `CheatSheet <http://openalea.gforge.inria.fr/doc/openalea/doc/_build/html/source/sphinx/rest_syntax.html/>`_
- `Configuration <https://www.sphinx-doc.org/en/master/usage/configuration.html/>`_
- `Online Sphinx rtd-theme demo <https://sphinx-rtd-theme.readthedocs.io/en/stable/demo/demo.html/>`_
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


  reStructuredText Text Roles
    are valid both for reST and Sphinx processing.

    They are: ``:emphasis:``, ``:strong:``, ``:literal:``, ``:code:``, ``:math:``, ``:pep-reference:``, ``:rfc-reference:``, ``:subscript:``, ``:superscript:``, ``:title-reference:``, ``:raw:``.

    The first three are seldom used because we prefer the shortcuts provided by previous `reST` inline markup.
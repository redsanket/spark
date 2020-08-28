.. _projects_documentation_external:

#############
External Docs
#############

.. contents:: Table of Contents
  :local:
  :depth: 3

-----------

As hadoop team, we want to provide a concise and accurate up-to-date user-guide
to help users and hadoop clients achieving the following:

* Quick instructions for onboarding.
* Important resources and links.
* Finding answers for frequently asked question.
* Debugging and troubleshooting tips for hadoop components.
* Tips and hints for performance tuning
* Overviews and tutorials

The working branch for hadoop-team is
`yhadoop-dev <https://git.vzbuilders.com/ahussein/docs/tree/yhadoop-dev>`_.
Once, the content is ready, it will be merged into the master branch.

To track the progress of each section in the documentation, we use a
`Google Sheet - yhadoop-dev-documentation <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit?usp=sharing>`_.

Each entry represents a unique url from the base url
`https://git.vzbuilders.com/ahussein/docs/tree/yhadoop-dev/external/hadoop/guide`

The categories tab defines the categories of the component:

* *Troubleshooting*: Debugging, solving issues,..etc
* *Overview*: General description of components and subcomponents
* *Guidelines*: Used for tutorials and Howtos


.. admonition:: Ubrella Jira...
   :class: readingbox

    `YHADOOP-3388 | yhadoop-dev user-guide external documentation <https://jira.vzbuilders.com/browse/YHADOOP-3388>`_

-----------

Workflow 
========

Each entry goes through the following phases until it is ready to be published:

* *Moved*: Copied to the Sphinx Git Repo.
* *Pass-1*: Ahmed does a first pass to fix obvious things, delete irrelevant,
  and move contents based on TOC.
* *Pass-2*: Someone gets assigned to review that section (URL).
  Once he reviewed it, he change the status of the component to "`Reviewed`".
* *Reviewed*: The technical content has been reviewed and verified.
  Ahmed does another pass to review formatting and syntax of the RestructuredText.
* *Published*: When the changes are merged . we mark the content as "`Published`".
  Then we can share the URLs with Guy who can use for the BDML-guide.

Developers are mainly working on Pass-2. They can relax following
restructured-text syntax and leave it to Ahmed who can make sure that the format
is in its best shape. |br|
When they start on an entry, they change the status from
`not started`, `in-progress`, to `Done`.
Once it is marked as `Done`, Ahmed will change the phase to "reviewed" and
reset the status to `not-started`.

Each component has its own chapter tab:

* `Mapreduce <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit#gid=1728522281>`_
* `Yarn <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit#gid=1033938181>`_
* `HDFS <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit#gid=178202117>`_
* `Security <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit#gid=2146329232>`_

We define the following categories. Feel free to add more if necessary:

* *FAQ*: relevant to FAQ sections
* *Troubleshooting*: Debugging, solving issues,..etc
* *Overview*: General description of components and subcomponents
* *Guidelines*  Used for tutorials and  Howtos

There will be child tasks created to represent checkpoints to fit into Agile
model by releasing documentation gradually.

-----------

Contributing to the Doc
=======================

Picking up task
---------------

* Go to the Google Sheet -
  `yhadoop-dev-documentation <https://docs.google.com/spreadsheets/d/16t3YxowoE8H2sPAQVp9aVxjkCoJ5wHrzNTWnO_8V4Fc/edit>`_
* Pick task(s) assigned to you in any of the main chapter tabs.
* Update the status as "`In-Progress`"
* You do not need to worry about learning `Sphinx/restructured-text`.
  Ahmed will perform another pass to take care of the formatting.
* Update the entry's "`Status`" marking it as "`Done`" after you create PR.

Next, we explain two options to make changes and PRs.


Option-1.Contributing to the doc from GUI
-----------------------------------------

* Go to ahussein's doc repository `https://git.vzbuilders.com/ahussein/docs.git`
* Github has limited support to Restructured-text. 
* Make sure that you are on
  `yhadoop-dev branch <https://git.vzbuilders.com/ahussein/docs/tree/yhadoop-dev>`_.
* Edit the raw Directly
* once you are done, Create a PR

Option-2.Contributing to the doc offline on Development machine
---------------------------------------------------------------

* Fork the ahussein's hadoop-docs repository `https://git.vzbuilders.com/ahussein/docs.git`
* All the features are based on {{yhadoop-dev}} branch. Make sure you are not
  creating PRs onto the master branch.
* Once you are done with your changes:
  
  * rebase
  * create a PR

For a quick start, you can follow the steps and resources in the following links:

* `Installation and prerequisite <https://git.vzbuilders.com/pages/hadoop/docs/hadoop/home.html>`_
* `reStructuredText Resources <https://git.vzbuilders.com/pages/hadoop/docs/hadoop/home.html#contributing-to-the-doc>`_

Option-3.Send a GDoc or a resource link
---------------------------------------

If you are more comfortable using any other platform, then feel free to use it
then share with Ahmed. |br|
Ahmed can import the content into sphinx.

-----------

FAQ
===

.. rubric:: What is Expected from me to do?


* The documentation has been copied from an old wiki archive based on Hadoop-0.2.
* After moving the content, Ahmed does a quick pass, "`Pass-1`", to refine the
  content as much as possible.
* Since there are so many pages to finish by Ahmed, the quality of "`Pass-1`"
  may not be enough to release.

For each entry in the google sheet, it is expected that:

* Review technical information: Feel free to rephrase and make significant
  changes as necessary.
* Ahmed may lack knowledge regarding grid operations, and legacy content that
  was available in Hadoop-0.2. Therefore a second pass will be highly valuable
  to cover that.
* Review correctness of commands and guidelines. Ahmed may not be able to try
  every CLI due to the large area he needs to cover.
* You do not have to worry much about Sphinx environment. If you think it is more
  efficient just to point technical content (like GDoc, online article..etc) then
  feel free to do that. Just communicate with Ahmed who will be happy to import
  the content into sphinx.
* You can link to external resources instead of reinventing the wheel.
  However, it is recommended we have a self-enclosed documentation as much as
  possible. Then, you feel free to link to external resources for more details.

.. rubric:: Which Version of Hadoop should I use as reference? How do I link to
            External Hadoop Documentation?

* We are building hadoop documentation based on hadoop 2.10.
* Do not hardcode the hadoop version in the text.
  Instead use "`|HADOOP_RELEASE_VERSION|`" which will be dynamically replaced by
  the sphinx engine.
* To refer to hadoop docs. Always refer to the
  `'https://hadoop.apache.org/docs/r2.10.0/%s'`. Again you should not hardcode
  the URL inside the text. Instead, use the macro available for URL generation. |br|
  For example, you can add url to https://hadoop.apache.org/docs/r2.10.0/hadoop-project-dist/hadoop-common/CommandsManual.html, by simply adding this code 

.. code-block:: rst

   :hadoop_rel_doc:`Hadoop Commands Guide <hadoop-project-dist/hadoop-common/CommandsManual.html>`


.. rubric:: What if I believe that the section needs to be moved to another section?

* Fee free to make structure modifications.
* When you move an entry across pages/chapters, please make sure to create a relevant entry in the google sheet.
* Do not delete the old entry from the google sheet. Instead, update the `Status` to `Moved`, and add where it has been moved to in the `Comments` column.

.. rubric:: Do I have to learn sphinx or restructured-text?

No.

If you are not interested, just review the technical content. Ahmed will do a final pass to fix any formatting issues with the content.


.. rubric:: Do I only pick entries assigned to me?

If there is nothing assigned to you, you can pick any entry that satisfies the
following two criteria:

* entry in phase "`Pass-1`", and
* status is "`Done`"

or 

* entry in phase "`Pass-2`", and
* status is "`Not started`"

Once you decide, make sure to change the phase to "`Pass-2" and the status to
"`In-progress`" so that no one else will think it is available.
# Hadoop Documentation

## Overview

This repository contains the documentation for Hadoop users and for the Yahoo team
developing Hadoop technology. The Hadoop user documentation can be found in the
`external` directory, where each technology is given its own directory. For example,
if you wanted to see the user documentation for Hive, you would look in `hadoop/docs/external/hive`.

## Authoring Format

The external documentation is generally authored in reStructuredText and rendered into HTML.

## Where Do I View the Rendered External Documentation?

The user documentation can be found on `http://git.corp.yahoo.com/pages/hadoop/docs/<product>`, where 
`<product>` can be `hue`, `hive`, `storm`, `starling`, or `oozie`. We may have other
Hadoop guides in the future as well. For example,
the Hive User Guide can be found at http://git.corp.yahoo.com/pages/hadoop/docs/hive/. 

The following is the general URI syntax for documentation: `http://devel.corp.yahoo.com/{hadoop_product}/guide` 

## Contributing Documentation

The process of contributing to the documentation for a project is not much different from contributing code.
For those who have worked with Git and contributed source code to a project, you'll only need to learn
the syntax of [reStructuredText (reST)](http://docutils.sourceforge.net/docs/ref/rst/restructuredtext.html).

### Templates

Most likely, you will be contributing tutorials to the guides, so you can pass on your knowledge to your
teammates and other Yahoos. We have included templates where applicable for tutorials and cookbook examples
for your convenience. We greatly appreciate any contributions, including corrections of typos, formatting,
editing, etc.

### How To Contribute
 
#### Prerequisites

Install the following:

* [Git](http://git-scm.com/book/en/Getting-Started-Installing-Git, "Installing Git")
* (Optional) [Sphinx](http://sphinx-doc.org/latest/install.html, "Sphinx Installation")

#### Making Changes

##### Edits 

1. Go to https://git.corp.yahoo.com/hadoop/docs/tree/master/external//<product>/guide/.
1. Find the file you want to edit.
1. Open the file in the GitHub Editor.
1. Click the **Edit** icon.
1. Make your changes.
1. Provide a short commit message.
1. Click **Commit changes** to submit your PR.

##### Substantive Edits / Reorganization of Content

These are for much larger changes, so we recommend using the fork/clone paradigm. 

1. [Fork the repository](https://help.github.com/articles/fork-a-repo, "Forking Git Repository") containing the source files for the documentation. 
1. Clone your fork locally.
1. [Add the remote repository](https://help.github.com/articles/adding-a-remote, "Add a Remote") 
   pointing to the original repository so you can pull in the latest changes to your local repository.
1. (Optional) Create a new branch for your work. This helps you to isolate your changes against the original source files.
1. Make your changes: edits, new documentation, updates. Create new documentation with templates when you can by copying the templates and using a descriptive name for your tutorial/code example.
1. [Fetch the latest changes](https://help.github.com/articles/fetching-a-remote#fetch, "Fetch From a Remote") 
   from the original remote repository and resolve any conflicts.
1. Build the documentation locally to confirm that the HTML is rendered correctly: 
    * `$ make build`
    * `$ cd docs/<product>` # `<product>` can be `oozie`, `hive`, `hue`, `storm`, or `hbase` 
    * `$ open index.html`
    * Navigate to page that was changed and confirm that the changes are correct.
    * `$ make clean` # Removes the build files.
1. [Create a pull request](https://help.github.com/articles/creating-a-pull-request, "Create Pull Request"). 
1. Update your pull request based on the feedback and suggestions in the comments.
1. Once your pull request has been merged, do your regular Git upkeep:
   * Pull the updates from the original repository into your master branch.
   * Delete the branch you used for the pull request both locally and remotely.
1. View the updated documentation at http://git.corp.yahoo.com/pages/hadoop/docs/<product>.
 
## What if I Find Issues With the Documentation?

We welcome users to file issues or make pull requests, so if you
find a mistake or can't find information, let us know by filing an issue.
If you think you can write the documentation, please do so and then submit a PR.
Our team will review your PR, make comments/suggestions, and eventually
merge your PR.


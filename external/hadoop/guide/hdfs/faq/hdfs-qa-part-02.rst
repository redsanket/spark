Hadoop Archives
===============

HDFS does not perform well when there are a large number of files significantly
smaller than the HDFS block size.
You can create :abbr:`HAR (Hadoop Archives)` files to reduce the number of files.
Hadoop archives are special format archives with the extension "`.har`" that map
to a file system directory. 

**Usage:**

.. code-block:: bash

  hadoop archive -archiveName name -p <parent> [-r <replication factor>] <src>* <dest>


``-archiveName``
  is the name of the archive you would like to create.
  An example would be "`foo.har`". The name should have a "`*.har`" extension.
``-p``
  The parent argument is to specify the relative path to which the files
  should be archived to. |br| Example would be :
  ``-p /foo/bar a/b/c e/f/g``. Here "`/foo/bar`" is the parent path and
  "`a/b/c`", "`e/f/g`" are relative paths to parent. Note that this is a
  Map/Reduce job that creates the archives.
  You would need a map reduce cluster to run this.
``-r``
  indicates the desired replication factor; if this optional argument is not
  specified, a replication factor of 3 will be used.



If you just want to archive a single directory `/foo/bar` then you can just use:
  
  .. code-block:: bash

     hadoop archive -archiveName zoo.har -p /foo/bar -r 3 /outputdir

.. sidebar:: Reading....

  * Apache Docs - :hadoop_rel_doc:`Archives Guide <hadoop-archives/HadoopArchives.html>`
  * Bdml-guide - `Creating Archives <https://git.vzbuilders.com/pages/developer/Bdml-guide/grid_cline/#creating-archives>`_

If you specify source files that are in an encryption zone, they will be
decrypted and written into the archive. If the har file is not located in an
encryption zone, then they will be stored in clear (decrypted) form.
If the `har` file is located in an encryption zone they will be stored in encrypted
form.

How to set 777 Permission as default
====================================

I need the files I create to be readable and writable by my colleagues.
I set the dir to be 777, but every time I delete the files, and recreate them,
they become non-readable again by my colleagues.

**Ans**:

  .. code-block:: bash

    hdfs dfs -chmod [-R] <MODE[,MODE]... | OCTALMODE> URI [URI ...]

The mode of a new file or directory is restricted by the umask set as a
configuration parameter. When the existing ``create(path, …)`` method (without
the permission parameter) is used, the mode of the new file is
``0666 & ^umask``.

When the new ``create(path, permission, …)`` method (with the permission
parameter ``P``) is used, the mode of the new file is
``P & ^umask & 0666``.

When a new directory is created with the existing
``mkdirs(path)`` method (without the permission parameter), the mode of the new
directory is ``0777 & ^umask``. When the new ``mkdirs(path, permission)``
method (with the permission parameter ``P``) is used, the mode of new directory
is ``P & ^umask & 0777``.

The umask used when creating files and directories. It can be set by the
following configuration ``fs.permissions.umask-mode`` makes all files readable
and writeable.

For more details, see Apache Docs - :hadoop_rel_doc:`HDFS Permissions Guide <hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html>`


Encryption Zones
================

How do encryptions zones affect GDM copies?
-------------------------------------------

* GDM understands encryption zones and for the most part makes it completely
  transparent to users. 
* Copying data from EZA to EZB is possible but required paranoid approval.
  This should be extremely rare and should only be utilized when absolutely required.
* GDM cannot copy from an encrypted directory to an unencrypted directory.
  Always encrypt the leaves of any GDM copies first.
* If you have questions regarding GDM and EZs, please contact `#gdm-users` during
  business hours US Central time.

How do encryption zones affect distcp?
-------------------------------------------

* Because data encryption keys are generated when a file is being created,
  copying the same file from one location to another will result in two files
  that are NOT comparable in their encrypted form.
  Their HDFS checksums are different, and they would fail to cmp.
* So, when copying files using `distcp`,
  add the following option ``-skipcrccheck -update``. This will instruct distcp
  to NOT perform a crc check after copying files. 
  


==================
HiveQL Cheat Sheet 
==================

This chapter provides usage examples for the most common HiveQL statements. 
See the `Hive Language Manual DDL <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`_
for a comprehensive compilation of HiveQL. 

.. contents:: Contents
   :depth: 2
   :local:

.. _hive_cs-db:

Databases
=========

.. _db-showing:

Showing
-------

::

     SHOW DATABASES;


.. _db-describing:

Describing
----------


::

    DESCRIBE foo;

.. _db-creating:

Creating
--------

::

    CREATE DATABASE foo;

::

    CREATE DATABASE IF NOT EXISTS foo;

::

    CREATE DATABASE foo LOCATION '/users/sumeetsi';

.. _db-using:

Using
-----

::

    USE foo;



.. _db-dropping:

Dropping
--------

::

    DROP DATABASE foo;

::

    DROP DATABASE IF EXISTS foo;



.. _hive_cs-tables:

Tables
======

.. _tables-showing:

Showing
-------

::

    SHOW TABLES; 

.. _tables-describing:

Describing
----------

::

    DESCRIBE foo;

.. _tables-creating:

Creating
--------

::

    CREATE TABLE employees(name STRING, age INT);

::

    CREATE TABLE IF NOT EXISTS employees(name STRING, age INT)
        LOCATION ‘/users/sumeetsi/employees’;
		or

::


    CREATE TABLE employees(name STRING, age INT)
        PARTITIONED BY (join_dt STRING);

::


    CREATE TABLE employees(name STRING, age INT)
        STORED AS RCFILE;

.. _tables-altering:

Altering a Table
----------------

::

    ALTER TABLE employees RENAME TO snv_employees;	

::

    ALTER TABLE employees
        REPLACE COLUMNS (emp_name STRING, emp_age INT);

::

    ALTER TABLE employees ADD COLUMNS (emp_id STRING);


::

    ALTER TABLE all_employees
        DROP PARTITION (location='Las Vegas');

.. _tables-truncating:

Truncating a Table
------------------

::

    TRUNCATE TABLE all_employees PARTITION (location='NYC');

.. _hive_cs-query:

Querying Data
=============

.. _query-all:

All Columns
-----------

::

    SELECT * FROM employees;


.. _query-filter:

Filtering Results
-----------------

::

    SELECT name, age FROM employees
        WHERE age > 30;

::

    SELECT name, age, gender FROM employees
        WHERE age < 30 AND gender = "Female";

.. _query-limit:

Limiting Results
----------------

::

    SELECT * FROM employees LIMIT 10;

.. _query-configure:

Configuring Output
------------------

::

    SET hive.exec.compress.output=false;
    SET hive.cli.print.header=true;

.. _query-insert:

Inserting Data From Select Statement
------------------------------------

:: 

    INSERT OVERWRITE LOCAL DIRECTORY ‘/homes/sumeetsi/hivedemo’
    SELECT * FROM all_employees
    WHERE location = ‘Sunnyvale’;
		etc.

.. _hive_cs-join:

Joins
=====

:: 
   
    SELECT e.name, d.dept_name
    FROM all_employees e JOIN departments d
    ON (e.dept_id = d.dept_id);
   
::

    SELECT e.name, d.dept_name
    FROM all_employees e
    LEFT OUTER JOIN departments d
    ON (e.dept_id = d.dept_id);	
   

.. _join-inner:

Inner Join
----------

::

    SELECT table1.guid, c1, c2 FROM table1 INNER JOIN table2 ON table1.guid = table2.guid;

.. _join-outer:

Outer Join
----------

::

    SELECT table1.guid, c1, c2 FROM table1 LEFT OUTER JOIN table2 ON table1.guid = table2.guid;

::

    SELECT table1.guid, c1, c2 FROM table1 RIGHT OUTER JOIN table2 ON table1.guid = table2.guid;

::

    SELECT table1.guid, c1, c2 FROM table1 FULL OUTER JOIN table2 ON table1.guid = table2.guid;

   
   

# Minirel Database Management System

Minirel is a single-user database management system (DBMS) developed as part of UW-Madison CS564 project. It is designed to execute specific SQL queries to give insights into the DBMS's organization and operations.

## Project Overview

Minirel was developed with the aim of understanding the inner workings of a DBMS. The project consists of four main parts, each focusing on a different aspect of the DBMS.

### Part 3: Buffer Manager

The Buffer Manager controls the memory resident database pages in Minirel. It handles requests for data pages, checking their availability in the buffer pool. It also features management of "dirty" pages - copies in the buffer pool that differ from the copy on disk, optimizing memory usage.

### Part 4: HeapFile Manager

The HeapFile Manager component of Minirel imposes a logical ordering on the pages via a linked list, differentiating it from the physical DB layer of files.

### Part 5: Database Utilities and Operators

This part of the project focuses on the design and implementation of Minirel's catalog relations, enhancing the functionality of the DBMS. Minirel supports various SQL-like commands such as selection, projection, insertion, and deletion. These commands are parsed and appropriate calls are made to the backend, providing an interface for querying and updating the database.

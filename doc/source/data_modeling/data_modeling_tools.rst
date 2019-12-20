.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Cassandra Data Modeling Tools
=============================

There are several tools available to help you design and
manage your Cassandra schema and build queries.

* `Hackolade <https://hackolade.com/nosqldb.html#cassandra>`_
  is a data modeling tool that supports schema design for Cassandra and
  many other NoSQL databases. Hackolade supports the unique concepts of
  CQL such as partition keys and clustering columns, as well as data types
  including collections and UDTs. It also provides the ability to create
  Chebotko diagrams.

* `Kashlev Data Modeler <http://kdm.dataview.org/>`_ is a Cassandra
  data modeling tool that automates the data modeling methodology
  described in this documentation, including identifying
  access patterns, conceptual, logical, and physical data modeling, and
  schema generation. It also includes model patterns that you can
  optionally leverage as a starting point for your designs.

* DataStax DevCenter is a tool for managing
  schema, executing queries and viewing results. While the tool is no
  longer actively supported, it is still popular with many developers and
  is available as a `free download <https://academy.datastax.com/downloads>`_.
  DevCenter features syntax highlighting for CQL commands, types, and name
  literals. DevCenter provides command completion as you type out CQL
  commands and interprets the commands you type, highlighting any errors
  you make. The tool provides panes for managing multiple CQL scripts and
  connections to multiple clusters. The connections are used to run CQL
  commands against live clusters and view the results. The tool also has a
  query trace feature that is useful for gaining insight into the
  performance of your queries.

* IDE Plugins - There are CQL plugins available for several Integrated
  Development Environments (IDEs), such as IntelliJ IDEA and Apache
  NetBeans. These plugins typically provide features such as schema
  management and query execution.

Some IDEs and tools that claim to support Cassandra do not actually support
CQL natively, but instead access Cassandra using a JDBC/ODBC driver and
interact with Cassandra as if it were a relational database with SQL
support. Wnen selecting tools for working with Cassandra you’ll want to
make sure they support CQL and reinforce Cassandra best practices for
data modeling as presented in this documentation.

*Material adapted from Cassandra, The Definitive Guide. Published by
O'Reilly Media, Inc. Copyright © 2020 Jeff Carpenter, Eben Hewitt.
All rights reserved. Used with permission.*
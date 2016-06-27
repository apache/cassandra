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

.. _cql:

The Cassandra Query Language (CQL)
==================================

This document describes the Cassandra Query Language (CQL) [#]_. Note that this document describes the last version of
the languages. However, the `changes <#changes>`_ section provides the diff between the different versions of CQL.

CQL offers a model close to SQL in the sense that data is put in *tables* containing *rows* of *columns*. For
that reason, when used in this document, these terms (tables, rows and columns) have the same definition than they have
in SQL. But please note that as such, they do **not** refer to the concept of rows and columns found in the deprecated
thrift API (and earlier version 1 and 2 of CQL).

.. toctree::
   :maxdepth: 2

   definitions
   types
   ddl
   dml
   indexes
   mvs
   security
   functions
   json
   triggers
   appendices
   changes

.. [#] Technically, this document CQL version 3, which is not backward compatible with CQL version 1 and 2 (which have
   been deprecated and remove) and differs from it in numerous ways.

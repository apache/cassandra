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

.. highlight:: cql

.. _arithmetic_operators:

Arithmetic Operators
--------------------

CQL supports the following operators:

=============== =======================================================================================================
 Operator        Description
=============== =======================================================================================================
 \- (unary)      Negates operand
 \+              Addition
 \-              Substraction
 \*              Multiplication
 /               Division
 %               Returns the remainder of a division
=============== =======================================================================================================

.. _number-arithmetic:

Number Arithmetic
^^^^^^^^^^^^^^^^^

All arithmetic operations are supported on numeric types or counters.

The return type of the operation will be based on the operand types:

============= =========== ========== ========== ========== ========== ========== ========== ========== ==========
 left/right   tinyint      smallint   int        bigint     counter    float      double     varint     decimal
============= =========== ========== ========== ========== ========== ========== ========== ========== ==========
 **tinyint**   tinyint     smallint   int        bigint     bigint     float      double     varint     decimal
 **smallint**  smallint    smallint   int        bigint     bigint     float      double     varint     decimal
 **int**       int         int        int        bigint     bigint     float      double     varint     decimal
 **bigint**    bigint      bigint     bigint     bigint     bigint     double     double     varint     decimal
 **counter**   bigint      bigint     bigint     bigint     bigint     double     double     varint     decimal
 **float**     float       float      float      double     double     float      double     decimal    decimal
 **double**    double      double     double     double     double     double     double     decimal    decimal
 **varint**    varint      varint     varint     decimal    decimal    decimal    decimal    decimal    decimal
 **decimal**   decimal     decimal    decimal    decimal    decimal    decimal    decimal    decimal    decimal
============= =========== ========== ========== ========== ========== ========== ========== ========== ==========

``*``, ``/`` and ``%`` operators have a higher precedence level than ``+`` and ``-`` operator. By consequence,
they will be evaluated before. If two operator in an expression have the same precedence level, they will be evaluated
left to right based on their position in the expression.

.. _datetime--arithmetic:

Datetime Arithmetic
^^^^^^^^^^^^^^^^^^^

A ``duration`` can be added (+) or substracted (-) from a ``timestamp`` or a ``date`` to create a new
``timestamp`` or ``date``. So for instance::

    SELECT * FROM myTable WHERE t = '2017-01-01' - 2d

will select all the records with a value of ``t`` which is in the last 2 days of 2016.

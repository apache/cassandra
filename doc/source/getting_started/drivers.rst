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

.. _client-drivers:

Client drivers
--------------

Here are known Cassandra client drivers organized by language. Before choosing a driver, you should verify the Cassandra
version and functionality supported by a specific driver.

Java
^^^^

- `Achilles <http://achilles.archinnov.info/>`__
- `Astyanax <https://github.com/Netflix/astyanax/wiki/Getting-Started>`__
- `Casser <https://github.com/noorq/casser>`__
- `Datastax Java driver <https://github.com/datastax/java-driver>`__
- `Kundera <https://github.com/impetus-opensource/Kundera>`__
- `PlayORM <https://github.com/deanhiller/playorm>`__

================= =================== ======== ====== ==================== ======= =======
Cassandra         Achilles            Astyanax Casser DataStax Java Driver Kundera PlayORM
================= =================== ======== ====== ==================== ======= =======
3.11.0            5.3.1                               4.3.0                3.13  
3.7               4.2.3, 5.0.0, 5.2.1                                      3.13  
2.2.3             4.0.1                                                    3.13  
2.1.5             3.2.3                                                    3.13 
2.1.4                                          1.2.0                       3.13  
2.1                                                                        3.13  
2.0.15            3.0.22                                                   3.13                                                                             
================= =================== ======== ====== ==================== ======= =======

Python
^^^^^^

- `Datastax Python driver <https://github.com/datastax/python-driver>`__

================= ======================
Cassandra         DataStax Python Driver
================= ======================
2.1+              3.1.9
================= ======================

Ruby
^^^^

- `Datastax Ruby driver <https://github.com/datastax/ruby-driver>`__

================= ====================
Cassandra         DataStax Ruby Driver
================= ====================
2.1, 2.2, 3.x     3.2.3
================= ====================

C# / .NET
^^^^^^^^^

- `Cassandra Sharp <https://github.com/pchalamet/cassandra-sharp>`__
- `Datastax C# driver <https://github.com/datastax/csharp-driver>`__
- `Fluent Cassandra <https://github.com/managedfusion/fluentcassandra>`__

================= =============== ================== ================
Cassandra         Cassandra Sharp C# DataStax Driver Fluent Cassandra
================= =============== ================== ================
3.x               4.0                               
1.2+                              3.11               1.2.6
================= =============== ================== ================

Nodejs
^^^^^^

- `Datastax Nodejs driver <https://github.com/datastax/nodejs-driver>`__
- `Node-Cassandra-CQL <https://github.com/jorgebay/node-cassandra-cql>`__

================= ====================== ==================
Cassandra         DataStax Nodejs Driver Node Cassandra CQL
================= ====================== ==================
1.2+              4.1                    0.5
================= ====================== ==================

PHP
^^^

- `CQL \| PHP <http://code.google.com/a/apache-extras.org/p/cassandra-pdo>`__
- `Datastax PHP driver <https://github.com/datastax/php-driver/>`__
- `PHP-Cassandra <https://github.com/aparkhomenko/php-cassandra>`__
- `PHP Library for Cassandra <http://evseevnn.github.io/php-cassandra-binary/>`__

+--------------+------+---------------+---------------+--------------------+
| Cassandra    | CQL  | DataStax PHP  | PHP-Cassandra | PHP Library for    |
|              | PHP  | Driver        |               | Cassandra          |
+==============+======+===============+===============+====================+
| 2.1+         |      | 1.3.0         |               |                    |
+--------------+------+---------------+---------------+--------------------+

C++
^^^

- `Datastax C++ driver <https://github.com/datastax/cpp-driver>`__
- `libQTCassandra <http://sourceforge.net/projects/libqtcassandra>`__

================= =================== ==============
Cassandra         DataStax C++ driver libQTCassandra
================= =================== ==============
2.1, 2.2, 3.0+    2.13              
================= =================== ==============

Scala
^^^^^

- `Datastax Spark connector <https://github.com/datastax/spark-cassandra-connector>`__
- `Phantom <https://github.com/newzly/phantom>`__
- `Quill <https://github.com/getquill/quill>`__

================= =============== ======= =====
Cassandra         Spark Connector Phantom Quill
================= =============== ======= =====
3.1.0                             2.2.1   3.4.5
2.1.5, 2.2, 3.0   2.4                    
2.1.5             1.4, 1.3               
2.1, 2.0          1.2                    
2.1, 2.0          1.1                    
2.0               1.0                    
================= =============== ======= =====

Clojure
^^^^^^^

- `Alia <https://github.com/mpenet/alia>`__
- `Cassaforte <https://github.com/clojurewerkz/cassaforte>`__
- `Hayt <https://github.com/mpenet/hayt>`__

========= ===== ========== =====
Cassandra Alia  Cassaforte Hayt
========= ===== ========== =====
2.0+      4.2.1 3.0        4.1.0
========= ===== ========== =====

Erlang
^^^^^^

- `CQerl <https://github.com/matehat/cqerl>`__
- `Erlcass <https://github.com/silviucpp/erlcass>`__

========= ===== =======
Cassandra CQerl Ericass
========= ===== =======
3.1       1.9.1
3.0             3.2.6
========= ===== =======

Go
^^

- `CQLc <http://relops.com/cqlc/>`__
- `Gocassa <https://github.com/hailocab/gocassa>`__
- `GoCQL <https://github.com/gocql/gocql>`__



Haskell
^^^^^^^

- `Cassy <https://github.com/ozataman/cassy>`__

========= =====
Cassandra Cassy
========= =====
          0.7.1
========= =====

Rust
^^^^

- `Rust CQL <https://github.com/neich/rust-cql>`__

========= ========
Cassandra RUST CQL
========= ========
2.1       0.0.2
========= ========

Perl
^^^^

- `Cassandra::Client and DBD::Cassandra <https://github.com/tvdw/perl-dbd-cassandra>`__

========= ================ =============
Cassandra Cassandra Client DBD-Cassandra
========= ================ =============
          0.16             0.57
========= ================ =============

Elixir
^^^^^^

- `Xandra <https://github.com/lexhide/xandra>`__
- `CQEx <https://github.com/matehat/cqex>`__

========= ====== =====
Cassandra Xandra CQEx
========= ====== =====
          0.12.0 0.2.0
========= ====== =====

Dart
^^^^

- `dart_cassandra_cql <https://github.com/achilleasa/dart_cassandra_cql>`__

========= =====
Cassandra Dart
========= =====
          0.1.5
========= =====

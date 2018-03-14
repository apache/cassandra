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

Inserting and querying
----------------------

The API to Cassandra is :ref:`CQL <cql>`, the Cassandra Query Language. To use CQL, you will need to connect to the
cluster, which can be done:

- either using cqlsh,
- or through a client driver for Cassandra.

CQLSH
^^^^^

cqlsh is a command line shell for interacting with Cassandra through CQL. It is shipped with every Cassandra package,
and can be found in the bin/ directory alongside the cassandra executable. It connects to the single node specified on
the command line. For example::

    $ bin/cqlsh localhost
    Connected to Test Cluster at localhost:9042.
    [cqlsh 5.0.1 | Cassandra 3.8 | CQL spec 3.4.2 | Native protocol v4]
    Use HELP for help.
    cqlsh> SELECT cluster_name, listen_address FROM system.local;

     cluster_name | listen_address
    --------------+----------------
     Test Cluster |      127.0.0.1

    (1 rows)
    cqlsh>

See the :ref:`cqlsh section <cqlsh>` for full documentation.

Client drivers
^^^^^^^^^^^^^^

A lot of client drivers are provided by the Community and a list of known drivers is provided in :ref:`the next section
<client-drivers>`. You should refer to the documentation of each drivers for more information on how to use them.

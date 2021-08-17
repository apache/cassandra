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

Troubleshooting
===============

As any distributed database does, sometimes Cassandra breaks and you will have
to troubleshoot what is going on. Generally speaking you can debug Cassandra
like any other distributed Java program, meaning that you have to find which
machines in your cluster are misbehaving and then isolate the problem using
logs and tools. Luckily Cassandra had a great set of instrospection tools to
help you.

These pages include a number of command examples demonstrating various
debugging and analysis techniques, mostly for Linux/Unix systems. If you don't
have access to the machines running Cassandra, or are running on Windows or
another operating system you may not be able to use the exact commands but
there are likely equivalent tools you can use.

.. toctree::
    :maxdepth: 2

    finding_nodes
    reading_logs
    use_nodetool
    use_tools

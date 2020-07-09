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

..  _license_compliance:

License Compliance
******************


The target of this document is to provide an overview and guidance how the Apache Cassandra project's source code and
artifacts maintain compliance with the `ASF Licensing policy <http://www.apache.org/legal/release-policy.html#licensing>`.

The repository contains a LICENSE file, and a NOTICE file.

The Apache Cassandra project enforces and verifies ASF License header conformance on all source files using the Apache RAT tool.

With a few exceptions, source files consisting of works submitted directly to the ASF by the copyright owner or owner's
agent must contain the appropriate ASF license header. Files without any degree of creativity don't require a license header.

Currently, RAT checks all .bat, .btm, .cql, .css, .g, .hmtl, .iml, .java, .jflex, .jks, .md, .mod, .name, .pom, .py, .sh, .spec, .textile, .yml, .yaml, .xml files for a LICENSE header.

If there is an incompliance, the build will fail with the following warning:

    Some files have missing or incorrect license information. Check RAT report in build/rat.txt for more details!

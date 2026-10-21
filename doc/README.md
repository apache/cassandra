<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

Apache Cassandra documentation directory
========================================

This directory contains the documentation maintained in-tree for Apache
Cassandra. This directory contains the following documents:
- The source of the official Cassandra documentation, in the `source/modules`
  subdirectory. See below for more details on how to edit/build that
  documentation.
- The specification(s) for the supported versions of native transport protocol.


Official documentation
----------------------

The source for the official documentation for Apache Cassandra can be found in the `modules/cassandra/pages` subdirectory. The documentation uses [antora](http://www.antora.org/) and is thus written in [asciidoc](http://asciidoc.org).

The `antora.yml` file is auto-generated and should not be manually edited. It is generated from the version in `build.xml` using `scripts/gen-antora-yml.py` and automatically detects whether building from a release tag or branch HEAD to set the appropriate full or short version.

The antora.yml and some of the asciidoc files are dynamically generated using `ant gen-asciidoc`.

## Building HTML Pages

To build the HTML documentation for this version only, you have two options:

**Option 1: Using globally installed Antora** (requires Node.js, Antora and Pandoc installed):
```bash
ant gen-doc
```
This uses the `site-local.yml` antora playbook to build only the current in-tree version.

**Option 2: Using Docker**:
```bash
.build/docker/build-docs.sh
```
This uses the same Docker image as cassandra-website, with all tooling pre-installed.

The HTML will be generated in `build/html/`.

For building documentation across multiple Cassandra versions, see the build instructions in the [cassandra-website](https://github.com/apache/cassandra-website) repo.


## Building Man Pages

Building the html documentation also generates a manpages file.

**View the man page**:
```bash
man ../build/man/cassandra-docs.7.gz
```

**Note**: This creates a single comprehensive reference manual (section 7) containing all documentation.


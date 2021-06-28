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
- Additional documentation on the SASI implementation (`SASI.md`). TODO: we
  should probably move the first half of that documentation to the general
  documentation, and the implementation explanation parts into the wiki.


Official documentation
----------------------

The source for the official documentation for Apache Cassandra can be found in
the `source` subdirectory. The documentation uses [antora](http://www.antora.org/)
and is thus written in [asciidoc](http://asciidoc.org).

To build the HTML documentation, you will need to first install antora and the
[Apache Cassandra theme](https://???). To add search, [lunr](https://lunrjs.com) must be installed.

```
npm i -g @antora/cli @antora/site-generator-default
npm install lunr
```

The documentation can then be built from this directory by calling `make html`
Alternatively, the top-level `ant gen-doc` target can be used.  

To build the documentation with Docker Compose, run:

```bash
cd ./doc

# build the Docker image
docker-compose build build-docs

# build the documentation
docker-compose run build-docs
```

To regenerate the documentation from scratch, run:

```bash
# return to the root directory of the Cassandra project
cd ..

# remove all generated documentation files based on the source code
ant realclean
```

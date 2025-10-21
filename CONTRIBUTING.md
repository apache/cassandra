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

# How to Contribute

There are many opportunities to contribute code to Apache Cassandra, including documentation updates, test improvements,
bug fixes, changes to the Java code base, and tooling improvements.

Before getting started, please read about [Contributing Code Changes](https://cassandra.apache.org/_/development/patches.html), and familiarize yourself with Cassandra's [code style guidelines](https://cassandra.apache.org/_/development/code_style.html), [testing](https://cassandra.apache.org/_/development/testing.html) and [code review checklist](https://cassandra.apache.org/_/development/how_to_review.html).

A recommended workflow to get started is:
1. Find or create an issue in the [Cassandra JIRA](https://issues.apache.org/jira/browse/CASSANDRA/) that describes the work you plan to do.
2. Create a personal fork of the [Apache Cassandra GitHub repo](https://github.com/apache/cassandra).
3. Clone your fork into your development environment.
4. Create your feature branch. Please see the branch naming suggestion, below.
5. Make, build, test and self-review your changes on your feature branch.
6. Submit the patch, either by creating a GitHub pull request, attaching a patch file to your JIRA, or posting a
link to a GitHub branch with your changes.

Branch naming: To ease collaboration, consider naming your feature branch like `your-name/jira-id/base-branch`. For
example, `jcshepherd/CASSANDRA-12345/trunk`. This convention will help with managing multiple collaborators on a shared
fork, and managing patches which differ across different Cassandra versions.

Note: [Committers](https://projects.apache.org/committee.html?cassandra) follow additional processes for handling patches, pull requests, and committing directly to [the official repo](https://gitbox.apache.org/), which the Apache Cassandra GitHub repo mirrors.

## Contributing to Related Projects

There are a number of [Cassandra-related projects](https://github.com/apache?q=cassandra&type=all&language=&sort=) where
contributions may be welcomed, including:
- [Cassandra Website](https://github.com/apache/cassandra-website)
- [Cassandra Sidecar](https://github.com/apache/cassandra-sidecar)
- [Cassandra Analytics](https://github.com/apache/cassandra-analytics)
- Cassandra drivers for [Java](https://github.com/apache/cassandra-java-driver) and [Go](https://github.com/apache/cassandra-gocql-driver)
- [Cassandra Spark Connector](https://github.com/apache/cassandra-spark-connector)
- [Cassandra DTests (distributed tests)](https://github.com/apache/cassandra-dtest)

... and [more](https://github.com/apache?q=cassandra&type=all&language=&sort=). Visit those repositories and their related
JIRA issues for more information on making contributions.

# Working with Submodules

Apache Cassandra uses git submodules for a set of dependencies, this is to make cross cutting changes easier for developers.  When working on such changes, there are a set of scripts to help with the process.

## Local Development

When starting a development branch, the following will change all submodules to a new branch based off the JIRA

```
$ .build/sh/development-switch.sh --jira CASSANDRA-<number>
```

When changes are made to a submodule (such as to accord), you need to commit and update the reference in Apache Cassandra

```
$ (cd modules/accord ; git commit -am 'Saving progress')
$ .build/sh/bump-accord.sh
```

## Commit and Merge Process

Due to the nature of submodules, the changes to the submodules must be committed and pushed before the changes to Apache Cassandra; these are different repositories so git's `--atomic` does not prevent conflicts from concurrent merges; the basic process is as follows:

* Follow the normal merge process for the submodule
* Update Apache Cassandra's submodule entry to point to the newly committed change; follow the Accord example below for an example

```
$ .build/sh/change-submodule-accord.sh
$ .build/sh/bump-accord.sh
```

# Useful Links

- How you can contribute to Apache Cassandra [presentation](http://www.slideshare.net/yukim/cassandrasummit2013) by Yuki Morishita
- Code style [wiki page](https://cwiki.apache.org/confluence/display/CASSANDRA2/CodeStyle)
- Running Cassandra in IDEA [guide](https://cwiki.apache.org/confluence/display/CASSANDRA2/RunningCassandraInIDEA)
- Running Cassandra in Eclipse [guide](https://cwiki.apache.org/confluence/display/CASSANDRA2/RunningCassandraInEclipse)
- Cassandra Cluster Manager - [CCM](https://github.com/pcmanus/ccm) and a guide [blog post](http://www.datastax.com/dev/blog/ccm-a-development-tool-for-creating-local-cassandra-clusters)
- Cassandra Distributed Tests aka [dtests](https://github.com/apache/cassandra-dtest)
- Cassandra Testing Guidelines - see TESTING.md

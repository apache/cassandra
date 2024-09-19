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

# Apache Cassandra and Pull Requests

Apache Cassandra doesn't use GitHub pull requests as part of the development process.
In fact, this repository is a GitHub mirror of [the official repo](https://gitbox.apache.org/repos/asf/cassandra.git).

# How to Contribute

Use [Cassandra JIRA](https://issues.apache.org/jira/browse/CASSANDRA/) to create an issue, then either attach a patch or post a link to a GitHub branch with your changes.

# Working with submodules

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

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

### Install ANT
1. cd into the directory where you usually store software distributions. We are going to download ANT from there. Say it's `~/my_softwares`, run `cd ~/my_softwares`.
2. Run the following, copy the ANT_HOME path in the last line of the output:
   ```
   wget http://artifactory.uber.internal:4587/artifactory/libs-release-local/org/apache/ant/apache-ant-1.10.12-bin.tar.gz
   tar xzvf apache-ant-1.10.12-bin.tar.gz
   ANT_HOME="$(readlink -f apache-ant-1.10.12)"
   echo "ANT_HOME is $ANT_HOME"
   ```
3. Add ANT_HOME in your `~/.zshrc` file
   ```
   # Set ANT_HOME
   ANT_HOME="<what you copied in step 2>"
   export ANT_HOME

   # Update PATH
   export PATH="$ANT_HOME/bin:$PATH"
   ```
4. `source ~/.zshrc` or restart your terminals
(This instruction actually comes from .jenkins/release.sh. Please update the instruction if you observe any changes in .jenkins/release.sh, for example the version changes. Thanks.)

### How to resolve the imports in IntelliJ
1. `ant generate-idea-files`
2. In the IntelliJ's project tree, find this directory, `cassandra/src/java`, right click it and "Mark Directory as" -> "Sources Root".

### Starting a Cassandra node locally
1. Turn on VPN
2. Build the jar file locally by running `ant jar` at the root folder of this repo.
   If the command succeeds, you shall see in the output a full path to a jar file that has just been built. Sample output:
   ```
   _main-jar:
      [jar] Building jar: /Users/panl/cassandra_ucs/cassandra/build/apache-cassandra-4.123.456.789-SNAPSHOT.jar
   ```
3. Download an **arbitrary** (meaning the version doesn't matter) Cassandra artifact from [here](https://artifactory-dca1.uberinternal.com/artifactory/webapp/#/artifacts/browse/tree/General/libs-release-local-dca1/org/apache/cassandra/apache-cassandra).
   All you need to download is the tar.gz file, e.g. apache-cassandra/3.0.25.1/apache-cassandra-4.0.6.5-bin.tar.gz
4. Untar the tar.gz file
5. cd into it. For example: `% cd apache-cassandra-4.0.6.5`
6. cd in the _lib_ folder. For example: `% cd lib`
7. In thie folder there are many jar files. Find the one whose name goes like "apache-cassandra-xxx.jar". For example:
   ```
   % ls | grep apache-cassandra
   apache-cassandra-4.0.6.5.jar
   ```
8. Here comes the most important step: we need to change the jar file in step 6 into a soft-link pointing to
   the jar file we created in step 2. For example:
   ```
   rm -rf apache-cassandra-4.0.6.5.jar
   ln -s <full_jar_path_in_step2> apache-cassandra-4.0.6.5.jar
   ```

9. Run `arch` in your terminal. If you're running with M1/M2 architecture(arch returns arm64), you'll need JNA > 5.8 to start Cassandra 4.0 node. 
   Goto the folder of step 5 and do `ls lib | grep jna` to check the JNA jar (e.g. jna-5.6.0.jar).
   Download source code from [JNA gitrepo](https://github.com/java-native-access/jna). For example, jna-5.8.0.tar.gz. Untar the tar.gz file, cd into it and run `ant jar` to build the jar file.
   You should see 
   ```
   [jar] Building jar: /Users/yuqiy/Downloads/jna-5.8.0/build/jna.jar
   ```
   Remove the original JNA jar `rm lib/jna-5.6.0.jar` and replace it with the new built jar from above (you can either rename it or keep it as jna.jar).

10. Now we are good to start the Cassandra node. Go to the folder of step 5.
   Run `bin/cassandra` to start the Cassandra process locally.
   If you are unsure whether there is a Cassandra process already running locally, run `ps -ef|grep org.apache.cassandra.service.CassandraDaemon  |grep -v grep |awk '{print }' |xargs kill -9` to kill it.

11. Now with a running Cassandra node, you can do all regular tests in another terminal:
    for nodetool, run `bin/nodetool`; for cqlsh, run `bin/cqlsh`


### Test a multi-node Cassandra locally
1. Setup your local `~/.pip/pip.conf` by copy-pasting the content from: https://sourcegraph.uberinternal.com/code.uber.internal/uber-developer/-/blob/pip.conf
2. pip install ccm
3. Go to your Cassandra source code, e.g., `cd /Users/abc/cassandra_code/`
4. Build your code: `ant realclean`, then `ant jar`
5. (MacOS only) Depending on the number of nodes you need to add, run the folling commands to add the loopback ips
   ```
   sudo ifconfig lo0 alias 127.0.0.2
   sudo ifconfig lo0 alias 127.0.0.3
   ...
   ```
6. Start a three-node Cassandra cluster as follows: `ccm create test1  -n 3 -s`
7. See the three nodes ring as: `ccm node1 ring`
8. Cqlsh as follows: `ccm node1 cqlsh  --python /usr/local/bin/python3.10`
9. Cassandra container's data, etc., can be found at: `cd ~/.ccm/test1/`

Type `ccm` to get more help on various commands. More details here: https://github.com/riptano/ccm#installation

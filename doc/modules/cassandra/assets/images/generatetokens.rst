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

generatetokens
------------

Pre-generates tokens for a datacenter with the given number of nodes using the token allocation algorithm. Useful in edge-cases when generated tokens needs to be known in advance of bootstrapping nodes. In nearly all cases it is best to just let the bootstrapping nodes automatically generate their own tokens.
ref: https://issues.apache.org/jira/browse/CASSANDRA-16205


Usage
^^^^^
generatetokens -n NODES -t TOKENS --rf REPLICATION_FACTOR [--partitioner PARTITIONER] [--racks RACK_NODE_COUNTS]


===================================                   ================================================================================
    -n,--nodes <arg>                                  Number of nodes.
    -t,--tokens <arg>                                 Number of tokens/vnodes per node.
    --rf <arg>                                        Replication factor.
    -p,--partitioner <arg>                            Database partitioner, either Murmur3Partitioner or RandomPartitioner.
    --racks <arg>                                     Number of nodes per rack, separated by commas. Must add up to the total node count. For example, 'generatetokens -n 30 -t 8 --rf 3 --racks 10,10,10' will generate tokens for three racks of 10 nodes each.
===================================                   ================================================================================


This command, if used, is expected to be run before the Cassandra node is first started. The output from the command is used to configure the nodes `num_tokens` setting in the `cassandra.yaml`


Example Output
^^^^^^^^^^^^^^
Example usage and output is

    $ tools/bin/generatetokens -n 9 -t 4 --rf 3 --racks 3,3,3

    Generating tokens for 9 nodes with 4 vnodes each for replication factor 3 and partitioner Murmur3Partitioner
    Node 0 rack 0: [-6270077235120413733, -1459727275878514299, 2887564907718879562, 5778609289102954400]
    Node 1 rack 1: [-8780789453057732897, -3279530982831298765, 1242905755717369197, 8125606440590916903]
    Node 2 rack 2: [-7240625170832344686, -4453155190605073029, 74749827226930055, 4615117940688406403]
    Node 3 rack 0: [-5361616212862743381, -2369629129354906532, 2065235331718124379, 6952107864846935651]
    Node 4 rack 1: [-8010707311945038792, -692488724325792122, 3751341424203642982, 7538857152718926277]
    Node 5 rack 2: [-7625666241388691739, -3866343086718185897, 5196863614895680401, 8895780530621367810]
    Node 6 rack 0: [-5815846723991578557, -1076108000102153211, 1654070543717746788, 8510693485606142356]
    Node 7 rack 1: [-2824580056093102649, 658827791472149626, 3319453165961261272, 6365358576974945025]
    Node 8 rack 2: [-4159749138661629463, -1914678202616710416, 4905990777792043402, 6658733220910940338]
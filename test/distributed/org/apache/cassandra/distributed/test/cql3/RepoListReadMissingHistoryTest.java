/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test.cql3;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class RepoListReadMissingHistoryTest extends TestBaseImpl
{
    @Test
    public void test() throws Throwable
    {
        try (Cluster cluster = Cluster.create(3))
        {
            cluster.schemaChange("\t\tCREATE KEYSPACE IF NOT EXISTS ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};\n");
            cluster.schemaChange("\t\tCREATE TABLE ks1.tbl (\n" +
                                 "\t\t    pk0 int,\n" +
                                 "\t\t    pk1 tinyint,\n" +
                                 "\t\t    ck0 int,\n" +
                                 "\t\t    s0 frozen<list<frozen<list<int>>>> static,\n" +
                                 "\t\t    s1 list<frozen<list<int>>> static,\n" +
                                 "\t\t    v1 frozen<list<frozen<list<int>>>>,\n" +
                                 "\t\t    v0 list<frozen<list<int>>>,\n" +
                                 "\t\t    PRIMARY KEY ((pk0, pk1), ck0)\n" +
                                 "\t\t) WITH CLUSTERING ORDER BY (ck0 DESC)\n" +
                                 "\t\t    AND transactional_mode = 'full'\n" +
                                 "\t\t    AND speculative_retry = '99p';\n");
            ClusterUtils.awaitAccordEpochReady(cluster, ClusterUtils.waitForCMSToQuiesce(cluster).getEpoch());

            cluster.coordinator(2).execute("BEGIN TRANSACTION \n" +
                                           "\tINSERT INTO ks1.tbl (pk0, pk1, ck0, v1, s1, v0, s0) VALUES (396322556, 40, -1748133413 - -1024523175, [[665805949], [-783123743, 2047305302], [714248282]], [[103141047]], [[-1283821483, -1019535361], [-106957965, -457242862, 1983685136]], [[-1708678893, -1627160677], [-1342370394], [-2105454556, 1159184135, 2055191601]]); \n" +
                                           "COMMIT TRANSACTION", ConsistencyLevel.ALL);
            cluster.coordinator(1).execute("UPDATE ks1.tbl SET s1[10]=[-310220905], v1=[[875071167, -1953008271]] WHERE  pk0 = 396322556 AND  pk1 = 40 AND  ck0 = 1033194523 - 1803482217", ConsistencyLevel.ALL);
        }
    }
}

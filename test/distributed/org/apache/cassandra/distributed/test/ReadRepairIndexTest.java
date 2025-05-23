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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.distributed.shared.AssertUtils.row;

@RunWith(Parameterized.class)
public class ReadRepairIndexTest extends TestBaseImpl
{
    private static final int NUM_NODES = 2;

    enum IndexType
    {
        SECONDARY(CassandraIndex.NAME),
        SAI(StorageAttachedIndex.NAME);

        final String name;

        IndexType(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    /**
     * The read repair strategy to be used
     */
    @Parameterized.Parameter
    public ReadRepairStrategy strategy;

    /**
     * The node to be used as coordinator
     */
    @Parameterized.Parameter(1)
    public int coordinator;

    /**
     * Whether to flush data after mutations
     */
    @Parameterized.Parameter(2)
    public boolean flush;

    /**
     * Whether paging is used for the distributed queries
     */
    @Parameterized.Parameter(3)
    public boolean paging;

    @Parameterized.Parameter(4)
    public ReplicationType replicationType;

    @SuppressWarnings("ClassEscapesDefinedScope")
    @Parameterized.Parameter(5)
    public IndexType indexType;


    @Parameterized.Parameters(name = "{index}: strategy={0} coordinator={1} flush={2} paging={3} replication={4} index={5}")
    public static Collection<Object[]> data()
    {
        List<Object[]> result = new ArrayList<>();
        for (int coordinator = 1; coordinator <= NUM_NODES; coordinator++)
            for (boolean flush : BOOLEANS)
                for (boolean paging : BOOLEANS)
                    for (ReplicationType replication : ReplicationType.values())
                        for (IndexType indexType : IndexType.values())
                            result.add(new Object[]{ ReadRepairStrategy.BLOCKING, coordinator, flush, paging, replication, indexType});
        return result;
    }

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = Cluster.build(NUM_NODES)
                              .withConfig(config -> config.set("read_request_timeout", "1m")
                                                          .set("write_request_timeout", "1m"))
                              .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    protected Tester tester(String restriction)
    {
        return new Tester(restriction, cluster, strategy, coordinator, flush, paging, replicationType, indexType);
    }

    protected static class Tester extends ReadRepairQueryTester.AbstractTester<Tester>
    {
        private int nameSeq = 0;
        private final IndexType indexType;

        @SuppressWarnings("ClassEscapesDefinedScope")
        public Tester(String restriction, Cluster cluster, ReadRepairStrategy strategy, int coordinator, boolean flush, boolean paging, ReplicationType replicationType, IndexType indexType)
        {
            super(restriction, cluster, strategy, coordinator, flush, paging, replicationType);
            this.indexType = indexType;
        }

        @Override
        Tester self()
        {
            return this;
        }

        Tester createIndex(String column)
        {
            String query = String.format("CREATE INDEX %s_index_%d ON %s(%s) USING '%s'", tableName, nameSeq++, qualifiedTableName, column, indexType);
            cluster.schemaChange(query);
            return this;
        }
    }

    /**
     * A partition that would not be an index hit on one node would be on the other
     */
    @Test
    public void singlePartitionUpdatedPartition()
    {
        tester("WHERE k=1 AND v=2")
        .createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))")
        .createIndex("v")
        .mutate(2, "INSERT INTO %s (k, c, v) VALUES (1, 2, 2)")
        .mutate(1, "INSERT INTO %s (k, c, v) VALUES (1, 1, 1)")
        .queryColumns("k, c, v", 1, 0,
                      rows(row(1, 2, 2)),
                      rows(row(1, 2, 2)),
                      rows(row(1, 2, 2)))
        .tearDown(1,
                  rows(row(1, 1, 1), row(1, 2, 2)),
                  rows(row(1, 2, 2)));

    }
    @Test
    public void rangeReadTest()
    {
        tester("WHERE v=2")
        .createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k))")
        .createIndex("v")
        .mutate(2, "INSERT INTO %s (k, v) VALUES (1, 2)")
        .mutate(1, "INSERT INTO %s (k, v) VALUES (2, 1)")
        .mutate(2, "INSERT INTO %s (k, v) VALUES (3, 1)")
        .mutate(1, "INSERT INTO %s (k, v) VALUES (4, 2)")
        .queryColumns("k, v", 2, 0,
                      rows(row(1, 2), row(4, 2)),
                      rows(row(1, 2), row(4, 2)),
                      rows(row(1, 2), row(4, 2)))
        .tearDown(2,
                  rows(row(1, 2), row(2, 1), row(4, 2), row(3, 1)),
                  (replicationType.isTracked()
                   ? rows(row(1, 2), row(2, 1), row(4, 2), row(3, 1))
                   : rows(row(1, 2), row(2, 1), row(4, 2))),
                  (replicationType.isTracked()
                   ? rows(row(1, 2), row(2, 1), row(4, 2), row(3, 1))
                   : rows(row(1, 2), row(4, 2), row(3, 1))));
    }

    @Test
    public void sortedRangeRead()
    {
        Assume.assumeTrue("CassandraIndex doesn't support numerical ranges", indexType == IndexType.SAI);

        tester("WHERE v>2")
        .createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))")
        .createIndex("v")
        .mutate(2, "INSERT INTO %s (k, c, v) VALUES (1, 2, 2)")
        .mutate(1, "INSERT INTO %s (k, c, v) VALUES (1, 4, 4)")
        .mutate(2, "INSERT INTO %s (k, c, v) VALUES (5, 2, 1)")
        .mutate(1, "INSERT INTO %s (k, c, v) VALUES (8, 4, 3)")
        .queryColumns("k, c, v", 2, 0,
                      rows(row(1, 4, 4), row(8, 4, 3)),
                      rows(row(1, 4, 4), row(8, 4, 3)),
                      rows(row(1, 4, 4), row(8, 4, 3)))
        .tearDown(2,
                  rows(row(5, 2, 1), row(1, 2, 2), row(1, 4, 4), row(8, 4, 3)),
                  replicationType.isTracked()
                    ? rows(row(5, 2, 1), row(1, 2, 2), row(1, 4, 4), row(8, 4, 3))
                    : rows(row(1, 4, 4), row(8, 4, 3)),
                  rows(row(5, 2, 1), row(1, 2, 2), row(1, 4, 4), row(8, 4, 3)));

    }
}

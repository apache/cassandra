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
package org.apache.cassandra.cql3.validation.entities;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.triggers.ITrigger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class VirtualTableTest extends CQLTester
{
    private static final String KS_NAME = "test_virtual_ks";
    private static final String VT1_NAME = "vt1";
    private static final String VT2_NAME = "vt2";

    private static class WritableVirtualTable extends AbstractVirtualTable
    {
        private final ColumnMetadata valueColumn;
        private final Map<String, Integer> backingMap = new HashMap<>();

        WritableVirtualTable(String keyspaceName, String tableName)
        {
            super(TableMetadata.builder(keyspaceName, tableName)
                               .kind(TableMetadata.Kind.VIRTUAL)
                               .addPartitionKeyColumn("key", UTF8Type.instance)
                               .addRegularColumn("value", Int32Type.instance)
                               .build());
            valueColumn = metadata().regularColumns().getSimple(0);
        }

        @Override
        public DataSet data()
        {
            SimpleDataSet data = new SimpleDataSet(metadata());
            backingMap.forEach((key, value) -> data.row(key).column("value", value));
            return data;
        }

        @Override
        public void apply(PartitionUpdate update)
        {
            String key = (String) metadata().partitionKeyType.compose(update.partitionKey().getKey());
            update.forEach(row ->
            {
                Integer value = Int32Type.instance.compose(row.getCell(valueColumn).value());
                backingMap.put(key, value);
            });
        }
    }

    @BeforeClass
    public static void setUpClass()
    {
        TableMetadata vt1Metadata =
            TableMetadata.builder(KS_NAME, VT1_NAME)
                         .kind(TableMetadata.Kind.VIRTUAL)
                         .addPartitionKeyColumn("pk", UTF8Type.instance)
                         .addClusteringColumn("c", UTF8Type.instance)
                         .addRegularColumn("v1", Int32Type.instance)
                         .addRegularColumn("v2", LongType.instance)
                         .build();

        SimpleDataSet vt1data = new SimpleDataSet(vt1Metadata);

        vt1data.row("pk1", "c1").column("v1", 11).column("v2", 11L)
               .row("pk2", "c1").column("v1", 21).column("v2", 21L)
               .row("pk1", "c2").column("v1", 12).column("v2", 12L)
               .row("pk2", "c2").column("v1", 22).column("v2", 22L)
               .row("pk1", "c3").column("v1", 13).column("v2", 13L)
               .row("pk2", "c3").column("v1", 23).column("v2", 23L);

        VirtualTable vt1 = new AbstractVirtualTable(vt1Metadata)
        {
            public DataSet data()
            {
                return vt1data;
            }
        };
        VirtualTable vt2 = new WritableVirtualTable(KS_NAME, VT2_NAME);

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(vt1, vt2)));

        CQLTester.setUpClass();
    }

    @Test
    public void testQueries() throws Throwable
    {
        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt1 WHERE pk = 'UNKNOWN'"));

        assertRowsNet(executeNet("SELECT * FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c = 'UNKNOWN'"));

        // Test DISTINCT query
        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks.vt1"),
                      row("pk1"),
                      row("pk2"));

        assertRowsNet(executeNet("SELECT DISTINCT pk FROM test_virtual_ks.vt1 WHERE token(pk) > token('pk1')"),
                      row("pk2"));

        // Test single partition queries
        assertRowsNet(executeNet("SELECT v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c = 'c1'"),
                      row(11, 11L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c IN ('c1', 'c2')"),
                      row("c1", 11, 11L),
                      row("c2", 12, 12L));

        assertRowsNet(executeNet("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk = 'pk1' AND c IN ('c2', 'c1') ORDER BY c DESC"),
                      row("c2", 12, 12L),
                      row("c1", 11, 11L));

        // Test multi-partition queries
        assertRows(execute("SELECT * FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row("pk1", "c1", 11, 11L),
                   row("pk1", "c2", 12, 12L),
                   row("pk2", "c1", 21, 21L),
                   row("pk2", "c2", 22, 22L));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC"),
                   row("pk1", "c2", 12),
                   row("pk2", "c2", 22),
                   row("pk1", "c1", 11),
                   row("pk2", "c1", 21));

        assertRows(execute("SELECT pk, c, v1 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') ORDER BY c DESC LIMIT 1"),
                   row("pk1", "c2", 12));

        assertRows(execute("SELECT c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1' , 'c3') ORDER BY c DESC PER PARTITION LIMIT 1"),
                   row("c3", 13, 13L),
                   row("c3", 23, 23L));

        assertRows(execute("SELECT count(*) FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')"),
                   row(4L));

        for (int pageSize = 1; pageSize < 5; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT pk, c, v1, v2 FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c1", 21, 21L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1') LIMIT 2", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks.vt1 WHERE pk IN ('pk2', 'pk1') AND c IN ('c2', 'c1')", pageSize),
                          row(4L));
        }

        // Test range queries
        for (int pageSize = 1; pageSize < 4; pageSize++)
        {
            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L),
                          row("pk1", "c2", 12, 12L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) < token('pk2') AND c IN ('c2', 'c1') LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c1", 11, 11L));

            assertRowsNet(executeNetWithPaging("SELECT * FROM test_virtual_ks.vt1 WHERE token(pk) <= token('pk2') AND c > 'c1' PER PARTITION LIMIT 1 ALLOW FILTERING", pageSize),
                          row("pk1", "c2", 12, 12L),
                          row("pk2", "c2", 22, 22L));

            assertRowsNet(executeNetWithPaging("SELECT count(*) FROM test_virtual_ks.vt1 WHERE token(pk) = token('pk2') AND c < 'c3' ALLOW FILTERING", pageSize),
                          row(2L));
        }
    }

    @Test
    public void testModifications() throws Throwable
    {
        // check for clean state
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"));

        // fill the table, test UNLOGGED batch
        execute("BEGIN UNLOGGED BATCH " +
                "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1';" +
                "UPDATE test_virtual_ks.vt2 SET value = 2 WHERE key ='pk2';" +
                "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3';" +
                "APPLY BATCH");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2"),
                   row("pk1", 1),
                   row("pk2", 2),
                   row("pk3", 3));

        // test that LOGGED batches don't allow virtual table updates
        assertInvalidMessage("Cannot include a virtual table statement in a logged batch",
                             "BEGIN BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1';" +
                             "UPDATE test_virtual_ks.vt2 SET value = 2 WHERE key ='pk2';" +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3';" +
                             "APPLY BATCH");

        // test that UNLOGGED batch doesn't allow mixing updates for regular and virtual tables
        createTable("CREATE TABLE %s (key text PRIMARY KEY, value int)");
        assertInvalidMessage("Mutations for virtual and regular tables cannot exist in the same batch",
                             "BEGIN UNLOGGED BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 1 WHERE key ='pk1'" +
                             "UPDATE %s                  SET value = 2 WHERE key ='pk2'" +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3'" +
                             "APPLY BATCH");

        // update a single value with UPDATE
        execute("UPDATE test_virtual_ks.vt2 SET value = 11 WHERE key ='pk1'");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE key = 'pk1'"),
                   row("pk1", 11));

        // update a single value with INSERT
        executeNet("INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk2', 22)");
        assertRows(execute("SELECT * FROM test_virtual_ks.vt2 WHERE key = 'pk2'"),
                   row("pk2", 22));

        // test that deletions are (currently) rejected
        assertInvalidMessage("Virtual tables don't support DELETE statements",
                             "DELETE FROM test_virtual_ks.vt2 WHERE key ='pk1'");

        // test that TTL is (currently) rejected with INSERT and UPDATE
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                             "INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk1', 11) USING TTL 86400");
        assertInvalidMessage("Expiring columns are not supported by virtual tables",
                             "UPDATE test_virtual_ks.vt2 USING TTL 86400 SET value = 11 WHERE key ='pk1'");

        // test that LWT is (currently) rejected with virtual tables in batches
        assertInvalidMessage("Conditional BATCH statements cannot include mutations for virtual tables",
                             "BEGIN UNLOGGED BATCH " +
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3' IF value = 2;" +
                             "APPLY BATCH");

        // test that LWT is (currently) rejected with virtual tables in UPDATEs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                             "UPDATE test_virtual_ks.vt2 SET value = 3 WHERE key ='pk3' IF value = 2");

        // test that LWT is (currently) rejected with virtual tables in INSERTs
        assertInvalidMessage("Conditional updates are not supported by virtual tables",
                             "INSERT INTO test_virtual_ks.vt2 (key, value) VALUES ('pk2', 22) IF NOT EXISTS");
    }

    @Test
    public void testInvalidDDLOperations() throws Throwable
    {
        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "DROP KEYSPACE test_virtual_ks");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "ALTER KEYSPACE test_virtual_ks WITH durable_writes = false");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TABLE test_virtual_ks.test (id int PRIMARY KEY)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TYPE test_virtual_ks.type (id int)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "DROP TABLE test_virtual_ks.vt1");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "ALTER TABLE test_virtual_ks.vt1 DROP v1");

        assertInvalidMessage("Error during truncate: Cannot truncate virtual tables",
                             "TRUNCATE TABLE test_virtual_ks.vt1");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE INDEX ON test_virtual_ks.vt1 (v1)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE MATERIALIZED VIEW test_virtual_ks.mvt1 AS SELECT c, v1 FROM test_virtual_ks.vt1 WHERE c IS NOT NULL PRIMARY KEY(c)");

        assertInvalidMessage("Virtual keyspace 'test_virtual_ks' is not user-modifiable",
                             "CREATE TRIGGER test_trigger ON test_virtual_ks.vt1 USING '" + TestTrigger.class.getName() + '\'');
    }

    /**
     * Noop trigger for audit log testing
     */
    public static class TestTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition update)
        {
            return null;
        }
    }

    @Test
    public void testMBeansMethods() throws Throwable
    {
        StorageServiceMBean mbean = StorageService.instance;

        assertJMXFails(() -> mbean.forceKeyspaceCompaction(false, KS_NAME));
        assertJMXFails(() -> mbean.forceKeyspaceCompaction(false, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.scrub(true, true, true, true, 1, KS_NAME));
        assertJMXFails(() -> mbean.scrub(true, true, true, true, 1, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.verify(true, KS_NAME));
        assertJMXFails(() -> mbean.verify(true, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.upgradeSSTables(KS_NAME, false, 1));
        assertJMXFails(() -> mbean.upgradeSSTables(KS_NAME, false, 1, VT1_NAME));

        assertJMXFails(() -> mbean.garbageCollect("ROW", 1, KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.forceKeyspaceFlush(KS_NAME));
        assertJMXFails(() -> mbean.forceKeyspaceFlush(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.truncate(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.loadNewSSTables(KS_NAME, VT1_NAME));

        assertJMXFails(() -> mbean.getAutoCompactionStatus(KS_NAME));
        assertJMXFails(() -> mbean.getAutoCompactionStatus(KS_NAME, VT1_NAME));
    }

    @FunctionalInterface
    private static interface ThrowingRunnable
    {
        public void run() throws Throwable;
    }

    private void assertJMXFails(ThrowingRunnable r) throws Throwable
    {
        try
        {
            r.run();
            fail();
        }
        catch (IllegalArgumentException e)
        {
            assertEquals("Cannot perform any operations against virtual keyspace " + KS_NAME, e.getMessage());
        }
    }
}

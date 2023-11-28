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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.schema.Schema;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

/**
 * SAI queries, like all filtering queries, must correctly resolve divergent views of row data across replicas. In
 * particular, cases where writes do not propagate to all replicas can, if not resolved correctly, lead to consistency
 * violations, as rows that should not appear in our results do, while rows that should appear do not.
 * <p> 
 * The variables that affect the behavior of these queries are, at minimum, the following:
 * <p> 
 * 1.) The combination of write & read consistency levels used by the client. Reads at ONE/LOCAL_ONE trivially
 *     avoid having to resolve data from diverging replicas.
 * 2.) Interaction of existing data (if there is any), with partial updates and deletes. The same query that
 *     erroneously returns stale matches with naïve resolution and partial updates on top of existing data might 
 *     fail to return live matches with partial updates and no previous data.
 * 3.) The number of query clauses and their targets. Clauses may target partition keys, clustering keys, static 
 *     columns, and regular columns, and some combinations are more problematic than others.
 * 4.) The repaired state of SSTables that participate in the query. A fully repaired on-disk set of SSTables cannot
 *     produce erroneous results due to split rows (i.e. rows which contain partial updates of different columns
 *     across different partitions).
 * 5.) Whether data resides in SSTables or Memtables. The latter is implicitly unrepaired.
 * 6.) Interaction w/ existing mechanisms on the distributed read path that deal with short reads, replica filtering
 *     protection, etc.
 * 7.) The relationship between columns selected and columns restricted by queries. (If coordinator filtering is
 *     involved at the implementation level, retrieving enough information to do that filtering is important.)
 * 8.) The timestamps of partial updates and deletes, especially for single-column queries that might produce
 *     stale matches if not resolved correctly.
 */
@RunWith(Parameterized.class)
public class PartialUpdateHandlingTest extends TestBaseImpl
{
    private static final String TEST_TABLE_NAME = "test_partial_updates";

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(3).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());

        // All parameterized test scenarios share the same table and attached indexes, but write to different partitions
        // that are deleted after each scenario completes.
        String createTableDDL = String.format("CREATE TABLE %s.%s (pk int, ck int, s int static, a int, b int, PRIMARY KEY (pk, ck)) WITH read_repair = 'NONE'",
                                              KEYSPACE, TEST_TABLE_NAME);
        CLUSTER.schemaChange(createTableDDL);
        CLUSTER.disableAutoCompaction(KEYSPACE);

        CLUSTER.schemaChange(String.format("CREATE INDEX ck_idx ON %s.%s(ck) USING 'sai'", KEYSPACE, TEST_TABLE_NAME));
        CLUSTER.schemaChange(String.format("CREATE INDEX s_idx ON %s.%s(s) USING 'sai'", KEYSPACE, TEST_TABLE_NAME));
        CLUSTER.schemaChange(String.format("CREATE INDEX a_idx ON %s.%s(a) USING 'sai'", KEYSPACE, TEST_TABLE_NAME));
        CLUSTER.schemaChange(String.format("CREATE INDEX b_idx ON %s.%s(b) USING 'sai'", KEYSPACE, TEST_TABLE_NAME));

        SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
    }
    
    static class Specification
    {
        final boolean restrictPartitionKey;
        final String[] columns;
        final ConsistencyLevel readCL;
        final boolean existing;
        final StatementType partialUpdateType;
        final int partitionKey;
        final boolean flushPartials;

        Specification(boolean restrictPartitionKey,
                      String[] columns,
                      ConsistencyLevel readCL,
                      boolean existing,
                      StatementType partialUpdateType,
                      int partitionKey,
                      boolean flushPartials)
        {
            this.restrictPartitionKey = restrictPartitionKey;
            this.columns = columns;
            this.readCL = readCL;
            this.existing = existing;
            this.partialUpdateType = partialUpdateType;
            this.partitionKey = partitionKey;
            this.flushPartials = flushPartials;
        }
        
        public Object[] nonKeyColumns()
        {
            return Arrays.stream(columns).filter(c -> !c.equals("ck") && !c.equals("pk")).toArray();
        }

        public String tableName()
        {
            return TEST_TABLE_NAME;
        }

        @Override
        public String toString()
        {
            return "restrictPartitionKey=" + restrictPartitionKey +
                   ", columns=" + Arrays.toString(columns) +
                   ", existing=" + existing +
                   ", readCL=" + readCL +
                   ", partialUpdateType=" + partialUpdateType +
                   ", partitionKey=" + partitionKey +
                   ", flushPartials=" + flushPartials;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Specification that = (Specification) o;
            return Arrays.equals(columns, that.columns) 
                   && readCL == that.readCL && existing == that.existing && restrictPartitionKey == that.restrictPartitionKey 
                   && partialUpdateType == that.partialUpdateType && partitionKey == that.partitionKey && flushPartials == that.flushPartials;
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash(readCL, existing, restrictPartitionKey, partialUpdateType, partitionKey, flushPartials);
            result = 31 * result + Arrays.hashCode(columns);
            return result < 0 ? -result : result;
        }
    }
    
    static class Model
    {
        final Specification specification;

        final Map<String, Integer> previousCells = new HashMap<>();
        final Map<String, Integer> currentCells = new HashMap<>();

        private int nextCellValue = 1;

        Model(Specification specification)
        {
            this.specification = specification;
        }

        public void writeAndRepairRow()
        {
            StringBuilder insert = new StringBuilder("INSERT INTO ").append(KEYSPACE).append('.').append(specification.tableName());
            insert.append("(pk, ck");

            for (Object column : specification.nonKeyColumns())
                insert.append(", ").append(column);

            insert.append(") VALUES (").append(specification.partitionKey).append(", 0");
            currentCells.put("pk", specification.partitionKey);
            currentCells.put("ck", 0);

            for (Object column : specification.nonKeyColumns())
            {
                int value = nextCellValue++;
                currentCells.put((String) column, value);
                insert.append(", ").append(value);
            }

            insert.append(')');

            CLUSTER.coordinator(1).execute(insert.toString(), ConsistencyLevel.ALL);
            CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        }

        public void updatePartially()
        {
            previousCells.putAll(currentCells);

            int node = 1;
            currentCells.put("pk", specification.partitionKey);
            currentCells.put("ck", 0);
            
            for (Object column : specification.nonKeyColumns())
            {
                if (specification.partialUpdateType == StatementType.UPDATE)
                    node = updateReplica(node, column);
                else if (specification.partialUpdateType == StatementType.DELETE)
                    node = deleteReplica(node, column);
                else
                    throw new IllegalStateException("Partial updates must be UPDATE or DELETE!");
            }
        }

        private int updateReplica(int node, Object column)
        {
            int value = nextCellValue++;
            currentCells.put((String) column, value);
            String dml = String.format("INSERT INTO %s.%s(pk, ck, %s) VALUES (?, 0, ?)", KEYSPACE, specification.tableName(), column);
            CLUSTER.get(node++).executeInternal(dml, specification.partitionKey, value);

            if (specification.readCL == QUORUM)
                CLUSTER.get(node).executeInternal(dml, specification.partitionKey, value);
            return node;
        }

        private int deleteReplica(int node, Object column)
        {
            currentCells.remove((String) column);
            String dml = String.format("DELETE %s FROM %s.%s WHERE pk = %d and ck = 0", 
                                       column, KEYSPACE, specification.tableName(), specification.partitionKey);

            if (column.equals("s"))
                dml = String.format("DELETE %s FROM %s.%s WHERE pk = %d", 
                                    column, KEYSPACE, specification.tableName(), specification.partitionKey);

            CLUSTER.get(node++).executeInternal(dml);

            if (specification.readCL == QUORUM)
                CLUSTER.get(node).executeInternal(dml);
            return node;
        }

        public void validateCurrent()
        {
            Object[][] result = validateModel(currentCells);

            // Hard-code the partition key and clustering key:
            List<Object> expectedRow = Lists.newArrayList(specification.partitionKey, 0);

            for (Object column : specification.nonKeyColumns())
                expectedRow.add(currentCells.get((String) column));

            assertRows(result, expectedRow.toArray());
        }
        
        public void validatePrevious()
        {
            // Ensure queries against the previous version of the row no longer match.
            assertRows(validateModel(previousCells));
        }

        private Object[][] validateModel(Map<String, Integer> cells)
        {
            StringBuilder select = new StringBuilder("SELECT pk, ck");

            for (Object column : specification.nonKeyColumns())
                select.append(", ").append(column);

            select.append(" FROM ").append(KEYSPACE).append('.').append(specification.tableName()).append(" WHERE ");

            ArrayList<String> restricted = Lists.newArrayList(specification.columns);
            if (specification.restrictPartitionKey)
                restricted.add("pk");

            List<String> clauses = restricted.stream().map(c -> c + " = " + cells.get(c)).collect(Collectors.toList());
            select.append(String.join(" AND ", clauses));

            if (specification.readCL == QUORUM)
            {
                // Make sure node 2, which is the only complete replica, can't participate in a query...
                CLUSTER.get(2).runOnInstance(() -> {
                    Keyspace keyspace = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE));
                    SecondaryIndexManager sim = keyspace.getColumnFamilyStore(TEST_TABLE_NAME).indexManager;
                    for (String s : Arrays.asList("ck_idx", "s_idx", "a_idx", "b_idx"))
                        sim.makeIndexNonQueryable(sim.getIndexByName(s), Index.Status.BUILD_FAILED);
                });

                SAIUtil.waitForIndexNonQueryable(CLUSTER, KEYSPACE, 2);
            }

            Object[][] result = CLUSTER.coordinator(1).execute(select.toString(), specification.readCL);

            if (specification.readCL == QUORUM)
            {
                CLUSTER.get(2).runOnInstance(() -> {
                    Keyspace keyspace = Objects.requireNonNull(Schema.instance.getKeyspaceInstance(KEYSPACE));
                    SecondaryIndexManager sim = keyspace.getColumnFamilyStore(TEST_TABLE_NAME).indexManager;
                    for (String s : Arrays.asList("ck_idx", "s_idx", "a_idx", "b_idx"))
                        sim.makeIndexQueryable(sim.getIndexByName(s), Index.Status.BUILD_SUCCEEDED);
                });

                SAIUtil.waitForIndexQueryable(CLUSTER, KEYSPACE);
            }

            return result;
        }
    }

    @Parameterized.Parameter
    public Specification specification;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        
        // Each test scenario operates over a different partition key.
        int partitionKey = 0;

        for (ConsistencyLevel readCL : new ConsistencyLevel[] { ALL, QUORUM })
        {
            for (boolean restrictPartitionKey : new boolean[] { false, true })
            {
                for (boolean flushPartials : new boolean[] { false, true })
                {
                    for (String[] columns : new String[][] { { "ck", "a" }, { "ck", "s" }, { "s", "a" }, { "a", "b" }, { "a" } })
                        for (boolean existing : new boolean[] { false, true })
                            parameters.add(new Object[]{ new Specification(restrictPartitionKey, columns, readCL, existing, StatementType.UPDATE, partitionKey++, flushPartials) });

                    for (String[] columns : new String[][] { { "s", "a" }, { "a", "b" }, { "a" } })
                        // These scenarios assume existing data.
                        parameters.add(new Object[]{ new Specification(restrictPartitionKey, columns, readCL, true, StatementType.DELETE, partitionKey++, flushPartials) });
                }
            }
        }

        return parameters;
    }

    @Test
    public void testPartialUpdateResolution()
    {
        Model model = new Model(specification);

        // Write and repair a version of the row that contains values for all columns we might query:
        if (specification.existing)
        {
            model.writeAndRepairRow();
            model.validateCurrent();
        }

        // Introduce a partial update that spans replicas:
        model.updatePartially();

        if (specification.flushPartials)
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        // TODO: Remove, but for now enabling trivially passes all tests:
        //CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();

        // If we wrote an initial (repaired) version of the row, do negative validation.
        // (i.e. Ensure a query that would have matched that row no longer returns any matches.) 
        if (specification.existing)
            model.validatePrevious();

        // In DELETE scenarios, which always have existing data, (negative) validation is already complete by now:
        if (specification.partialUpdateType == StatementType.UPDATE)
            model.validateCurrent();
    }

    @After
    public void deletePartition()
    {
        CLUSTER.coordinator(1).execute(String.format("DELETE FROM %s.%s WHERE pk = %d", KEYSPACE, specification.tableName(), specification.partitionKey), ALL);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}

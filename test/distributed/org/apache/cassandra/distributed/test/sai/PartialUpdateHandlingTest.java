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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.plan.Expression;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.index.sai.plan.Expression.IndexOperator.EQ;
import static org.apache.cassandra.index.sai.plan.Expression.IndexOperator.RANGE;

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
    private static final int PARTITIONS_PER_TEST = 20;
    private static final int NODES = 2;

    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(NODES).withConfig(config -> config.set("hinted_handoff_enabled", false).with(GOSSIP).with(NETWORK)).start());

        // All parameterized test scenarios share the same table and attached indexes, but write to different partitions
        // that are deleted after each scenario completes.
        String createTableDDL = String.format("CREATE TABLE %s.%s (pk int, pk2 int, ck int, s int static, y int static, a int, b int, x int, PRIMARY KEY ((pk, pk2), ck)) WITH read_repair = 'NONE'",
                                              KEYSPACE, TEST_TABLE_NAME);
        CLUSTER.schemaChange(createTableDDL);
        CLUSTER.disableAutoCompaction(KEYSPACE);

        CLUSTER.schemaChange(String.format("CREATE INDEX pk2_idx ON %s.%s(pk2) USING 'sai'", KEYSPACE, TEST_TABLE_NAME));
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
        final boolean existing;
        final StatementType partialUpdateType;
        final int partitionKey;
        final boolean flushPartials;
        final Expression.IndexOperator validationMode;

        Specification(boolean restrictPartitionKey,
                      String[] columns,
                      boolean existing,
                      StatementType partialUpdateType,
                      int partitionKey,
                      boolean flushPartials,
                      Expression.IndexOperator validationMode)
        {
            this.restrictPartitionKey = restrictPartitionKey;
            this.columns = columns;
            this.existing = existing;
            this.partialUpdateType = partialUpdateType;
            this.partitionKey = partitionKey;
            this.flushPartials = flushPartials;
            this.validationMode = validationMode;
        }

        public String[] nonKeyColumns()
        {
            return Arrays.stream(columns).filter(c -> !c.equals("ck") && !c.equals("pk") && !c.equals("pk2")).toArray(String[]::new);
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
                   ", partialUpdateType=" + partialUpdateType +
                   ", partitionKey=" + partitionKey +
                   ", flushPartials=" + flushPartials +
                   ", validationMode=" + validationMode;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Specification that = (Specification) o;
            return Arrays.equals(columns, that.columns) 
                   && existing == that.existing && restrictPartitionKey == that.restrictPartitionKey 
                   && partialUpdateType == that.partialUpdateType && partitionKey == that.partitionKey && flushPartials == that.flushPartials;
        }

        @Override
        public int hashCode()
        {
            int result = Objects.hash(existing, restrictPartitionKey, partialUpdateType, partitionKey, flushPartials);
            result = 31 * result + Arrays.hashCode(columns);
            return result < 0 ? -result : result;
        }
    }

    static class Model
    {
        final Specification specification;

        final List<Map<String, Integer>> previousRows = new ArrayList<>(PARTITIONS_PER_TEST);
        final List<Map<String, Integer>> currentRows = new ArrayList<>(PARTITIONS_PER_TEST);

        private int nextCellValue = 1;
        private int nextTimestamp = 1;

        Model(Specification specification)
        {
            this.specification = specification;
        }

        public void writeRepairedRows()
        {
            for (int i = 0; i < PARTITIONS_PER_TEST; i++)
            {
                StringBuilder insert = new StringBuilder("INSERT INTO ").append(KEYSPACE).append('.').append(specification.tableName());
                insert.append("(pk, pk2, ck");

                for (Object column : specification.nonKeyColumns())
                    insert.append(", ").append(column);

                int partitionKey = specification.partitionKey + i;
                insert.append(") VALUES (").append(partitionKey).append(", ").append(partitionKey).append(", 0");
                Map<String, Integer> row = new HashMap<>();
                row.put("pk", partitionKey);
                row.put("pk2", partitionKey);
                row.put("ck", 0);

                for (String column : specification.nonKeyColumns())
                {
                    int value = nextCellValue++;
                    row.put(column, value);
                    insert.append(", ").append(value);
                }

                insert.append(") USING TIMESTAMP ").append(nextTimestamp++);

                currentRows.add(row);
                CLUSTER.coordinator(1).execute(insert.toString(), ConsistencyLevel.ALL);
            }

            CLUSTER.get(1).nodetoolResult("repair", KEYSPACE).asserts().success();
        }

        public void writeUnrepairedRows()
        {
            // Bookmark the model state before partial updates are applied:
            for (Map<String, Integer> row : currentRows)
                previousRows.add(new HashMap<>(row));

            int node = 1;

            for (int i = 0; i < PARTITIONS_PER_TEST; i++)
            {
                for (String column : specification.nonKeyColumns())
                {
                    if (specification.partialUpdateType == StatementType.INSERT)
                        node = updateReplica(node, column, i);
                    else if (specification.partialUpdateType == StatementType.DELETE)
                        node = deleteReplica(node, column, i);
                    else
                        throw new IllegalStateException("Partial update must be either INSERT or DELETE");
                }
            }
        }

        private int updateReplica(int node, String column, int partitionIndex)
        {
            int value = nextCellValue++;
            int partitionKey = specification.partitionKey + partitionIndex;

            if (currentRows.size() > partitionIndex)
            {
                // A row already exists, so just update it:
                currentRows.get(partitionIndex).put(column, value);
            }
            else
            {
                // Create a new row with the appropriate cells and add it to the model:
                Map<String, Integer> row = new HashMap<>();
                row.put("pk", partitionKey);
                row.put("pk2", partitionKey);
                row.put("ck", 0);
                row.put(column, value);
                currentRows.add(row);

                assert currentRows.size() == partitionIndex + 1 : "Partition " + partitionIndex + " added at position " + (currentRows.size() - 1);
            }

            String dml = String.format("INSERT INTO %s.%s(pk, pk2, ck, %s) VALUES (?, ?, 0, ?) USING TIMESTAMP %d",
                                       KEYSPACE, specification.tableName(), column, nextTimestamp++);
            CLUSTER.get(node).executeInternal(dml, partitionKey, partitionKey, value);
            node = nextNode(node);
            return node;
        }

        private int deleteReplica(int node, Object column, int partitionIndex)
        {
            // Deletion should only happen when we've written the initial set of repaired partitions:
            assert currentRows.size() == PARTITIONS_PER_TEST : "Delete requested with only " + currentRows.size() + " model rows";
            currentRows.get(partitionIndex).remove((String) column);

            int partitionKey = specification.partitionKey + partitionIndex;
            String dml = String.format("DELETE %s FROM %s.%s USING TIMESTAMP %d WHERE pk = %d AND pk2 = %d AND ck = 0",
                                       column, KEYSPACE, specification.tableName(), nextTimestamp++, partitionKey, partitionKey);

            if (isStatic((String) column))
                dml = String.format("DELETE %s FROM %s.%s USING TIMESTAMP %d WHERE pk = %d AND pk2 = %d",
                                    column, KEYSPACE, specification.tableName(), nextTimestamp++, partitionKey, partitionKey);

            CLUSTER.get(node).executeInternal(dml);
            node = nextNode(node);
            return node;
        }
        
        private static boolean isStatic(String column)
        {
            return column.equals("s") || column.equals("y"); 
        }

        private static int nextNode(int node)
        {
            return Math.max(1, (node + 1) % (NODES + 1));
        }

        public void validateCurrent()
        {
            Object[][] result = queryWithModel(currentRows);
            assert specification.validationMode == EQ || specification.validationMode == RANGE : "Validation mode must be EQ or RANGE";
            int resultRowCount = specification.validationMode == EQ ? 1 : PARTITIONS_PER_TEST / 2;

            Object[][] expectedRows = new Object[resultRowCount][];

            for (int i = 0; i < resultRowCount; i++)
            {
                int partitionKey = specification.partitionKey + i;
                List<Object> expectedRow = Lists.newArrayList(partitionKey, partitionKey, 0);

                for (String column : specification.nonKeyColumns())
                    expectedRow.add(currentRows.get(i).get(column));

                expectedRows[i] = expectedRow.toArray();
            }

            // Sort by partition key value to make the result sets comparable:
            Arrays.sort(result, Comparator.comparingInt(row -> (Integer) row[0]));
            assertRows(result, expectedRows);
        }

        public void validatePrevious()
        {
            // Ensure queries against the previous version of the row no longer match.
            assertRows(queryWithModel(previousRows));
        }

        private Object[][] queryWithModel(List<Map<String, Integer>> modelRows)
        {
            StringBuilder select = new StringBuilder("SELECT pk, pk2, ck");

            for (Object column : specification.nonKeyColumns())
                select.append(", ").append(column);

            select.append(" FROM ").append(KEYSPACE).append('.').append(specification.tableName()).append(" WHERE ");

            ArrayList<String> restricted = Lists.newArrayList(specification.columns);

            if (specification.restrictPartitionKey)
            {
                restricted.add("pk");
                restricted.add("pk2");
            }

            List<String> clauses = new ArrayList<>();
            boolean needsAllowFiltering = false;

            if (specification.validationMode == EQ)
            {
                Map<String, Integer> primaryRow = modelRows.get(0);
                assertEquals(specification.partitionKey, primaryRow.get("pk").intValue());
                
                for (String column : restricted)
                {
                    clauses.add(column + " = " + primaryRow.get(column));
                    needsAllowFiltering |= isNotIndexed(column);
                }
            }
            else if (specification.validationMode == RANGE)
            {
                // Attempt to match the first half of the model's rows...
                for (String column : restricted)
                {
                    int min = modelRows.get(0).get(column);
                    clauses.add(column + " >= " + min);
                    int max = modelRows.get(PARTITIONS_PER_TEST / 2).get(column);
                    clauses.add(column + " < " + max);

                    needsAllowFiltering |= isNotIndexed(column);
                }
            }
            else
                throw new IllegalStateException("Validation mode must be EQ or RANGE");

            select.append(String.join(" AND ", clauses));

            if (needsAllowFiltering)
                select.append(" ALLOW FILTERING");

            Object[][] fullResult = CLUSTER.coordinator(1).execute(select.toString(), ALL);

            Iterator<Object[]> pagedResult = CLUSTER.coordinator(1).executeWithPaging(select.toString(), ALL, 1);
            assertRows(pagedResult, fullResult);

            return fullResult;
        }

        private static boolean isNotIndexed(String column)
        {
            return column.equals("x") || column.equals("y");
        }
    }

    @Parameterized.Parameter
    public Specification specification;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();

        // Each test scenario operates over a different set of partition keys. This starting key is the one
        // used in partition-restricted queries.
        int nextPartitionKey = 0;

        for (boolean flushPartials : new boolean[] { false, true })
        {
            for (boolean restrictPartitionKey : new boolean[] { false, true })
            {
                for (String[] columns : new String[][] { { "ck", "a" }, { "ck", "s" }, { "s", "a" }, { "a", "b" }, { "s", "x" }, { "s", "y" }, { "a", "x" }, { "a", "y" }, { "a" }, { "s" } })
                    for (boolean existing : new boolean[] { false, true })
                    {
                        parameters.add(new Object[] { new Specification(restrictPartitionKey, columns, existing, StatementType.INSERT, nextPartitionKey, flushPartials, EQ) });
                        nextPartitionKey += PARTITIONS_PER_TEST;
                    }

                // Deletion scenarios assume existing data.
                for (String[] columns : new String[][] { { "s", "a" }, { "a", "b" }, { "s", "x" }, { "a", "x" }, { "a", "y" }, { "a" }, { "s" } })
                {
                    parameters.add(new Object[] { new Specification(restrictPartitionKey, columns, true, StatementType.DELETE, nextPartitionKey, flushPartials, EQ) });
                    nextPartitionKey += PARTITIONS_PER_TEST;
                }
            }

            // Note that scenarios around indexes on a partition key element only appear here where we neither
            // delete nor restrict on partition, as both would be nonsensical.
            for (String[] columns : new String[][] { { "pk2", "a" }, { "s", "a" }, { "a", "b" }, { "s", "x" }, { "a", "x" }, { "a", "y" }, { "a" }, { "s" } })
                for (boolean existing : new boolean[] { false, true })
                {
                    parameters.add(new Object[]{ new Specification(false, columns, existing, StatementType.INSERT, nextPartitionKey, flushPartials, RANGE) });
                    nextPartitionKey += PARTITIONS_PER_TEST;
                }

            // Deletion scenarios assume existing data.
            for (String[] columns : new String[][] { { "s", "a" }, { "a", "b" }, { "s", "x" }, { "a", "x" }, { "a", "y" }, { "a" }, { "s" } })
            {
                parameters.add(new Object[]{ new Specification(false, columns, true, StatementType.DELETE, nextPartitionKey, flushPartials, RANGE) });
                nextPartitionKey += PARTITIONS_PER_TEST;
            }
        }

        return parameters;
    }

    @Test
    public void testPartialUpdateResolution()
    {
        Model model = new Model(specification);

        // Write and repair rows that contain values for all columns we might query:
        if (specification.existing)
        {
            model.writeRepairedRows();
            model.validateCurrent();
        }

        // Introduce partial writes that span replicas:
        model.writeUnrepairedRows();

        if (specification.flushPartials)
            // Flushg partial rows from Memtable-attached indexes to SSTable indexes:
            CLUSTER.stream().forEach(i -> i.flush(KEYSPACE));

        // If we wrote an initial (repaired) version of the row, do negative validation.
        // (i.e. Ensure queries that would have initially produced matches no longer do.) 
        if (specification.existing)
            model.validatePrevious();

        // In DELETE scenarios, which always have existing data, (negative) validation is already complete by now:
        if (specification.partialUpdateType == StatementType.INSERT)
            model.validateCurrent();
    }

    @After
    public void truncateTable()
    {
        CLUSTER.coordinator(1).execute(String.format("TRUNCATE TABLE %s.%s ", KEYSPACE, specification.tableName()), ALL);
    }

    @AfterClass
    public static void shutDownCluster()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }
}

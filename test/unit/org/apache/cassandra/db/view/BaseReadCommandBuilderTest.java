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

package org.apache.cassandra.db.view;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.cql3.QueryProcessor.parseStatement;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BaseReadCommandBuilderTest extends CQLTester
{
    QueryOptions options = QueryOptions.forInternalCalls(null);
    ClientState state = ClientState.forInternalCalls();

    @BeforeClass
    public static void init()
    {
        CQLTester.prepareServer();
        CQLTester.requireNetwork();
    }

    @Test
    public void testSimplePartitionKey()
    {
        String tbl = createTable("CREATE TABLE %s (" +
                                 "k int, " +
                                 "c bigint, " +
                                 "intval smallint, " +
                                 "random text, " +
                                 "PRIMARY KEY (k, c))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (intval, c, k)");
        TableMetadata baseTableMetadata = Schema.instance.getTableMetadata(keyspace(), tbl);
        assertNotNull(baseTableMetadata);
        TableMetadata viewMetadata = Keyspace.open(keyspace()).getMetadata().getTableOrViewNullable(viewName);
        assertNotNull(viewMetadata);
        View view = Keyspace.open(keyspace()).viewManager.getByName(viewName);
        assertNotNull(view);

        CQLStatement stmt = parseStatement(String.format("DELETE FROM %s.%s WHERE intval = 1 AND c = 2 AND k = 3", viewMetadata.keyspace, viewMetadata.name), state);
        assertTrue(stmt instanceof ModificationStatement);
        ModificationStatement modStmt = (ModificationStatement) stmt;

        CQLStatement baseStmt = parseStatement(String.format("SELECT * FROM %s.%s WHERE k = 3 AND c = 2", baseTableMetadata.keyspace, baseTableMetadata.name), state);
        assertTrue(baseStmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) baseStmt;

        verifyRawDataMap(modStmt, selectStmt, view, baseTableMetadata, viewMetadata);
        verifyReadCommand(modStmt, selectStmt, view);
    }

    @Test
    public void testCompositePartitionKeyInMV()
    {
        String tbl = createTable("CREATE TABLE %s (" +
                                 "k int, " +
                                 "c bigint, " +
                                 "intval smallint, " +
                                 "random text, " +
                                 "PRIMARY KEY (k, c))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY ((intval, c), k)");
        TableMetadata baseTableMetadata = Schema.instance.getTableMetadata(keyspace(), tbl);
        assertNotNull(baseTableMetadata);
        TableMetadata viewMetadata = Keyspace.open(keyspace()).getMetadata().getTableOrViewNullable(viewName);
        assertNotNull(viewMetadata);
        View view = Keyspace.open(keyspace()).viewManager.getByName(viewName);
        assertNotNull(view);

        CQLStatement stmt = parseStatement(String.format("DELETE FROM %s.%s WHERE intval = 1 AND c = 2 AND k = 3", viewMetadata.keyspace, viewMetadata.name), state);
        assertTrue(stmt instanceof ModificationStatement);
        ModificationStatement modStmt = (ModificationStatement) stmt;

        CQLStatement baseStmt = parseStatement(String.format("SELECT * FROM %s.%s WHERE k = 3 AND c = 2", baseTableMetadata.keyspace, baseTableMetadata.name), state);
        assertTrue(baseStmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) baseStmt;

        verifyRawDataMap(modStmt, selectStmt, view, baseTableMetadata, viewMetadata);
        verifyReadCommand(modStmt, selectStmt, view);
    }

    @Test
    public void testCompositePartitionKeyInBase()
    {
        String tbl = createTable("CREATE TABLE %s (" +
                                 "k int, " +
                                 "c bigint, " +
                                 "intval smallint, " +
                                 "random text, " +
                                 "PRIMARY KEY ((k, c), intval))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (k, intval, c)");
        TableMetadata baseTableMetadata = Schema.instance.getTableMetadata(keyspace(), tbl);
        assertNotNull(baseTableMetadata);
        TableMetadata viewMetadata = Keyspace.open(keyspace()).getMetadata().getTableOrViewNullable(viewName);
        assertNotNull(viewMetadata);
        View view = Keyspace.open(keyspace()).viewManager.getByName(viewName);
        assertNotNull(view);

        CQLStatement stmt = parseStatement(String.format("DELETE FROM %s.%s WHERE intval = 1 AND c = 2 AND k = 3", viewMetadata.keyspace, viewMetadata.name), state);
        assertTrue(stmt instanceof ModificationStatement);
        ModificationStatement modStmt = (ModificationStatement) stmt;

        CQLStatement baseStmt = parseStatement(String.format("SELECT * FROM %s.%s WHERE k = 3 AND c = 2 AND intval = 1", baseTableMetadata.keyspace, baseTableMetadata.name), state);
        assertTrue(baseStmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) baseStmt;

        verifyRawDataMap(modStmt, selectStmt, view, baseTableMetadata, viewMetadata);
        verifyReadCommand(modStmt, selectStmt, view);
    }

    @Test
    public void testParitialSelectionInMV()
    {
        String tbl = createTable("CREATE TABLE %s (" +
                                 "k int, " +
                                 "c bigint, " +
                                 "intval smallint, " +
                                 "random text, " +
                                 "PRIMARY KEY ((k, c), intval))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS SELECT k, c, intval FROM %s WHERE k IS NOT NULL AND c IS NOT NULL AND intval IS NOT NULL PRIMARY KEY (k, intval, c)");
        TableMetadata baseTableMetadata = Schema.instance.getTableMetadata(keyspace(), tbl);
        assertNotNull(baseTableMetadata);
        TableMetadata viewMetadata = Keyspace.open(keyspace()).getMetadata().getTableOrViewNullable(viewName);
        assertNotNull(viewMetadata);
        View view = Keyspace.open(keyspace()).viewManager.getByName(viewName);
        assertNotNull(view);

        CQLStatement stmt = parseStatement(String.format("DELETE FROM %s.%s WHERE intval = 1 AND c = 2 AND k = 3", viewMetadata.keyspace, viewMetadata.name), state);
        assertTrue(stmt instanceof ModificationStatement);
        ModificationStatement modStmt = (ModificationStatement) stmt;

        CQLStatement baseStmt = parseStatement(String.format("SELECT * FROM %s.%s WHERE k = 3 AND c = 2 AND intval = 1", baseTableMetadata.keyspace, baseTableMetadata.name), state);
        assertTrue(baseStmt instanceof SelectStatement);
        SelectStatement selectStmt = (SelectStatement) baseStmt;

        verifyRawDataMap(modStmt, selectStmt, view, baseTableMetadata, viewMetadata);
        verifyReadCommand(modStmt, selectStmt, view);
    }

    @Test
    public void testReadResult() throws Throwable
    {
        String tbl = createTable("CREATE TABLE %s (" +
                                 "k int, " +
                                 "c bigint, " +
                                 "intval smallint, " +
                                 "random text, " +
                                 "PRIMARY KEY (k, c))");
        String viewName = createView("CREATE MATERIALIZED VIEW %s AS SELECT k, c, random FROM %s WHERE k IS NOT NULL AND c IS NOT NULL PRIMARY KEY (c, k)");
        execute("INSERT INTO %s (k, c, intval, random) VALUES (3, 2, 1, 'test')");
        execute("INSERT INTO %s (k, c, intval, random) VALUES (0, 0, 0, 'test0')");

        TableMetadata baseTableMetadata = Schema.instance.getTableMetadata(keyspace(), tbl);
        assertNotNull(baseTableMetadata);
        TableMetadata viewMetadata = Keyspace.open(keyspace()).getMetadata().getTableOrViewNullable(viewName);
        assertNotNull(viewMetadata);
        View view = Keyspace.open(keyspace()).viewManager.getByName(viewName);
        assertNotNull(view);

        CQLStatement stmt = parseStatement(String.format("DELETE FROM %s.%s WHERE c = 2 AND k = 3", viewMetadata.keyspace, viewMetadata.name), state);
        assertTrue(stmt instanceof ModificationStatement);
        ModificationStatement modStmt = (ModificationStatement) stmt;

        BaseReadCommandBuilder builder = new BaseReadCommandBuilder(view,
                                                                    modStmt.buildPartitionKeyNames(options, state).get(0),
                                                                    Iterables.getOnlyElement(modStmt.createClustering(options, state)));
        // build the read command
        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd = builder.buildBaseTableReadCommand(nowInSec);

        try (PartitionIterator iter = cmd.execute(ConsistencyLevel.ONE, state, Dispatcher.RequestTime.forImmediateExecution()))
        {
            assertTrue(iter.hasNext());
            RowIterator rowIter = iter.next();
            // verify the partition key is the same as the one we inserted
            assertEquals(ByteBufferUtil.bytes(3), rowIter.partitionKey().getKey());
            assertTrue(rowIter.hasNext());
            Row row = rowIter.next();
            // verify the clustering is the same as the one we inserted
            assertEquals(Util.clustering(baseTableMetadata.comparator, 2L), row.clustering());
            // verify the row is the same as the one we inserted
            assertEquals(ByteBufferUtil.bytes((short) 1), row.getCell(baseTableMetadata.getColumn(ByteBufferUtil.bytes("intval"))).buffer());
            assertEquals(ByteBufferUtil.bytes("test"), row.getCell(baseTableMetadata.getColumn(ByteBufferUtil.bytes("random"))).buffer());
            // verify 2 columns are selected (intval, random)
            assertEquals(2, row.columnCount());
            // only 1 row
            assertFalse(rowIter.hasNext());
            // only 1 partition
            assertFalse(iter.hasNext());
        }
    }

    private void verifyReadCommand(ModificationStatement modStmt,
                                   SelectStatement selectStmt,
                                   View view)
    {
        BaseReadCommandBuilder builder = new BaseReadCommandBuilder(view,
                                                                    modStmt.buildPartitionKeyNames(options, state).get(0),
                                                                    Iterables.getOnlyElement(modStmt.createClustering(options, state)));
        // build the read command
        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand cmd = builder.buildBaseTableReadCommand(nowInSec);
        ReadQuery expected = selectStmt.getQuery(options, nowInSec);
        SinglePartitionReadCommand.Group group = (SinglePartitionReadCommand.Group) expected;
        assertEquals(1, group.queries.size());
        assertEquals(group.queries.get(0).toString(), cmd.toString());
    }

    private void verifyRawDataMap(ModificationStatement modStmt,
                                  SelectStatement selectStmt,
                                  View view,
                                  TableMetadata baseTableMetadata,
                                  TableMetadata viewMetadata)
    {
        BaseReadCommandBuilder builder = new BaseReadCommandBuilder(view,
                                                                    modStmt.buildPartitionKeyNames(options, state).get(0),
                                                                    Iterables.getOnlyElement(modStmt.createClustering(options, state)));
        // raw data for base read
        ByteBuffer basePartitionKey = selectStmt.getRestrictions().getPartitionKeys(options, state).get(0);
        Clustering<?> baseClusteringKey = selectStmt.getRestrictions().getClusteringColumns(options, state).iterator().next();

        // build the primary key map
        Map<ColumnMetadata, ByteBuffer> primaryKey = builder.viewRawDataMap();
        assertEquals(viewMetadata.partitionKeyColumns().size() + viewMetadata.clusteringColumns().size(), primaryKey.size());

        // re-index the primaryKey by column name (ColumnIdentifier)
        Map<ColumnIdentifier, ByteBuffer> viewPK = primaryKey.entrySet()
                                                             .stream()
                                                             .collect(Collectors.toMap(e -> e.getKey().name, Map.Entry::getValue));

        // verify the partition key for base table is found in the view's primary key map, and the values match
        if (baseTableMetadata.partitionKeyType instanceof CompositeType)
        {
            CompositeType compositeType = (CompositeType) baseTableMetadata.partitionKeyType;
            ByteBuffer[] components = compositeType.split(basePartitionKey);

            List<ColumnMetadata> pkCols = baseTableMetadata.partitionKeyColumns();
            for (int i = 0; i < pkCols.size(); i++)
            {
                ColumnMetadata col = pkCols.get(i);
                assertEquals(components[i], viewPK.get(col.name));
            }
        }
        else
            assertEquals(basePartitionKey, viewPK.get(baseTableMetadata.partitionKeyColumns().get(0).name));


        // verify the clustering key for base table is found in the view's primary key map, and the values match
        List<ColumnMetadata> ckCols = baseTableMetadata.clusteringColumns();
        for (int i = 0; i < ckCols.size(); i++)
        {
            ColumnMetadata col = ckCols.get(i);
            assertEquals(baseClusteringKey.bufferAt(col.position()), viewPK.get(col.name));
        }
    }
}

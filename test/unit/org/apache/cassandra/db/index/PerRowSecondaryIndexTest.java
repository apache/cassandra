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
package org.apache.cassandra.db.index;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.ThriftSessionManager;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.thrift.TException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PerRowSecondaryIndexTest
{

    // test that when index(key) is called on a PRSI index,
    // the data to be indexed can be read using the supplied
    // key. TestIndex.index(key) simply reads the data to be
    // indexed & stashes it in a static variable for inspection
    // in the test.

    private static final String KEYSPACE1 = "PerRowSecondaryIndex";
    private static final String CF_INDEXED = "Indexed1";
    private static final String INDEXED_COLUMN = "indexed";

    private static CassandraServer server;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        SchemaLoader.prepareServer();
        SchemaLoader.startGossiper();
        new EmbeddedCassandraService().start();
        ThriftSessionManager.instance.setCurrentSocket(new InetSocketAddress(9160));
        for (KSMetaData ksm : SchemaLoader.schemaDefinition(null))
                MigrationManager.announceNewKeyspace(ksm);
//        SchemaLoader.createKeyspace(KEYSPACE1,
//                                    SimpleStrategy.class,
//                                    KSMetaData.optsWithRF(1),
//                                    SchemaLoader.perRowIndexedCFMD(KEYSPACE1, CF_INDEXED));
        server = new CassandraServer();
        server.set_keyspace(KEYSPACE1);
    }

    @Before
    public void clearTestStub()
    {
        PerRowSecondaryIndexTest.TestIndex.reset();
    }

    @Test
    public void testIndexInsertAndUpdate()
    {
        // create a row then test that the configured index instance was able to read the row
        Mutation rm;
        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", Util.cellname("indexed"), ByteBufferUtil.bytes("foo"), 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("foo"), indexedRow.getColumn(Util.cellname("indexed")).value());

        // update the row and verify what was indexed
        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k1"));
        rm.add("Indexed1", Util.cellname("indexed"), ByteBufferUtil.bytes("bar"), 2);
        rm.apply();

        indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        assertEquals(ByteBufferUtil.bytes("bar"), indexedRow.getColumn(Util.cellname("indexed")).value());
        assertTrue(Arrays.equals("k1".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testColumnDelete()
    {
        // issue a column delete and test that the configured index instance was notified to update
        Mutation rm;
        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k2"));
        rm.delete("Indexed1", Util.cellname("indexed"), 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);

        for (Cell cell : indexedRow.getSortedColumns())
            assertFalse(cell.isLive());

        assertTrue(Arrays.equals("k2".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }

    @Test
    public void testRowDelete()
    {
        // issue a row level delete and test that the configured index instance was notified to update
        Mutation rm;
        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k3"));
        rm.delete("Indexed1", 1);
        rm.apply();

        ColumnFamily indexedRow = PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_ROW;
        assertNotNull(indexedRow);
        for (Cell cell : indexedRow.getSortedColumns())
            assertFalse(cell.isLive());

        assertTrue(Arrays.equals("k3".getBytes(), PerRowSecondaryIndexTest.TestIndex.LAST_INDEXED_KEY.array()));
    }
    
    @Test
    public void testInvalidSearch() throws IOException
    {
        Mutation rm;
        rm = new Mutation("PerRowSecondaryIndex", ByteBufferUtil.bytes("k4"));
        rm.add("Indexed1", Util.cellname("indexed"), ByteBufferUtil.bytes("foo"), 1);
        rm.apply();
        
        // test we can search:
        UntypedResultSet result = QueryProcessor.executeInternal("SELECT * FROM \"PerRowSecondaryIndex\".\"Indexed1\" WHERE indexed = 'foo'");
        assertEquals(1, result.size());

        // test we can't search if the searcher doesn't validate the expression:
        try
        {
            QueryProcessor.executeInternal("SELECT * FROM \"PerRowSecondaryIndex\".\"Indexed1\" WHERE indexed = 'invalid'");
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof InvalidRequestException || (e.getCause() != null && (e.getCause() instanceof InvalidRequestException)));
        }
    }

    @Test
    public void testInvalidCqlInsert()
    {
        // test we can insert if the index validates the expression:
        QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".\"Indexed1\" (key, indexed) VALUES ('valid','valid')", KEYSPACE1));

        // test we can't insert if the index doesn't validate the key:
        try
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".\"Indexed1\" (key, indexed) VALUES ('invalid','valid')", KEYSPACE1));
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
        }

        // test we can't insert if the index doesn't validate the columns:
        try
        {
            QueryProcessor.executeInternal(String.format("INSERT INTO \"%s\".\"Indexed1\" (key, indexed) VALUES ('valid','invalid')", KEYSPACE1));
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e.getCause() instanceof InvalidRequestException);
        }
    }

    @Test
    public void testInvalidThriftInsert() throws IOException, TException
    {

        long timestamp = System.currentTimeMillis();
        ColumnPath cp = new ColumnPath(CF_INDEXED);
        ColumnParent par = new ColumnParent(CF_INDEXED);
        cp.column = ByteBufferUtil.bytes(INDEXED_COLUMN);

        // test we can insert if the index validates the expression:
        ByteBuffer key = ByteBufferUtil.bytes("valid");
        server.insert(key, par, new Column(key).setValue(ByteBufferUtil.bytes("valid")).setTimestamp(timestamp), ConsistencyLevel.ONE);

        // test we can't insert if the index doesn't validate the key:
        try
        {
            key = ByteBufferUtil.bytes("invalid");
            server.insert(key, par, new Column(key).setValue(ByteBufferUtil.bytes("valid")).setTimestamp(timestamp), ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }

        // test we can't insert if the index doesn't validate the columns:
        try
        {
            key = ByteBufferUtil.bytes("valid");
            server.insert(key, par, new Column(key).setValue(ByteBufferUtil.bytes("invalid")).setTimestamp(timestamp), ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }
    }

    @Test
    public void testInvalidThriftCas() throws IOException, TException
    {
        // test we can insert if the index validates the expression:
        ByteBuffer key = ByteBufferUtil.bytes("valid");
        Column column = new Column(key).setValue(ByteBufferUtil.bytes("valid")).setTimestamp(System.currentTimeMillis());
        server.cas(key, CF_INDEXED, Collections.<Column>emptyList(), Collections.singletonList(column), ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ONE);

        // test we can't insert if the index doesn't validate the key:
        try
        {
            key = ByteBufferUtil.bytes("invalid");
            server.cas(key, CF_INDEXED, Collections.<Column>emptyList(), Collections.singletonList(column), ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }

        // test we can't insert if the index doesn't validate the columns:
        try
        {
            key = ByteBufferUtil.bytes("valid");
            column.setValue(ByteBufferUtil.bytes("invalid"));
            server.cas(key, CF_INDEXED, Collections.<Column>emptyList(), Collections.singletonList(column), ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }
    }

    @Test
    public void testInvalidThriftBatchMutate() throws IOException, TException
    {
        ByteBuffer key = ByteBufferUtil.bytes("valid");
        long timestamp = System.currentTimeMillis();

        org.apache.cassandra.thrift.Mutation mutation = new org.apache.cassandra.thrift.Mutation();
        Column column = new Column(key).setValue(ByteBufferUtil.bytes("valid")).setTimestamp(System.currentTimeMillis());
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.setColumn(column);
        mutation.setColumn_or_supercolumn(cosc);

        server.batch_mutate(Collections.singletonMap(key, Collections.singletonMap(CF_INDEXED, Collections.singletonList(mutation))), ConsistencyLevel.ONE);

        // test we can't insert if the index doesn't validate the key:
        try
        {
            key = ByteBufferUtil.bytes("invalid");
            server.batch_mutate(Collections.singletonMap(key, Collections.singletonMap(CF_INDEXED, Collections.singletonList(mutation))), ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }

        // test we can't insert if the index doesn't validate the columns:
        try
        {
            key = ByteBufferUtil.bytes("valid");
            cosc.setColumn(new Column(key).setValue(ByteBufferUtil.bytes("invalid")).setTimestamp(timestamp));
            server.batch_mutate(Collections.singletonMap(key, Collections.singletonMap(CF_INDEXED, Collections.singletonList(mutation))), ConsistencyLevel.ONE);
            fail("Query should have been invalid!");
        }
        catch (Exception e)
        {
            assertTrue(e instanceof org.apache.cassandra.thrift.InvalidRequestException);
        }
    }

    public static class TestIndex extends PerRowSecondaryIndex
    {
        public static volatile boolean ACTIVE = true;
        public static ColumnFamily LAST_INDEXED_ROW;
        public static ByteBuffer LAST_INDEXED_KEY;

        public static void reset()
        {
            ACTIVE = true;
            LAST_INDEXED_KEY = null;
            LAST_INDEXED_ROW = null;
        }

        @Override
        public boolean indexes(CellName name)
        {
            return ACTIVE;
        }
        
        @Override
        public boolean indexes(ColumnDefinition cdef)
        {
            return ACTIVE;
        }
        
        @Override
        public void index(ByteBuffer rowKey, ColumnFamily cf)
        {
            QueryFilter filter = QueryFilter.getIdentityFilter(DatabaseDescriptor.getPartitioner().decorateKey(rowKey),
                                                               baseCfs.getColumnFamilyName(),
                                                               System.currentTimeMillis());
            LAST_INDEXED_ROW = cf;
            LAST_INDEXED_KEY = rowKey;
        }

        @Override
        public void delete(DecoratedKey key, OpOrder.Group opGroup)
        {
        }

        @Override
        public void init()
        {
        }

        @Override
        public void reload()
        {
        }

        @Override
        public void validateOptions() throws ConfigurationException
        {
        }

        @Override
        public String getIndexName()
        {
            return this.getClass().getSimpleName();
        }

        @Override
        protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns)
        {
            return new SecondaryIndexSearcher(baseCfs.indexManager, columns)
            {
                
                @Override
                public List<Row> search(ExtendedFilter filter)
                {
                    return Arrays.asList(new Row(LAST_INDEXED_KEY, LAST_INDEXED_ROW));
                }

                @Override
                public void validate(IndexExpression indexExpression) throws InvalidRequestException
                {
                    if (indexExpression.value.equals(ByteBufferUtil.bytes("invalid")))
                        throw new InvalidRequestException("Invalid search!");
                }
                
            };
        }

        @Override
        public void forceBlockingFlush()
        {
        }

        @Override
        public ColumnFamilyStore getIndexCfs()
        {
            return baseCfs;
        }

        @Override
        public void removeIndex(ByteBuffer columnName)
        {
        }

        @Override
        public void invalidate()
        {
        }

        @Override
        public void truncateBlocking(long truncatedAt)
        {
        }

        @Override
        public long estimateResultRows() {
            return 0;
        }

        @Override
        public void validate(ByteBuffer key, ColumnFamily cf) throws InvalidRequestException
        {
            if (key.equals(ByteBufferUtil.bytes("invalid")))
            {
                throw new InvalidRequestException("Invalid key!");
            }
            for (Cell cell : cf)
            {
                if (cell.value().equals(ByteBufferUtil.bytes("invalid")))
                {
                    throw new InvalidRequestException("Invalid column!");
                }
            }
        }
    }
}

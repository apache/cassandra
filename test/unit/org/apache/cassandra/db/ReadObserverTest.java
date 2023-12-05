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

package org.apache.cassandra.db;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.schema.CachingParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ReadObserverTest
{
    public static class TestReadObserverFactory implements ReadObserverFactory
    {
        static List<Pair<TableMetadata, ReadObserver>> issuedObservers = new CopyOnWriteArrayList<>();

        @Override
        public ReadObserver create(TableMetadata table)
        {
            ReadObserver observer = mock(ReadObserver.class);
            issuedObservers.add(Pair.create(table, observer));
            return observer;
        }
    }

    private static final String CF = "Standard";
    private static final String KEYSPACE = "ReadObserverTest";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        CassandraRelevantProperties.CUSTOM_READ_OBSERVER_FACTORY.setString(TestReadObserverFactory.class.getName());

        DatabaseDescriptor.daemonInitialization();

        TableMetadata.Builder metadata =
        TableMetadata.builder(KEYSPACE, CF)
                     .addPartitionKeyColumn("key", BytesType.instance)
                     .addStaticColumn("s", AsciiType.instance)
                     .addClusteringColumn("col", AsciiType.instance)
                     .addRegularColumn("a", AsciiType.instance)
                     .addRegularColumn("b", AsciiType.instance)
                     .caching(CachingParams.CACHE_EVERYTHING);

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    metadata);

        LocalSessionAccessor.startup();
    }

    @Test
    public void testObserverCallbacks()
    {
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF);

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
        .clustering("cc")
        .add("a", ByteBufferUtil.bytes("regular"))
        .build()
        .apply();

        new RowUpdateBuilder(cfs.metadata(), 0, ByteBufferUtil.bytes("key"))
        .add("s", ByteBufferUtil.bytes("static"))
        .build()
        .apply();

        ReadCommand readCommand = Util.cmd(cfs, Util.dk("key")).build();
        assertFalse(Util.getAll(readCommand).isEmpty());

        List<Pair<TableMetadata, ReadObserver>> observers = TestReadObserverFactory.issuedObservers.stream()
                                                                                                   .filter(p -> p.left.name.equals(CF))
                                                                                                   .collect(Collectors.toList());
        assertEquals(1, observers.size());
        ReadObserver observer = observers.get(0).right;

        verify(observer).onPartition(eq(Util.dk("key")), eq(DeletionTime.LIVE));
        verify(observer).onUnfiltered(argThat(Unfiltered::isRow));
        verify(observer).onStaticRow(argThat(row -> row.columns().stream().allMatch(col -> col.name.toCQLString().equals("s"))));
        verify(observer).onComplete();
    }
}
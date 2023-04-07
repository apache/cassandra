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

package org.apache.cassandra.service;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.Executors;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.ByteOrderedPartitioner.BytesToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;

public class StorageServiceDrainTest
{
    private static final String KEYSPACE = "keyspace";
    private static final String TABLE = "table";
    private static final String COLUMN = "column";
    private static final int ROWS = 1000;

    @Before
    public void before() throws UnknownHostException
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);

        CommitLog.instance.start();

        CompactionManager.instance.disableAutoCompaction();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));

        StorageService.instance
                .getTokenMetadata()
                .updateNormalToken(new BytesToken((new byte[]{50})), InetAddressAndPort.getByName("127.0.0.1"));

        final ColumnFamilyStore table = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        for (int row = 0; row < ROWS; row++)
        {
            final ByteBuffer value = ByteBufferUtil.bytes(String.valueOf(row));
            new RowUpdateBuilder(table.metadata(), System.currentTimeMillis(), value)
                    .clustering(ByteBufferUtil.bytes(COLUMN))
                    .add("val", value)
                    .build()
                    .applyUnsafe();
        }
        Util.flush(table);
    }

    @Test
    public void testSSTablesImportAbort()
    {
        final ColumnFamilyStore table = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);

        assertTrue(table
                .importNewSSTables(Collections.emptySet(), false, false, false, false, false, false, false)
                .isEmpty());

        Executors.newSingleThreadExecutor().execute(() -> {
                try
                {
                    StorageService.instance.drain();
                }
                catch (final Exception exception)
                {
                    throw new RuntimeException(exception);
                }});

        while (!StorageService.instance.isDraining())
            Thread.yield();

        assertThatThrownBy(() -> table
                .importNewSSTables(Collections.emptySet(), false, false, false, false, false, false, false))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(InterruptedException.class);
    }
}

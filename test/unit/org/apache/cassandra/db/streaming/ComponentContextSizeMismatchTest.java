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

package org.apache.cassandra.db.streaming;

import java.io.IOException;
import java.nio.channels.FileChannel;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the CASSANDRA-21520 hardening in {@link ComponentContext#channel} where an entire-sstable (zero-copy)
 * streaming size mismatch was changed from an {@code assert} (a no-op when assertions are disabled, as in
 * production) into a real {@link IOException}. This test verifies the mismatch path fails cleanly with an
 * {@code IOException} (never an {@link AssertionError}) even when assertions are disabled, and that the matching
 * path still returns a usable channel.
 */
public class ComponentContextSizeMismatchTest
{
    private static final String KEYSPACE = "ComponentContextSizeMismatchTest";
    private static final String CF_STANDARD = "Standard1";

    private static SSTableReader sstable;

    @BeforeClass
    public static void defineSchemaAndPrepareSSTable()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD));

        ColumnFamilyStore store = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD);
        CompactionManager.instance.disableAutoCompaction();
        for (int j = 0; j < 10; j++)
        {
            new RowUpdateBuilder(store.metadata(), j, String.valueOf(j))
            .clustering("0")
            .add("val", ByteBufferUtil.EMPTY_BYTE_BUFFER)
            .build()
            .applyUnsafe();
        }
        Util.flush(store);
        CompactionManager.instance.performMaximal(store);

        sstable = store.getLiveSSTables().iterator().next();
    }

    @Test
    public void matchingSizeReturnsChannel() throws IOException
    {
        try (ComponentContext context = ComponentContext.create(sstable))
        {
            Component component = firstStreamableComponent(context);
            long advertised = context.manifest().sizeOf(component);
            try (FileChannel channel = context.channel(sstable.descriptor, component, advertised))
            {
                assertEquals("advertised size must match the streamed channel size", advertised, channel.size());
            }
        }
    }

    @Test
    public void sizeMismatchThrowsIOExceptionNotAssertionError() throws IOException
    {
        try (ComponentContext context = ComponentContext.create(sstable))
        {
            Component component = firstStreamableComponent(context);
            long advertised = context.manifest().sizeOf(component);
            long wrongSize = advertised + 1; // pretend the on-disk file no longer matches the advertised manifest

            try
            {
                context.channel(sstable.descriptor, component, wrongSize);
                fail("Expected an IOException when the advertised size does not match the on-disk size");
            }
            catch (AssertionError e)
            {
                // The whole point of the hardening: this must NOT be an AssertionError, since assertions are
                // disabled in production and the corrupt bytes would otherwise be shipped silently.
                throw new AssertionError("size mismatch must fail via IOException, not AssertionError", e);
            }
            catch (IOException e)
            {
                assertTrue("exception message should describe the size mismatch",
                           e.getMessage().contains("file size to be"));
                assertTrue("exception message should include the actual on-disk size",
                           e.getMessage().contains(String.valueOf(advertised)));
            }
        }
    }

    private static Component firstStreamableComponent(ComponentContext context)
    {
        return context.manifest().components().iterator().next();
    }
}

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

package org.apache.cassandra.test.microbench.sstable;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.schema.KeyspaceParams;

public abstract class AbstractSSTableBench
{
    private final static Logger logger = LoggerFactory.getLogger(AbstractSSTableBench.class);

    public static final String KEYSPACE = "SSTableWriterBench";
    public static final String TABLE = "table";
    public static final String TABLE_WITH_CLUSTERING = "table_with_clustering";

    public SSTableFormat getFormat(String formatName)
    {
        return SSTableFormat.Type.valueOf(formatName).info;
    }

    public Keyspace prepareMetadata()
    {
        ServerTestUtils.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE, 0, BytesType.instance, BytesType.instance, BytesType.instance),
                                    SchemaLoader.standardCFMD(KEYSPACE, TABLE_WITH_CLUSTERING, 0, BytesType.instance, BytesType.instance, BytesType.instance));

        CommitLog.instance.stopUnsafe(true);
        return Keyspace.open(KEYSPACE);
    }

    /**
     * Create partition keys from numbers in range {@code [min; max)} of size in bytes as in {@code keySize}
     */
    public DecoratedKey[] prepareDecoratedKeys(int min, int max, int keySize)
    {
        int n = max - min;
        DecoratedKey[] keys = new DecoratedKey[n];
        for (int i = 0; i < n; i++)
        {
            ByteBuffer buf = ByteBuffer.allocate(keySize);
            buf.putInt(0, i + min);
            keys[i] = Murmur3Partitioner.instance.decorateKey(buf.duplicate());
        }
        Arrays.sort(keys);
        return keys;
    }

    /**
     * Create clustering keys from numbers in range {@code [min; max)} of size in bytes as in {@code keySize}
     */
    public ByteBuffer[] prepareBuffers(int min, int max, int KEY_SIZE)
    {
        int n = max - min;
        ByteBuffer[] ckeys = new ByteBuffer[n];
        for (int i = 0; i < n; i++)
        {
            ckeys[i] = ByteBuffer.allocate(KEY_SIZE);
            ckeys[i].putInt(0, i + min);
        }
        return ckeys;
    }

    public SSTableWriter createWriter(ColumnFamilyStore table, SSTableFormat format, LifecycleTransaction txn) throws Exception
    {
        Path tableDir = Files.createTempDirectory(getClass().getSimpleName());
        Descriptor desc = table.newSSTableDescriptor(tableDir.toFile(), format.getType());

        return SSTableWriter.create(desc,
                                    0,
                                    0,
                                    null,
                                    false,
                                    new SerializationHeader(true, table.metadata(), table.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS),
                                    table.indexManager.listIndexGroups(),
                                    txn);
    }

}

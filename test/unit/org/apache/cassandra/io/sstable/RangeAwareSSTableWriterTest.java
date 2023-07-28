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

package org.apache.cassandra.io.sstable;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class RangeAwareSSTableWriterTest
{
    public static final String KEYSPACE1 = "Keyspace1";
    public static final String CF_STANDARD = "Standard1";

    public static ColumnFamilyStore cfs;

    @BeforeClass
    public static void defineSchema() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();

        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD)
                                                .partitioner(Murmur3Partitioner.instance));

        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        cfs = keyspace.getColumnFamilyStore(CF_STANDARD);
        cfs.clearUnsafe();
        cfs.disableAutoCompaction();
    }

    @Test
    public void testAccessWriterBeforeAppend() throws IOException
    {

        SchemaLoader.insertData(KEYSPACE1, CF_STANDARD, 0, 1);
        Util.flush(cfs);

        LifecycleTransaction txn = LifecycleTransaction.offline(OperationType.STREAM);

        RangeAwareSSTableWriter writer = new RangeAwareSSTableWriter(cfs,
                                                                     0,
                                                                     0,
                                                                     null,
                                                                     false,
                                                                     DatabaseDescriptor.getSelectedSSTableFormat(),
                                                                     0,
                                                                     0,
                                                                     txn,
                                                                     SerializationHeader.make(cfs.metadata(),
                                                                                              cfs.getLiveSSTables()));
        assertEquals(cfs.metadata.id, writer.getTableId());
        assertEquals(0L, writer.getBytesWritten());
        assertEquals(0L, writer.getOnDiskBytesWritten());
    }
}

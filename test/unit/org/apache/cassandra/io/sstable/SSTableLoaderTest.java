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

import java.io.File;
import java.util.List;

import com.google.common.io.Files;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;

public class SSTableLoaderTest extends SchemaLoader
{
    @BeforeClass
    public static void setup() throws Exception
    {
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @Test
    public void testLoadingSSTable() throws Exception
    {
        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + "Keyspace1" + File.separator + "Standard1");
        assert dataDir.mkdirs();
        CFMetaData cfmeta = Schema.instance.getCFMetaData("Keyspace1", "Standard1");
        SSTableSimpleUnsortedWriter writer = new SSTableSimpleUnsortedWriter(dataDir,
                                                                             cfmeta,
                                                                             StorageService.getPartitioner(),
                                                                             1);
        DecoratedKey key = Util.dk("key1");
        writer.newRow(key.getKey());
        writer.addColumn(ByteBufferUtil.bytes("col1"), ByteBufferUtil.bytes(100), 1);
        writer.close();

        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            public void init(String keyspace)
            {
                for (Range<Token> range : StorageService.instance.getLocalRanges("Keyspace1"))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
                setPartitioner(StorageService.getPartitioner());
            }

            public CFMetaData getCFMetaData(String keyspace, String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();

        List<Row> rows = Util.getRangeSlice(Keyspace.open("Keyspace1").getColumnFamilyStore("Standard1"));
        assertEquals(1, rows.size());
        assertEquals(key, rows.get(0).key);
        assertEquals(ByteBufferUtil.bytes(100), rows.get(0).cf.getColumn(Util.cellname("col1")).value());
    }
}

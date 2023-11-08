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

package org.apache.cassandra.hints;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileInputStreamPlus;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaTestUtil;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HintsUpgradeTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final String KEYSPACE = "Keyspace1";
    private static final String TABLE = "Standard1";
    private static final String CELLNAME = "name";

    private static final String DATA_DIR = "test/data/legacy-hints/";
    private static final String PROPERTIES_FILE = "hash.txt";
    private static final String HOST_ID_PROPERTY = "hostId";
    private static final String CFID_PROPERTY = "cfid";
    private static final String CELLS_PROPERTY = "cells";
    private static final String DESCRIPTOR_TIMESTAMP_PROPERTY = "descriptorTimestamp";
    private static final String HASH_PROPERTY = "hash";

    static TableMetadata.Builder metadataBuilder = TableMetadata.builder(KEYSPACE, TABLE)
                                                                .addPartitionKeyColumn("key", AsciiType.instance)
                                                                .addClusteringColumn("col", AsciiType.instance)
                                                                .addRegularColumn("val", BytesType.instance)
                                                                .addRegularColumn("val0", BytesType.instance)
                                                                .compression(SchemaLoader.getCompressionParameters());

    @BeforeClass
    public static void initialize()
    {
        SchemaLoader.loadSchema();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1));
    }

    @After
    public void teardown()
    {
        SchemaTestUtil.announceTableDrop(KEYSPACE, TABLE);
    }

    private static class HintProperties
    {
        private UUID hostId;
        private String tableId;
        private int cells;
        private long descriptorTimestamp;
        private int hash;
        private File dir;
    }

    private HintProperties init(String version) throws Exception
    {
        HintProperties properties = loadHintProperties(DATA_DIR + version);
        SchemaTestUtil.announceNewTable(metadataBuilder.id(TableId.fromString(properties.tableId)).build());
        return properties;
    }

    @Test // version 1 of hints
    public void test30() throws Exception
    {
        HintProperties properties = init("3.0.29");
        readHints(properties);
    }

    @Test // version 2 of hints
    public void test41() throws Exception
    {
        HintProperties properties = init("4.1.3");
        readHints(properties);
    }

    private HintProperties loadHintProperties(String dir) throws Exception
    {
        Properties prop = new Properties();
        prop.load(new FileInputStreamPlus(new File(dir + File.pathSeparator() + PROPERTIES_FILE)));

        HintProperties hintProperties = new HintProperties();

        hintProperties.hostId = UUID.fromString(prop.getProperty(HOST_ID_PROPERTY));
        hintProperties.tableId = prop.getProperty(CFID_PROPERTY);
        hintProperties.cells = Integer.parseInt(prop.getProperty(CELLS_PROPERTY));
        hintProperties.descriptorTimestamp = Long.parseLong(prop.getProperty(DESCRIPTOR_TIMESTAMP_PROPERTY));
        hintProperties.hash = Integer.parseInt(prop.getProperty(HASH_PROPERTY));
        hintProperties.dir = new File(dir);

        return hintProperties;
    }

    private void readHints(HintProperties hintProperties)
    {
        HintsCatalog catalog = HintsCatalog.load(hintProperties.dir, ImmutableMap.of());
        assertTrue(catalog.hasFiles());

        HintsStore store = catalog.getNullable(hintProperties.hostId);
        assertNotNull(store);
        assertEquals(hintProperties.hostId, store.hostId);

        HintsDescriptor descriptor = store.poll();
        assertEquals(hintProperties.descriptorTimestamp, descriptor.timestamp);

        Hasher hasher = new Hasher();

        try (HintsReader hintsReader = HintsReader.open(new File(hintProperties.dir, descriptor.fileName())))
        {
            for (HintsReader.Page page : hintsReader)
                page.hintsIterator().forEachRemaining(hint -> hasher.accept(hint.mutation));
        }

        assertEquals(hintProperties.hash, hasher.hash);
        assertEquals(hintProperties.cells, hasher.cells);
    }

    private static class Hasher implements Consumer<Mutation>
    {
        int hash = 0;
        int cells = 0;

        @Override
        public void accept(Mutation mutation)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
            {
                for (Row row : update)
                {
                    if (row.clustering().size() > 0 &&
                        AsciiType.instance.compose(row.clustering().bufferAt(0)).startsWith(CELLNAME))
                    {
                        for (Cell<?> cell : row.cells())
                        {
                            hash = hash(hash, cell.buffer());
                            ++cells;
                        }
                    }
                }
            }
        }
    }

    private static int hash(int hash, ByteBuffer bytes)
    {
        int shift = 0;
        for (int i = 0; i < bytes.limit(); i++)
        {
            hash += (bytes.get(i) & 0xFF) << shift;
            shift = (shift + 8) & 0x1F;
        }
        return hash;
    }
}
